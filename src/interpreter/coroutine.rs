use super::heap::HeapKey;
use super::native_function::NativeFunction;
use super::{execution::ExecutionContext, heap::HeapValue};
use super::{MultiValue, ReturnMode, Vm, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use std::rc::Rc;

pub(crate) type ContinuationCallback = NativeFunction<Result<MultiValue, RuntimeError>>;

#[derive(Default, Clone, Copy)]
pub(crate) struct YieldPermissions {
    pub(crate) parent_allows_yield: bool,
    pub(crate) allows_yield: bool,
}

#[derive(Clone)]
pub(crate) enum Continuation {
    Entry(HeapKey),
    Callback(ContinuationCallback),
    Execution {
        execution: ExecutionContext,
        return_mode: ReturnMode,
        stack_start: usize,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CoroutineStatus {
    Suspended,
    Running,
    Dead,
}

#[derive(Clone)]
pub(crate) struct Coroutine {
    pub(crate) status: CoroutineStatus,
    /// Vec<Continuation, parent_allows_yield>
    pub(crate) continuation_stack: Vec<(Continuation, bool)>,
    pub(crate) err: Option<Rc<RuntimeError>>,
}

impl Coroutine {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of::<usize>() * 2 + std::mem::size_of::<(Continuation, bool)>();

        // todo: currently ignoring the heap size of our continuations

        size
    }

    pub(crate) fn new(function_key: HeapKey) -> Self {
        Self {
            status: CoroutineStatus::Suspended,
            continuation_stack: vec![(Continuation::Entry(function_key), true)],
            err: None,
        }
    }

    pub fn resume(
        co_heap_key: HeapKey,
        mut args: MultiValue,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
        let vm = &mut *ctx.vm;
        let heap = &mut vm.execution_data.heap;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        for value in &args.values {
            value.test_validity(heap)?;
        }

        let Some(HeapValue::Coroutine(coroutine)) = heap.get_mut_unmarked(co_heap_key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        let mut coroutine = coroutine;

        // handle status
        if coroutine.status != CoroutineStatus::Suspended {
            return Err(RuntimeErrorData::ResumedNonSuspendedCoroutine.into());
        }

        coroutine.status = CoroutineStatus::Running;
        vm.coroutine_data.coroutine_stack.push(co_heap_key);

        let previous_yield_permissions = vm.coroutine_data.yield_permissions;

        let original_size = coroutine.heap_size();

        // handle continuations
        let result = loop {
            let Some((continuation, parent_allows_yield)) = coroutine.continuation_stack.pop()
            else {
                coroutine.status = CoroutineStatus::Dead;
                break Ok(args);
            };

            let vm = &mut *ctx.vm;
            vm.coroutine_data.yield_permissions.allows_yield = parent_allows_yield;

            let result = match continuation {
                Continuation::Entry(function_key) => {
                    match vm.execution_data.heap.get(function_key).unwrap() {
                        HeapValue::Function(func) => {
                            let context = ExecutionContext::new_function_call(
                                function_key,
                                func.clone(),
                                args,
                                vm,
                            );
                            vm.execution_stack.push(context);
                            ExecutionContext::resume(vm)
                        }
                        HeapValue::NativeFunction(native_function) => {
                            native_function.shallow_clone().call(args, ctx)
                        }
                        _ => unreachable!(),
                    }
                }
                Continuation::Callback(callback) => callback.call(Ok(args), ctx),
                Continuation::Execution {
                    mut execution,
                    return_mode,
                    stack_start,
                } => {
                    let result =
                        execution.handle_external_return(return_mode, stack_start, &mut args);
                    vm.store_multi(args);

                    vm.execution_stack.push(execution);
                    result
                        .map_err(|err| ExecutionContext::unwind_error(vm, err))
                        .and_then(|_| ExecutionContext::resume(vm))
                }
            };

            match result {
                Ok(values) => args = values,
                Err(mut err) => {
                    let vm = &mut *ctx.vm;

                    if let RuntimeErrorData::Yield(args) = err.data {
                        Self::handle_yield(co_heap_key, vm);
                        break Ok(args);
                    } else {
                        match Self::unwind_error(co_heap_key, err, ctx) {
                            // converted to Ok ("pcall"-like function)
                            Ok(value) => args = value,
                            Err(new_err) => {
                                err = new_err;

                                if let RuntimeErrorData::Yield(args) = err.data {
                                    // continuation callback yielded
                                    Self::handle_yield(co_heap_key, ctx.vm);
                                    break Ok(args);
                                }

                                // dead
                                let vm = &mut *ctx.vm;
                                let heap = &mut vm.execution_data.heap;

                                let Some(HeapValue::Coroutine(coroutine)) =
                                    heap.get_mut_unmarked(co_heap_key)
                                else {
                                    unreachable!();
                                };

                                coroutine.status = CoroutineStatus::Dead;
                                coroutine.continuation_stack.clear();
                                coroutine.err = Some(err.clone().into());
                                break Err(err);
                            }
                        }
                    }
                }
            };

            let vm = &mut *ctx.vm;
            let heap = &mut vm.execution_data.heap;

            let Some(HeapValue::Coroutine(co)) = heap.get_mut_unmarked(co_heap_key) else {
                return Err(RuntimeErrorData::InvalidRef.into());
            };

            coroutine = co;
        };

        let vm = &mut *ctx.vm;
        let gc = &mut vm.execution_data.gc;
        let heap = &mut vm.execution_data.heap;

        let Some(HeapValue::Coroutine(coroutine)) = heap.get_mut_unmarked(co_heap_key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };
        let new_size = coroutine.heap_size();

        gc.modify_used_memory(new_size as isize - original_size as isize);

        if gc.should_step() {
            gc.step(
                &vm.execution_data.metatable_keys,
                &vm.execution_data.cache_pools,
                &vm.execution_stack,
                heap,
            );
        }

        vm.coroutine_data.coroutine_stack.pop();
        vm.coroutine_data.yield_permissions = previous_yield_permissions;

        result
    }

    fn handle_yield(co_heap_key: HeapKey, vm: &mut Vm) {
        let gc = &mut vm.execution_data.gc;
        let heap = &mut vm.execution_data.heap;

        // using get_mut instead of get_mut_unmarked as we're adding to the continuation_stack
        let Some(HeapValue::Coroutine(coroutine)) = heap.get_mut(gc, co_heap_key) else {
            unreachable!();
        };

        coroutine.status = CoroutineStatus::Suspended;

        for data in vm.coroutine_data.in_progress_yield.drain(..).rev() {
            coroutine.continuation_stack.push(data);
        }
    }

    fn unwind_error(
        co_heap_key: HeapKey,
        mut err: RuntimeError,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
        loop {
            ctx.vm.coroutine_data.in_progress_yield.clear();

            let vm = &mut *ctx.vm;
            let heap = &mut vm.execution_data.heap;
            let Some(HeapValue::Coroutine(coroutine)) = heap.get_mut_unmarked(co_heap_key) else {
                unreachable!();
            };

            let Some((continuation, parent_allows_yield)) = coroutine.continuation_stack.pop()
            else {
                break;
            };

            match continuation {
                Continuation::Callback(callback) => {
                    vm.coroutine_data.yield_permissions.allows_yield = parent_allows_yield;

                    match callback.call(Err(err), ctx) {
                        Ok(values) => {
                            // converted to Ok ("pcall"-like function)
                            return Ok(values);
                        }
                        Err(new_err) => {
                            err = new_err;

                            if matches!(err.data, RuntimeErrorData::Yield(_)) {
                                // allow continuation callbacks to yield
                                return Err(err);
                            }
                        }
                    }
                }
                Continuation::Execution { execution, .. } => {
                    let vm = &mut *ctx.vm;
                    vm.execution_stack.push(execution);
                    err = ExecutionContext::continue_unwind(vm, err);
                }
                Continuation::Entry(_) => {}
            }
        }

        Err(err)
    }
}
