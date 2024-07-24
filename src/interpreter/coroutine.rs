use super::heap::HeapKey;
use super::{execution::ExecutionContext, heap::HeapValue};
use super::{MultiValue, ReturnMode, VmContext};
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData};
use std::rc::Rc;

#[derive(Clone)]
pub(crate) enum Continuation {
    Entry(HeapKey),
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
    pub(crate) continuation_stack: Vec<Continuation>,
    pub(crate) err: Option<Rc<RuntimeError>>,
}

impl Coroutine {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of::<usize>() * 2 + std::mem::size_of::<Continuation>();

        // todo: currently ignoring the heap size of our continuations

        size
    }

    pub(crate) fn new(function_key: HeapKey) -> Self {
        Self {
            status: CoroutineStatus::Suspended,
            continuation_stack: vec![Continuation::Entry(function_key)],
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
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let mut coroutine = coroutine;

        // handle status
        if coroutine.status != CoroutineStatus::Suspended {
            return Err(RuntimeErrorData::ResumedNonSuspendedCoroutine.into());
        }

        coroutine.status = CoroutineStatus::Running;
        vm.execution_data.coroutine_stack.push(co_heap_key);

        let original_size = coroutine.heap_size();
        let new_size;

        // handle continuations
        let result = loop {
            let Some(continuation) = coroutine.continuation_stack.pop() else {
                new_size = coroutine.heap_size();
                coroutine.status = CoroutineStatus::Dead;
                break Ok(args);
            };

            let vm = &mut *ctx.vm;

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
                    let gc = &mut vm.execution_data.gc;
                    let heap = &mut vm.execution_data.heap;

                    // using get_mut instead of get_mut_unmarked as we may end up adding to the continuation_stack
                    // otherwise we'll be clearing it, and it won't be too much extra work
                    let Some(HeapValue::Coroutine(coroutine)) = heap.get_mut(gc, co_heap_key)
                    else {
                        unreachable!();
                    };

                    if let RuntimeErrorData::Yield(args) = err.data {
                        coroutine.status = CoroutineStatus::Suspended;

                        for continuation in vm.execution_data.in_progress_yield.drain(..) {
                            coroutine.continuation_stack.push(continuation);
                        }

                        new_size = coroutine.heap_size();
                        break Ok(args);
                    } else {
                        coroutine.status = CoroutineStatus::Dead;

                        let stack = std::mem::take(&mut coroutine.continuation_stack);
                        new_size = coroutine.heap_size();

                        for continuation in stack.into_iter().rev() {
                            if let Continuation::Execution { execution, .. } = continuation {
                                vm.execution_stack.push(execution);
                                err = ExecutionContext::continue_unwind(vm, err);
                            }
                        }

                        let heap = &mut vm.execution_data.heap;

                        let Some(HeapValue::Coroutine(coroutine)) =
                            heap.get_mut_unmarked(co_heap_key)
                        else {
                            unreachable!();
                        };

                        coroutine.err = Some(err.clone().into());
                        break Err(err);
                    }
                }
            };

            let vm = &mut *ctx.vm;
            let heap = &mut vm.execution_data.heap;

            let Some(HeapValue::Coroutine(co)) = heap.get_mut_unmarked(co_heap_key) else {
                return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
            };

            coroutine = co;
        };

        let vm = &mut *ctx.vm;
        let gc = &mut vm.execution_data.gc;
        let heap = &mut vm.execution_data.heap;

        gc.modify_used_memory(new_size as isize - original_size as isize);

        if gc.should_step() {
            gc.step(
                &vm.execution_data.metatable_keys,
                &vm.execution_data.cache_pools,
                &vm.execution_stack,
                heap,
            );
        }

        vm.execution_data.coroutine_stack.pop();

        result
    }
}
