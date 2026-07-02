use super::execution::ExecutionContext;
use super::heap::{CoroutineObjectKey, StorageKey};
use super::{Vm, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::values::{FunctionRef, MultiValue, Value};
use slotmap::Key;

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) enum Continuation {
    Entry(StorageKey),
    Callback(FunctionRef),
    Execution(ExecutionContext),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CoroutineStatus {
    Suspended,
    Running,
    Dead,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Coroutine {
    pub(crate) status: CoroutineStatus,
    /// Vec<Continuation, parent_allows_yield>
    pub(crate) continuation_stack: Vec<(Continuation, bool)>,
    pub(crate) err: Option<RuntimeError>,
}

impl Coroutine {
    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        size += std::mem::size_of::<usize>() * 2 + std::mem::size_of::<(Continuation, bool)>();

        // todo: currently ignoring the heap size of our continuations

        size
    }

    pub(crate) fn new(function_key: StorageKey) -> Self {
        Self {
            status: CoroutineStatus::Suspended,
            continuation_stack: vec![(Continuation::Entry(function_key), true)],
            err: None,
        }
    }

    pub(crate) fn resume(
        co_key: CoroutineObjectKey,
        mut args: MultiValue,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
        if co_key.is_null() {
            return Err(RuntimeErrorData::ResumedNonSuspendedCoroutine.into());
        }

        let vm = &mut *ctx.vm;
        let heap = &mut vm.execution_data.heap;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        for value in &args.values {
            value.test_validity(heap)?;
        }

        let Some(coroutine) = heap.get_coroutine_mut_unmarked(co_key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        let mut coroutine = coroutine;

        // handle status
        if coroutine.status != CoroutineStatus::Suspended {
            return Err(RuntimeErrorData::ResumedNonSuspendedCoroutine.into());
        }

        coroutine.status = CoroutineStatus::Running;
        let coroutine_data = &mut vm.execution_data.coroutine_data;
        coroutine_data.coroutine_stack.push(co_key);

        let previous_yield_permissions = coroutine_data.yield_permitted;

        let original_size = coroutine.heap_size();

        // handle continuations
        let result = loop {
            let Some((continuation, parent_allows_yield)) = coroutine.continuation_stack.pop()
            else {
                coroutine.status = CoroutineStatus::Dead;
                break Ok(args);
            };

            let vm = &mut *ctx.vm;
            let coroutine_data = &mut vm.execution_data.coroutine_data;
            coroutine_data.yield_permitted = parent_allows_yield;

            let result = match continuation {
                Continuation::Entry(key) => match key {
                    StorageKey::Function(key) => ExecutionContext::call_interpreted(key, args, vm),
                    StorageKey::NativeFunction(key) => {
                        ExecutionContext::call_native_fn(key, args, vm)
                    }
                    _ => return Err(RuntimeError::new_invalid_internal_state()),
                },
                Continuation::Callback(function_ref) => {
                    coroutine_data.resumed_result = Some(Ok(args));

                    ExecutionContext::call_value(
                        Value::Function(function_ref).to_stack_value(),
                        vm.context().create_multi(),
                        vm,
                    )
                }
                Continuation::Execution(mut execution) => {
                    let exec_data = &mut vm.execution_data;
                    let result = execution.handle_external_return(exec_data, &mut args);
                    vm.context().store_multi(args);

                    vm.execution_stack.push(execution);
                    result
                        .map_err(|err| ExecutionContext::unwind_error(vm, err))
                        .and_then(|_| ExecutionContext::resume(vm))
                }
            };

            let coroutine_data = &mut vm.execution_data.coroutine_data;
            coroutine_data.yield_pending = false;

            match result {
                Ok(values) => args = values.unpack(ctx).unwrap(),
                Err(mut err) => {
                    let vm = &mut *ctx.vm;

                    if let RuntimeErrorData::Yield(args) = err.data {
                        Self::handle_yield(co_key, vm)?;
                        break Ok(args);
                    } else {
                        match Self::unwind_error(co_key, err, ctx) {
                            // converted to Ok ("pcall"-like function)
                            Ok(value) => args = value,
                            Err(new_err) => {
                                err = new_err;

                                if let RuntimeErrorData::Yield(args) = err.data {
                                    // continuation callback yielded
                                    Self::handle_yield(co_key, ctx.vm)?;
                                    break Ok(args);
                                }

                                // dead
                                let vm = &mut *ctx.vm;
                                let heap = &mut vm.execution_data.heap;

                                let Some(coroutine) = heap.get_coroutine_mut_unmarked(co_key)
                                else {
                                    err.data = RuntimeErrorData::new_invalid_internal_state();
                                    return Err(err);
                                };

                                coroutine.status = CoroutineStatus::Dead;
                                coroutine.continuation_stack.clear();
                                coroutine.err = Some(err.clone());
                                break Err(err);
                            }
                        }
                    }
                }
            };

            let vm = &mut *ctx.vm;
            let heap = &mut vm.execution_data.heap;

            let Some(co) = heap.get_coroutine_mut_unmarked(co_key) else {
                return Err(RuntimeErrorData::InvalidRef.into());
            };

            coroutine = co;
        };

        let vm = &mut *ctx.vm;
        let gc = &mut vm.execution_data.gc;
        let heap = &mut vm.execution_data.heap;

        let Some(coroutine) = heap.get_coroutine_mut_unmarked(co_key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };
        let new_size = coroutine.heap_size();

        gc.modify_used_memory(new_size as isize - original_size as isize);

        if gc.should_step() {
            gc.step(
                &vm.execution_data.metatable_keys,
                &vm.execution_data.cache_pools,
                heap,
                &vm.execution_stack,
                &vm.execution_data.coroutine_data,
                &vm.execution_data.debug_hook,
            );
        }

        let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;
        coroutine_data.coroutine_stack.pop();
        coroutine_data.yield_permitted = previous_yield_permissions;

        result
    }

    fn handle_yield(co_heap_key: CoroutineObjectKey, vm: &mut Vm) -> Result<(), RuntimeErrorData> {
        let gc = &mut vm.execution_data.gc;
        let heap = &mut vm.execution_data.heap;

        // using get_mut instead of get_mut_unmarked as we're adding to the continuation_stack
        let Some(coroutine) = heap.get_coroutine_mut(gc, co_heap_key) else {
            return Err(RuntimeErrorData::new_invalid_internal_state());
        };

        coroutine.status = CoroutineStatus::Suspended;

        let coroutine_data = &mut vm.execution_data.coroutine_data;

        for data in coroutine_data.in_progress_yield.drain(..).rev() {
            coroutine.continuation_stack.push(data);
        }

        Ok(())
    }

    fn unwind_error(
        co_key: CoroutineObjectKey,
        mut err: RuntimeError,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
        loop {
            let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

            coroutine_data.in_progress_yield.clear();

            let vm = &mut *ctx.vm;
            let heap = &mut vm.execution_data.heap;
            let Some(coroutine) = heap.get_coroutine_mut_unmarked(co_key) else {
                return Err(RuntimeError::new_invalid_internal_state());
            };

            let Some((continuation, parent_allows_yield)) = coroutine.continuation_stack.pop()
            else {
                break;
            };

            match continuation {
                Continuation::Callback(function_ref) => {
                    let coroutine_data = &mut vm.execution_data.coroutine_data;
                    coroutine_data.yield_permitted = parent_allows_yield;
                    coroutine_data.resumed_result = Some(Err(err));

                    match ExecutionContext::call_value(
                        Value::Function(function_ref).to_stack_value(),
                        vm.context().create_multi(),
                        vm,
                    ) {
                        Ok(values) => {
                            // converted to Ok ("pcall"-like function)
                            return Ok(values.unpack(ctx).unwrap());
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
                Continuation::Execution(execution) => {
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
