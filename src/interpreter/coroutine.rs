use super::execution::ExecutionContext;
use super::heap::{CoroutineObjectKey, NativeFnObjectKey, StorageKey};
use super::value_stack::ValueStack;
use super::{MultiValue, ReturnMode, Vm, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use std::rc::Rc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Copy)]
pub(crate) struct YieldPermissions {
    pub(crate) parent_allows_yield: bool,
    pub(crate) allows_yield: bool,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum Continuation {
    Entry(StorageKey),
    Callback(NativeFnObjectKey, ValueStack),
    Execution {
        execution: ExecutionContext,
        return_mode: ReturnMode,
        stack_start: usize,
    },
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CoroutineStatus {
    Suspended,
    Running,
    Dead,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Coroutine {
    pub(crate) status: CoroutineStatus,
    /// Vec<Continuation, parent_allows_yield>
    pub(crate) continuation_stack: Vec<(Continuation, bool)>,
    #[cfg_attr(feature = "serde", serde(skip))]
    pub(crate) err: Option<Rc<RuntimeError>>,
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

    pub fn resume(
        co_key: CoroutineObjectKey,
        mut args: MultiValue,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
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

        let previous_yield_permissions = coroutine_data.yield_permissions;

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
            coroutine_data.yield_permissions.allows_yield = parent_allows_yield;

            let result = match continuation {
                Continuation::Entry(key) => match key {
                    StorageKey::Function(key) => ExecutionContext::new_function_call(key, args, vm)
                        .and_then(|execution| {
                            vm.execution_stack.push(execution);
                            ExecutionContext::resume(vm)
                        }),
                    StorageKey::NativeFunction(key) => {
                        let Some(native_function) = vm.execution_data.heap.get_native_fn(key)
                        else {
                            return Err(RuntimeErrorData::InvalidInternalState.into());
                        };

                        native_function.shallow_clone().call(key, args, ctx)
                    }
                    _ => return Err(RuntimeErrorData::InvalidInternalState.into()),
                },
                Continuation::Callback(key, state) => {
                    let cache_pools = &vm.execution_data.cache_pools;
                    let heap = &mut vm.execution_data.heap;
                    let state_multi = MultiValue::from_value_stack(cache_pools, heap, &state);
                    let Some(callback) = heap.resume_callbacks.get(&key) else {
                        return Err(RuntimeErrorData::InvalidInternalState.into());
                    };

                    cache_pools.store_short_value_stack(state);

                    callback
                        .shallow_clone()
                        .call(key, (Ok(args), state_multi), ctx)
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
                                    err.data = RuntimeErrorData::InvalidInternalState;
                                    return Err(err);
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
            );
        }

        let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;
        coroutine_data.coroutine_stack.pop();
        coroutine_data.yield_permissions = previous_yield_permissions;

        result
    }

    fn handle_yield(co_heap_key: CoroutineObjectKey, vm: &mut Vm) -> Result<(), RuntimeErrorData> {
        let gc = &mut vm.execution_data.gc;
        let heap = &mut vm.execution_data.heap;

        // using get_mut instead of get_mut_unmarked as we're adding to the continuation_stack
        let Some(coroutine) = heap.get_coroutine_mut(gc, co_heap_key) else {
            return Err(RuntimeErrorData::InvalidInternalState);
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
            let cache_pools = &ctx.vm.execution_data.cache_pools;

            for (continuation, _) in coroutine_data.in_progress_yield.drain(..) {
                if let Continuation::Callback(_, state) = continuation {
                    cache_pools.store_short_value_stack(state);
                }
            }

            let vm = &mut *ctx.vm;
            let heap = &mut vm.execution_data.heap;
            let Some(coroutine) = heap.get_coroutine_mut_unmarked(co_key) else {
                return Err(RuntimeErrorData::InvalidInternalState.into());
            };

            let Some((continuation, parent_allows_yield)) = coroutine.continuation_stack.pop()
            else {
                break;
            };

            match continuation {
                Continuation::Callback(key, state) => {
                    let coroutine_data = &mut vm.execution_data.coroutine_data;
                    coroutine_data.yield_permissions.allows_yield = parent_allows_yield;

                    let cache_pools = &vm.execution_data.cache_pools;
                    let heap = &mut vm.execution_data.heap;
                    let state_multi = MultiValue::from_value_stack(cache_pools, heap, &state);
                    let Some(callback) = heap.resume_callbacks.get(&key) else {
                        return Err(RuntimeErrorData::InvalidInternalState.into());
                    };

                    cache_pools.store_short_value_stack(state);

                    match callback
                        .shallow_clone()
                        .call(key, (Err(err), state_multi), ctx)
                    {
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
