use super::coroutine::YieldPermissions;
use super::VmContext;
use super::{multi::MultiValue, Continuation};
use crate::errors::{RuntimeError, RuntimeErrorData};
use std::rc::Rc;

pub trait NativeFunctionTrait<A>:
    Fn(A, &mut VmContext) -> Result<MultiValue, RuntimeError>
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait<A>>;
}

impl<A, T: Fn(A, &mut VmContext) -> Result<MultiValue, RuntimeError> + Clone + 'static>
    NativeFunctionTrait<A> for T
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait<A>> {
        Rc::new(self.clone())
    }
}

pub(crate) struct NativeFunction<A> {
    callback: Rc<dyn NativeFunctionTrait<A>>,
}

impl<A> NativeFunction<A> {
    pub(crate) fn call(&self, args: A, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        let result = if ctx.vm.coroutine_data.yield_permissions.allows_yield {
            self.yieldable_call(args, ctx)
        } else {
            self.non_yielding_call(args, ctx)
        };

        if let Ok(return_values) = &result {
            let heap = &ctx.vm.execution_data.heap;

            for value in &return_values.values {
                value.test_validity(heap)?;
            }
        }

        result
    }

    fn non_yielding_call(&self, args: A, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        (self.callback)(args, ctx).map_err(|mut err| {
            if matches!(err.data, RuntimeErrorData::Yield(_)) {
                err.data = RuntimeErrorData::InvalidYield
            }
            err
        })
    }

    fn yieldable_call(&self, args: A, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        let coroutine_data = &mut ctx.vm.coroutine_data;
        let previous_yield_permissions = coroutine_data.yield_permissions;
        let continuation_previously_set = coroutine_data.continuation_set;

        coroutine_data.yield_permissions = YieldPermissions {
            parent_allows_yield: previous_yield_permissions.allows_yield,
            allows_yield: false,
        };
        coroutine_data.continuation_set = false;

        let result = match (self.callback)(args, ctx) {
            Ok(values) => {
                let coroutine_data = &mut ctx.vm.coroutine_data;

                if coroutine_data.continuation_set {
                    // we don't want to leak data here
                    coroutine_data.continuation_callbacks.pop();
                }

                if !coroutine_data.in_progress_yield.is_empty() {
                    Err(RuntimeErrorData::UnhandledYield.into())
                } else {
                    Ok(values)
                }
            }
            Err(mut err) => {
                let coroutine_data = &mut ctx.vm.coroutine_data;

                if matches!(err.data, RuntimeErrorData::Yield(_)) {
                    if coroutine_data.continuation_set {
                        // pass the continuation
                        let callback = coroutine_data.continuation_callbacks.pop().unwrap();
                        let continuation = Continuation::Callback(callback);

                        coroutine_data.in_progress_yield.push((
                            continuation,
                            coroutine_data.yield_permissions.parent_allows_yield,
                        ));
                    } else {
                        err.data = RuntimeErrorData::YieldMissingContinuation;
                    }
                } else if coroutine_data.continuation_set {
                    // we don't want to leak data here
                    coroutine_data.continuation_callbacks.pop();
                }

                Err(err)
            }
        };

        let coroutine_data = &mut ctx.vm.coroutine_data;
        coroutine_data.yield_permissions = previous_yield_permissions;
        coroutine_data.continuation_set = continuation_previously_set;

        result
    }

    pub(crate) fn shallow_clone(&self) -> Self {
        Self {
            callback: self.callback.clone(),
        }
    }
}

impl<A> Clone for NativeFunction<A> {
    fn clone(&self) -> Self {
        Self {
            callback: self.callback.deep_clone(),
        }
    }
}

impl<A, F> From<F> for NativeFunction<A>
where
    F: Fn(A, &mut VmContext) -> Result<MultiValue, RuntimeError> + Clone + 'static,
{
    fn from(value: F) -> Self {
        Self {
            callback: Rc::new(value),
        }
    }
}
