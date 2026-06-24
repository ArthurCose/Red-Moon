use super::coroutine::YieldPermissions;
use super::heap::NativeFnObjectKey;
use super::{Continuation, NativeCallContext, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use std::rc::Rc;

pub(crate) struct NativeFunction<A> {
    pub(crate) callback: Rc<dyn NativeFunctionTrait<A>>,
}

#[cfg(feature = "serde")]
impl<A> serde::Serialize for NativeFunction<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&(), serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, A> serde::Deserialize<'de> for NativeFunction<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _: () = serde::Deserialize::deserialize(deserializer)?;

        Ok(Self::from(|_, _: A, _: &mut VmContext| {
            Err(RuntimeErrorData::FunctionLostInSerialization.into())
        }))
    }
}

impl<A> NativeFunction<A> {
    pub(crate) fn call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<NativeCallContext, RuntimeError> {
        let coroutine_data = &ctx.vm.execution_data.coroutine_data;

        if coroutine_data.yield_permissions.allows_yield {
            self.yieldable_call(key, args, ctx)
        } else {
            self.non_yielding_call(key, args, ctx)
        }
    }

    fn non_yielding_call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<NativeCallContext, RuntimeError> {
        (self.callback)(key, args, ctx).map_err(|mut err| {
            if matches!(err.data, RuntimeErrorData::Yield(_)) {
                err.data = RuntimeErrorData::InvalidYield
            }
            err
        })
    }

    fn yieldable_call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<NativeCallContext, RuntimeError> {
        let execution_data = &mut ctx.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;

        if !coroutine_data.in_progress_yield.is_empty() {
            return Err(RuntimeErrorData::UnhandledYield.into());
        }

        let previous_yield_permissions = coroutine_data.yield_permissions;
        let continuation_previously_set = coroutine_data.continuation_state_set;

        coroutine_data.yield_permissions = YieldPermissions {
            parent_allows_yield: previous_yield_permissions.allows_yield,
            allows_yield: execution_data.heap.resume_callbacks.contains_key(&key),
        };
        coroutine_data.continuation_state_set = false;

        let result = match (self.callback)(key, args, ctx) {
            Ok(values) => {
                let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                if !coroutine_data.in_progress_yield.is_empty() {
                    Err(RuntimeErrorData::UnhandledYield.into())
                } else {
                    Ok(values)
                }
            }
            Err(mut err) => {
                let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                if matches!(err.data, RuntimeErrorData::Yield(_)) {
                    if coroutine_data.continuation_state_set {
                        let Some(state) = coroutine_data.continuation_states.pop() else {
                            return Err(RuntimeErrorData::InvalidInternalState.into());
                        };

                        // pass the continuation
                        let continuation = Continuation::Callback(key, state);

                        coroutine_data.in_progress_yield.push((
                            continuation,
                            coroutine_data.yield_permissions.parent_allows_yield,
                        ));
                    } else {
                        err.data = RuntimeErrorData::UnhandledYield;
                    }
                }

                Err(err)
            }
        };

        let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;
        coroutine_data.yield_permissions = previous_yield_permissions;
        coroutine_data.continuation_state_set = continuation_previously_set;

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
    F: Fn(NativeFnObjectKey, A, &mut VmContext) -> Result<NativeCallContext, RuntimeError>
        + Clone
        + 'static,
{
    fn from(value: F) -> Self {
        Self {
            callback: Rc::new(value),
        }
    }
}

pub(crate) trait NativeFunctionTrait<A>:
    Fn(NativeFnObjectKey, A, &mut VmContext) -> Result<NativeCallContext, RuntimeError>
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait<A>>;
}

impl<
    A,
    T: Fn(NativeFnObjectKey, A, &mut VmContext) -> Result<NativeCallContext, RuntimeError>
        + Clone
        + 'static,
> NativeFunctionTrait<A> for T
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait<A>> {
        Rc::new(self.clone())
    }
}
