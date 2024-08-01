use super::coroutine::YieldPermissions;
use super::heap::NativeFnObjectKey;
use super::VmContext;
use super::{multi::MultiValue, Continuation};
use crate::errors::{RuntimeError, RuntimeErrorData};
use std::rc::Rc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

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

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct NativeFunction<A> {
    #[cfg_attr(feature = "serde", serde(with = "serde_callback"))]
    pub(crate) callback: Rc<dyn NativeFunctionTrait<A>>,
}

#[cfg(feature = "serde")]
mod serde_callback {
    use super::*;

    pub(super) fn serialize<A, S>(
        _: &Rc<dyn NativeFunctionTrait<A>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Serialize::serialize(&(), serializer)
    }

    pub(super) fn deserialize<'de, A, D>(
        deserializer: D,
    ) -> Result<Rc<dyn NativeFunctionTrait<A>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _: () = serde::Deserialize::deserialize(deserializer)?;

        Ok(Rc::new(|_: A, _: &mut VmContext| {
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
    ) -> Result<MultiValue, RuntimeError> {
        let coroutine_data = &ctx.vm.execution_data.coroutine_data;

        let result = if coroutine_data.yield_permissions.allows_yield {
            self.yieldable_call(key, args, ctx)
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

    fn yieldable_call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
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

        let result = match (self.callback)(args, ctx) {
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
                        let state = coroutine_data.continuation_states.pop().unwrap();

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
    F: Fn(A, &mut VmContext) -> Result<MultiValue, RuntimeError> + Clone + 'static,
{
    fn from(value: F) -> Self {
        Self {
            callback: Rc::new(value),
        }
    }
}
