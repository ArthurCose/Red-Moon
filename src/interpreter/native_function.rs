use super::VmContext;
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::NativeCallContext;
use std::rc::Rc;

pub(crate) struct NativeFunction {
    pub(crate) callback: Rc<dyn NativeFunctionTrait>,
}

#[cfg(feature = "serde")]
impl serde::Serialize for NativeFunction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&(), serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for NativeFunction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _: () = serde::Deserialize::deserialize(deserializer)?;

        Ok(Self::from(
            |_: &mut NativeCallContext, _: &mut VmContext| {
                Err(RuntimeErrorData::FunctionLostInSerialization.into())
            },
        ))
    }
}

impl NativeFunction {
    pub(crate) fn call(
        &self,
        call_ctx: &mut NativeCallContext,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        (self.callback)(call_ctx, ctx)
    }

    pub(crate) fn shallow_clone(&self) -> Self {
        Self {
            callback: self.callback.clone(),
        }
    }
}

impl Clone for NativeFunction {
    fn clone(&self) -> Self {
        Self {
            callback: self.callback.deep_clone(),
        }
    }
}

impl<F> From<F> for NativeFunction
where
    F: Fn(&mut NativeCallContext, &mut VmContext) -> Result<(), RuntimeError> + Clone + 'static,
{
    fn from(value: F) -> Self {
        Self {
            callback: Rc::new(value),
        }
    }
}

pub(crate) trait NativeFunctionTrait:
    Fn(&mut NativeCallContext, &mut VmContext) -> Result<(), RuntimeError>
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait>;
}

impl<T: Fn(&mut NativeCallContext, &mut VmContext) -> Result<(), RuntimeError> + Clone + 'static>
    NativeFunctionTrait for T
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait> {
        Rc::new(self.clone())
    }
}
