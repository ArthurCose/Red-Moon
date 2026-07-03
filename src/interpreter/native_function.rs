use super::VmContext;
use crate::errors::RuntimeError;
use crate::interpreter::NativeCallContext;

#[cfg(feature = "implicit_closures")]
use std::rc::Rc;

#[cfg(any(not(feature = "implicit_closures"), feature = "serde"))]
type NativeFunctionPointer = fn(&mut NativeCallContext, &mut VmContext) -> Result<(), RuntimeError>;

pub(crate) struct NativeFunction {
    #[cfg(feature = "implicit_closures")]
    pub(crate) callback: Rc<dyn NativeFunctionTrait>,
    #[cfg(not(feature = "implicit_closures"))]
    pub(crate) callback: NativeFunctionPointer,
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
        use crate::errors::RuntimeErrorData;

        let _: () = serde::Deserialize::deserialize(deserializer)?;

        let stub: NativeFunctionPointer =
            |_: &mut NativeCallContext, _: &mut VmContext| -> Result<(), RuntimeError> {
                Err(RuntimeErrorData::FunctionLostInSerialization.into())
            };

        Ok(Self::from(stub))
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
            #[cfg(feature = "implicit_closures")]
            callback: self.callback.deep_clone(),
            #[cfg(not(feature = "implicit_closures"))]
            callback: self.callback,
        }
    }
}

#[cfg(feature = "implicit_closures")]
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

#[cfg(not(feature = "implicit_closures"))]
impl From<NativeFunctionPointer> for NativeFunction {
    fn from(value: NativeFunctionPointer) -> Self {
        Self { callback: value }
    }
}

#[cfg(feature = "implicit_closures")]
pub(crate) trait NativeFunctionTrait:
    Fn(&mut NativeCallContext, &mut VmContext) -> Result<(), RuntimeError>
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait>;
}

#[cfg(feature = "implicit_closures")]
impl<T: Fn(&mut NativeCallContext, &mut VmContext) -> Result<(), RuntimeError> + Clone + 'static>
    NativeFunctionTrait for T
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait> {
        Rc::new(self.clone())
    }
}
