use super::multi::MultiValue;
use super::vm::Vm;
use crate::errors::RuntimeError;
use std::rc::Rc;

pub trait NativeFunctionTrait: Fn(MultiValue, &mut Vm) -> Result<MultiValue, RuntimeError> {
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait>;
}

impl<T: Fn(MultiValue, &mut Vm) -> Result<MultiValue, RuntimeError> + Clone + 'static>
    NativeFunctionTrait for T
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait> {
        Rc::new(self.clone())
    }
}

pub(crate) struct NativeFunction {
    callback: Rc<dyn NativeFunctionTrait>,
}

impl NativeFunction {
    pub(crate) fn call(&self, args: MultiValue, vm: &mut Vm) -> Result<MultiValue, RuntimeError> {
        (self.callback)(args, vm)
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
    F: Fn(MultiValue, &mut Vm) -> Result<MultiValue, RuntimeError> + Clone + 'static,
{
    fn from(value: F) -> Self {
        Self {
            callback: Rc::new(value),
        }
    }
}
