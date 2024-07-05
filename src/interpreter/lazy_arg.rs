use super::{FromArg, FromValue, Value, Vm};
use crate::errors::RuntimeError;

/// Allows for lazy argument conversion while perserving argument position information for errors
pub struct LazyArg<T> {
    value: Value,
    position: usize,
    phantom_data: std::marker::PhantomData<T>,
}

impl<T: FromValue> LazyArg<T> {
    pub fn into_arg(self, vm: &mut Vm) -> Result<T, RuntimeError> {
        T::from_value(self.value, vm)
            .map_err(|err| RuntimeError::new_bad_argument(self.position, err))
    }
}

impl<T> FromArg for LazyArg<T> {
    fn from_arg(
        value: Value,
        position: usize,
        _: &mut Vm,
    ) -> Result<Self, crate::errors::RuntimeError> {
        Ok(Self {
            value,
            position,
            phantom_data: Default::default(),
        })
    }
}
