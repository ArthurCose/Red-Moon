use super::heap::{HeapRef, HeapValue};
use super::value_stack::StackValue;
use super::{FromMulti, IntoMulti, TableRef, Vm};
use crate::errors::RuntimeError;
use slotmap::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionRef(pub(crate) HeapRef);

impl FunctionRef {
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.key().data().as_ffi()
    }

    pub fn call<A: IntoMulti, R: FromMulti>(
        &self,
        args: A,
        vm: &mut Vm,
    ) -> Result<R, RuntimeError> {
        vm.call_function_key(self.0.key(), args)
    }
}

impl From<TableRef> for FunctionRef {
    #[inline]
    fn from(value: TableRef) -> Self {
        Self(value.0)
    }
}
