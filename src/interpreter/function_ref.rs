use super::heap::HeapRef;
use super::{FromMulti, IntoMulti, VmContext};
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
        ctx: &mut VmContext,
    ) -> Result<R, RuntimeError> {
        ctx.call_function_key(self.0.key(), args)
    }
}
