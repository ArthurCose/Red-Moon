use super::coroutine::Coroutine;
use super::heap::{HeapRef, HeapValue};
use super::{CoroutineStatus, IntoMulti, MultiValue, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use slotmap::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoroutineRef(pub(crate) HeapRef);

impl CoroutineRef {
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.key().data().as_ffi()
    }

    pub fn status(&self, ctx: &mut VmContext) -> Result<CoroutineStatus, RuntimeError> {
        let key = self.0.key();
        let Some(HeapValue::Coroutine(coroutine)) = ctx.vm.execution_data.heap.get(key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        Ok(coroutine.status)
    }

    pub fn resume<A: IntoMulti>(
        &self,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
        let args = args.into_multi(ctx)?;
        Coroutine::resume(self.0.key(), args, ctx)
    }
}
