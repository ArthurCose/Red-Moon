use super::{ForEachValue, MultiValue};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::VmContext;
use crate::interpreter::coroutine::{Coroutine, CoroutineStatus};
use crate::interpreter::heap::{CoroutineObjectKey, CounterRef, HeapRef, Storage};
use crate::tag_native_type;
use slotmap::Key;

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ThreadRef(pub(crate) HeapRef<CoroutineObjectKey>);

tag_native_type!(ThreadRef);

impl ThreadRef {
    pub(crate) fn new_main_thread() -> Self {
        Self(HeapRef {
            key: CoroutineObjectKey::null(),
            counter_ref: CounterRef::new_empty(),
        })
    }

    #[inline]
    pub fn id(&self) -> u64 {
        Storage::key_to_id(self.0.key().data(), Storage::COROUTINES_TAG)
    }

    pub fn status(&self, ctx: &mut VmContext) -> Result<CoroutineStatus, RuntimeError> {
        let key = self.0.key();
        let Some(coroutine) = ctx.vm.execution_data.heap.get_coroutine(key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        Ok(coroutine.status)
    }

    pub fn resume<A: ForEachValue>(
        &self,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<MultiValue, RuntimeError> {
        let args = MultiValue::pack(args, ctx)?;
        Coroutine::resume(self.0.key(), args, ctx)
    }
}
