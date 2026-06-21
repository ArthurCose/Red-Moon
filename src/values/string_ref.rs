use super::ByteString;
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::VmContext;
use crate::interpreter::heap::{BytesObjectKey, HeapRef, Storage};
use crate::tag_native_type;
use slotmap::Key;

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StringRef(pub(crate) HeapRef<BytesObjectKey>);

tag_native_type!(StringRef);

impl StringRef {
    #[inline]
    pub fn id(&self) -> u64 {
        Storage::key_to_id(self.0.key().data(), Storage::BYTE_STRINGS_TAG)
    }

    pub fn fetch<'vm>(&self, ctx: &'vm VmContext<'vm>) -> Result<&'vm ByteString, RuntimeError> {
        let heap = &ctx.vm.execution_data.heap;
        let Some(bytes) = heap.get_bytes(self.0.key()) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        Ok(bytes)
    }
}
