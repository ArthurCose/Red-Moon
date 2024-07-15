use super::heap::{HeapRef, HeapValue};
use super::{ByteString, Vm};
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData};
use slotmap::Key;

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct StringRef(pub(crate) HeapRef);

impl StringRef {
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.key().data().as_ffi()
    }

    pub fn fetch<'vm>(&self, vm: &'vm Vm) -> Result<&'vm ByteString, RuntimeError> {
        let heap = &vm.execution_data.heap;
        let Some(HeapValue::Bytes(bytes)) = heap.get(self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        Ok(bytes)
    }
}
