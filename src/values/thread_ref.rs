use super::{ForEachValue, MultiValue};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::coroutine::{Coroutine, CoroutineStatus};
use crate::interpreter::debug_hooks::DebugHook;
use crate::interpreter::heap::{CoroutineObjectKey, CounterRef, HeapRef, Storage};
use crate::interpreter::{HookMask, VmContext};
use crate::tag_native_type;
use crate::values::FunctionRef;
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

    fn debug_hook_mut<'vm>(
        &self,
        ctx: &'vm mut VmContext,
    ) -> Result<&'vm mut DebugHook, RuntimeErrorData> {
        let exec_data = &mut ctx.vm.execution_data;
        let coroutine_stack = &mut exec_data.coroutine_data.coroutine_stack;

        let co_key = self.0.key();

        // see if this is the main thread
        if co_key.is_null() {
            // the first resumed coroutine backs up the main thread's debug
            if let Some(&bottom_key) = coroutine_stack.first() {
                let Some(bottom_co) = exec_data.heap.get_coroutine_mut_unmarked(bottom_key) else {
                    return Err(RuntimeErrorData::InvalidInternalState);
                };

                return Ok(&mut bottom_co.debug_hook);
            }

            // if there's no coroutines running, then the currently applied debug hook is for the main thread
            return Ok(&mut exec_data.debug_hook);
        }

        // not the main thread

        // see if this thread is in the coroutine stack as the debug hook may have been backed up
        if let Some(i) = coroutine_stack.iter().position(|&key| key == co_key) {
            // coroutines back up the prev thread's debug hook when resumed
            if let Some(&next_key) = coroutine_stack.get(i + 1) {
                let Some(next_co) = exec_data.heap.get_coroutine_mut_unmarked(next_key) else {
                    return Err(RuntimeErrorData::InvalidInternalState);
                };

                return Ok(&mut next_co.debug_hook);
            }

            // we must be the top coroutine, so the active debug hook is ours
            return Ok(&mut exec_data.debug_hook);
        }

        // not in the coroutine stack, so we just need to grab our thread directly

        let Some(co) = exec_data.heap.get_coroutine_mut_unmarked(co_key) else {
            return Err(RuntimeErrorData::InvalidInternalState);
        };

        Ok(&mut co.debug_hook)
    }

    pub fn set_hook(
        &self,
        mask: HookMask,
        instruction_count: usize,
        callback: FunctionRef,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeErrorData> {
        let exec_data = &mut ctx.vm.execution_data;
        callback.test_validity(&exec_data.heap)?;

        let debug_hook = self.debug_hook_mut(ctx)?;
        debug_hook.reset();
        debug_hook.mask = mask;
        debug_hook.after_instructions = instruction_count;
        debug_hook.callback = Some(callback.0.key());

        Ok(())
    }

    #[inline]
    pub fn remove_hook(&self, ctx: &mut VmContext) -> Result<(), RuntimeErrorData> {
        self.debug_hook_mut(ctx)?.reset();
        Ok(())
    }

    pub fn hook(&self, ctx: &mut VmContext) -> Result<Option<FunctionRef>, RuntimeErrorData> {
        let Some(storage_key) = self.debug_hook_mut(ctx)?.callback else {
            return Ok(None);
        };

        let heap_key = ctx.vm.execution_data.heap.create_ref(storage_key);
        Ok(Some(FunctionRef(heap_key)))
    }

    #[inline]
    pub fn hook_mask(&self, ctx: &mut VmContext) -> Result<HookMask, RuntimeErrorData> {
        Ok(self.debug_hook_mut(ctx)?.mask)
    }

    #[inline]
    pub fn hook_count(&self, ctx: &mut VmContext) -> Result<usize, RuntimeErrorData> {
        Ok(self.debug_hook_mut(ctx)?.after_instructions)
    }
}
