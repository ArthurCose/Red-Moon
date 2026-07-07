use super::{ForEachValue, MultiValue};
use crate::errors::{RuntimeError, RuntimeErrorData, StackTrace};
use crate::interpreter::coroutine::{Coroutine, CoroutineStatus};
use crate::interpreter::debug_hooks::DebugHook;
use crate::interpreter::heap::{CoroutineObjectKey, CounterRef, HeapRef, Storage};
use crate::interpreter::{Continuation, HookMask, VmContext};
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

    pub fn status(&self, ctx: &mut VmContext) -> Result<CoroutineStatus, RuntimeErrorData> {
        let key = self.0.key();
        let Some(coroutine) = ctx.vm.execution_data.heap.get_coroutine(key) else {
            return Err(RuntimeErrorData::InvalidRef);
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
            if let Some(&(bottom_key, _)) = coroutine_stack.first() {
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
        if let Some(i) = coroutine_stack.iter().position(|&(key, _)| key == co_key) {
            // coroutines back up the prev thread's debug hook when resumed
            if let Some(&(next_key, _)) = coroutine_stack.get(i + 1) {
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

    pub fn traceback(
        &self,
        level: usize,
        ctx: &mut VmContext,
    ) -> Result<StackTrace, RuntimeErrorData> {
        let co_key = self.0.key();

        let coroutine_stack = &ctx.vm.execution_data.coroutine_data.coroutine_stack;
        let execution_stack = &ctx.vm.execution_stack;

        let executing_range = if co_key.is_null() {
            // main thread
            let end = coroutine_stack
                .first()
                .map(|&(_, execution_stack_start)| execution_stack_start)
                .unwrap_or(execution_stack.len());

            0..end
        } else {
            // coroutine
            let mut start = execution_stack.len();
            let mut end = execution_stack.len();

            for &(key, execution_stack_start) in coroutine_stack.iter().rev() {
                if key == co_key {
                    start = execution_stack_start;
                    break;
                }

                end = execution_stack_start
            }

            start..end
        };

        let Some(traceback_iter) = ctx.vm.execution_stack.get(executing_range) else {
            return Err(RuntimeErrorData::new_invalid_internal_state());
        };

        let traceback_iter = traceback_iter
            .iter()
            .rev()
            .flat_map(|execution| StackTrace::execution_traceback_iter(execution))
            .skip(level);

        let mut trace = StackTrace::default();
        trace.frames.extend(traceback_iter);

        if co_key.is_null() {
            // main thread, no need to look up stored coroutine execution stacks
            return Ok(trace);
        }

        let heap = &ctx.vm.execution_data.heap;
        let Some(coroutine) = heap.get_coroutine(co_key) else {
            return Err(RuntimeErrorData::InvalidRef);
        };

        let traceback_iter = coroutine
            .continuation_stack
            .iter()
            .rev()
            .flat_map(|(continuation, _)| {
                if let Continuation::Execution(execution) = continuation {
                    Some(StackTrace::execution_traceback_iter(execution))
                } else {
                    None
                }
            })
            .flatten()
            .skip(level.saturating_sub(trace.frames.len()));

        trace.frames.extend(traceback_iter);

        Ok(trace)
    }
}
