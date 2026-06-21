use crate::interpreter::ReturnMode;
use crate::interpreter::execution::CallResult;
use crate::interpreter::garbage_collector::GarbageCollector;
use crate::interpreter::heap::{Heap, StorageKey};
use crate::interpreter::value_stack::{StackValue, ValueStack};
use bitflags::bitflags;

bitflags! {
    #[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub struct HookMask: u8 {
        const CALL = 0b00000001;
        const RETURN = 0b00000010;
        const LINE = 0b00000100;
        const INSTRUCTION = 0b00001000;
    }
}

#[derive(Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct DebugHook {
    pub(crate) mask: HookMask,
    pub(crate) after_instructions: usize,
    pub(crate) instructions_executed: usize,
    pub(crate) callback: Option<StorageKey>,
    pub(crate) executing: bool,
}

impl DebugHook {
    pub(crate) fn reset(&mut self) {
        *self = Self {
            executing: self.executing,
            ..Default::default()
        }
    }

    #[inline]
    pub(crate) fn count_instruction(&mut self) -> bool {
        if !self.mask.contains(HookMask::INSTRUCTION) || self.executing {
            return false;
        }

        if self.instructions_executed < self.after_instructions {
            self.instructions_executed += 1;
            return false;
        }

        self.instructions_executed = 0;

        true
    }

    #[cold]
    pub(crate) fn call_count_hook(
        &mut self,
        gc: &mut GarbageCollector,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        register_base: usize,
    ) -> CallResult {
        self.instructions_executed = 0;
        self.executing = true;

        let callback_stack_value = match self.callback {
            Some(StorageKey::Function(key)) => StackValue::Function(key),
            Some(StorageKey::NativeFunction(key)) => StackValue::NativeFunction(key),
            _ => unreachable!(),
        };

        // add the function to the stack
        let fn_index = value_stack.len() - register_base;
        value_stack.extend([
            callback_stack_value,
            StackValue::Integer(2),
            heap.intern_bytes(gc, "count".as_bytes()).into(),
            StackValue::Nil,
        ]);

        CallResult::Call(fn_index, ReturnMode::Hook)
    }
}

impl Clone for DebugHook {
    fn clone(&self) -> Self {
        Self {
            mask: self.mask,
            after_instructions: self.after_instructions,
            instructions_executed: 0,
            callback: self.callback,
            executing: false,
        }
    }
}
