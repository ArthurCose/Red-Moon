use super::value_stack::ValueStack;
use super::MultiValue;
use crate::vec_cell::VecCell;
use std::rc::Rc;

const RECYCLE_LIMIT: usize = 64;

#[derive(Clone, Default)]
pub(crate) struct CachePools {
    pub(crate) multivalues: Rc<VecCell<MultiValue>>,
    pub(crate) value_stacks: Rc<VecCell<ValueStack>>,
    pub(crate) short_value_stacks: Rc<VecCell<ValueStack>>,
}

impl CachePools {
    #[inline]
    pub(crate) fn create_multi(&mut self) -> MultiValue {
        self.multivalues
            .pop()
            .unwrap_or_else(|| MultiValue { values: Vec::new() })
    }

    #[inline]
    pub(crate) fn store_multi(&mut self, mut multivalue: MultiValue) {
        if self.multivalues.len() < RECYCLE_LIMIT {
            multivalue.clear();
            self.multivalues.push(multivalue);
        }
    }

    pub(crate) fn create_value_stack(&mut self) -> ValueStack {
        self.value_stacks.pop().unwrap_or_default()
    }

    pub(crate) fn store_value_stack(&mut self, mut value_stack: ValueStack) {
        if self.value_stacks.len() < RECYCLE_LIMIT {
            value_stack.clear();
            self.value_stacks.push(value_stack);
        }
    }

    pub(crate) fn create_short_value_stack(&mut self) -> ValueStack {
        self.short_value_stacks.pop().unwrap_or_default()
    }

    pub(crate) fn store_short_value_stack(&mut self, mut value_stack: ValueStack) {
        if self.short_value_stacks.len() < RECYCLE_LIMIT {
            value_stack.clear();
            self.short_value_stacks.push(value_stack);
        }
    }
}
