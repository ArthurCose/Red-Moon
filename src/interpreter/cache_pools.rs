use super::heap::HeapKey;
use super::value_stack::{StackValue, ValueStack};
use super::MultiValue;
use crate::vec_cell::VecCell;

pub(crate) const RECYCLE_LIMIT: usize = 64;

#[derive(Default)]
pub(crate) struct CachePools {
    pub(crate) multivalues: VecCell<MultiValue>,
    pub(crate) value_stacks: VecCell<ValueStack>,
    pub(crate) short_value_stacks: VecCell<ValueStack>,
    pub(crate) weak_associations: VecCell<Vec<(HeapKey, StackValue)>>,
}

impl CachePools {
    #[inline]
    pub(crate) fn create_multi(&self) -> MultiValue {
        self.multivalues
            .pop()
            .unwrap_or_else(|| MultiValue { values: Vec::new() })
    }

    #[inline]
    pub(crate) fn store_multi(&self, mut multivalue: MultiValue) {
        if self.multivalues.len() < RECYCLE_LIMIT {
            multivalue.clear();
            self.multivalues.push(multivalue);
        }
    }

    pub(crate) fn create_value_stack(&self) -> ValueStack {
        self.value_stacks.pop().unwrap_or_default()
    }

    pub(crate) fn store_value_stack(&self, mut value_stack: ValueStack) {
        if self.value_stacks.len() < RECYCLE_LIMIT {
            value_stack.clear();
            self.value_stacks.push(value_stack);
        }
    }

    pub(crate) fn create_short_value_stack(&self) -> ValueStack {
        self.short_value_stacks.pop().unwrap_or_default()
    }

    pub(crate) fn store_short_value_stack(&self, mut value_stack: ValueStack) {
        if self.short_value_stacks.len() < RECYCLE_LIMIT {
            value_stack.clear();
            self.short_value_stacks.push(value_stack);
        }
    }

    pub(crate) fn create_weak_associations_list(&self) -> Vec<(HeapKey, StackValue)> {
        self.weak_associations.pop().unwrap_or_default()
    }

    pub(crate) fn store_weak_associations_list(&self, mut list: Vec<(HeapKey, StackValue)>) {
        if self.weak_associations.len() < RECYCLE_LIMIT {
            list.clear();
            self.weak_associations.push(list);
        }
    }
}
