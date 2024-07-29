use super::heap::{Heap, HeapKey, HeapValue};
use super::{Number, TypeName};
use crate::vec_cell::VecCell;
use std::ops::Range;

#[cfg(feature = "serde")]
use {
    crate::serde_util::impl_serde_deduplicating_rc,
    serde::{Deserialize, Serialize},
};

#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum StackValue {
    #[default]
    Nil,
    Bool(bool),
    Integer(i64),
    Float(f64),
    HeapValue(HeapKey),
    Pointer(HeapKey),
}

impl Eq for StackValue {}

impl std::hash::Hash for StackValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            StackValue::Nil => core::mem::discriminant(self).hash(state),
            StackValue::Bool(b) => b.hash(state),
            StackValue::Integer(i) => i.hash(state),
            StackValue::Float(f) => f.to_bits().hash(state),
            StackValue::HeapValue(key) => key.hash(state),
            StackValue::Pointer(key) => key.hash(state),
        }
    }
}

impl From<HeapKey> for StackValue {
    fn from(value: HeapKey) -> Self {
        StackValue::HeapValue(value)
    }
}

impl From<Number> for StackValue {
    fn from(value: Number) -> Self {
        match value {
            Number::Integer(i) => StackValue::Integer(i),
            Number::Float(f) => StackValue::Float(f),
        }
    }
}

impl StackValue {
    #[inline]
    pub(crate) fn get_deref(self, heap: &Heap) -> Self {
        let StackValue::Pointer(key) = self else {
            return self;
        };

        let HeapValue::StackValue(value) = heap.get(key).unwrap() else {
            unreachable!();
        };

        *value
    }

    pub(crate) fn type_name(self, heap: &Heap) -> TypeName {
        match self {
            StackValue::Nil => TypeName::Nil,
            StackValue::Bool(_) => TypeName::Bool,
            StackValue::Integer(_) | StackValue::Float(_) => TypeName::Number,
            StackValue::HeapValue(key) | StackValue::Pointer(key) => {
                heap.get(key).unwrap().type_name(heap)
            }
        }
    }
}

#[derive(Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct ValueStack {
    values: Vec<StackValue>,
}

#[cfg(feature = "serde")]
impl_serde_deduplicating_rc!(serde_value_stack_rc, ValueStack, ValueStack);

impl Clone for ValueStack {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.values.clone_from(&source.values);
    }
}

impl ValueStack {
    pub(crate) fn heap_size(&self) -> usize {
        self.values.len() * std::mem::size_of::<StackValue>()
    }

    pub(crate) fn clone_using(&self, short_value_stacks: &VecCell<ValueStack>) -> ValueStack {
        let mut up_values = short_value_stacks.pop().unwrap_or_default();
        up_values.clone_from(self);
        up_values
    }

    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn push(&mut self, value: StackValue) {
        self.values.push(value)
    }

    pub(crate) fn get(&self, index: usize) -> StackValue {
        if let Some(value) = self.values.get(index) {
            *value
        } else {
            StackValue::Nil
        }
    }

    pub(crate) fn get_deref(&self, heap: &Heap, index: usize) -> StackValue {
        self.get(index).get_deref(heap)
    }

    pub(crate) fn get_slice(&mut self, range: Range<usize>) -> &[StackValue] {
        if range.end > self.values.len() {
            self.values.resize_with(range.end, Default::default);
        }

        &self.values[range]
    }

    pub(crate) fn get_slice_mut(&mut self, range: Range<usize>) -> &mut [StackValue] {
        if range.end > self.values.len() {
            self.values.resize_with(range.end, Default::default);
        }

        &mut self.values[range]
    }

    pub(crate) fn is_truthy(&self, index: usize) -> bool {
        let stack_value = self.get(index);

        !matches!(stack_value, StackValue::Nil | StackValue::Bool(false))
    }

    pub(crate) fn set(&mut self, index: usize, value: StackValue) {
        if self.values.len() <= index {
            self.values.resize(index + 1, Default::default());
        }

        self.values[index] = value;
    }

    pub(crate) fn insert(&mut self, index: usize, value: StackValue) {
        if self.values.len() < index {
            self.values.resize(index, Default::default());
        }

        self.values.insert(index, value);
    }

    pub(crate) fn extend(&mut self, iter: impl IntoIterator<Item = StackValue>) {
        self.values.extend(iter);
    }

    pub(crate) fn chip(&mut self, start: usize, keep: usize) {
        let end = self.len().saturating_sub(keep).max(start);

        if start >= self.values.len() {
            return;
        }

        self.values.drain(start..end);

        debug_assert_eq!(self.len(), start + keep)
    }

    pub(crate) fn resize(&mut self, len: usize) {
        self.values.resize(len, StackValue::Nil);
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }

    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &StackValue> {
        self.values.iter()
    }
}

impl std::fmt::Debug for ValueStack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ValueStack [")?;

        for value in &self.values {
            writeln!(f, "  {value:?}")?;
        }

        writeln!(f, "]")
    }
}
