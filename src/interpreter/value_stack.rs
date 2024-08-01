use super::heap::{
    BytesObjectKey, CoroutineObjectKey, FnObjectKey, Heap, NativeFnObjectKey, StackObjectKey,
    StorageKey, TableObjectKey,
};
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
    Pointer(StackObjectKey),
    Bytes(BytesObjectKey),
    Table(TableObjectKey),
    NativeFunction(NativeFnObjectKey),
    Function(FnObjectKey),
    Coroutine(CoroutineObjectKey),
}

impl Eq for StackValue {}

impl std::hash::Hash for StackValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            StackValue::Nil => core::mem::discriminant(self).hash(state),
            StackValue::Bool(b) => b.hash(state),
            StackValue::Integer(i) => i.hash(state),
            StackValue::Float(f) => f.to_bits().hash(state),
            // todo: maybe hash with object type
            // but not expecting hashing with mixed types currently
            StackValue::Bytes(key) => key.hash(state),
            StackValue::Table(key) => key.hash(state),
            StackValue::NativeFunction(key) => key.hash(state),
            StackValue::Function(key) => key.hash(state),
            StackValue::Coroutine(key) => key.hash(state),
            StackValue::Pointer(key) => key.hash(state),
        }
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

        if let Some(value) = heap.get_stack_value(key) {
            *value
        } else {
            crate::debug_unreachable!();
            #[cfg(not(debug_assertions))]
            StackValue::Nil
        }
    }

    pub(crate) fn type_name(self, heap: &Heap) -> TypeName {
        match self {
            StackValue::Nil => TypeName::Nil,
            StackValue::Bool(_) => TypeName::Bool,
            StackValue::Integer(_) | StackValue::Float(_) => TypeName::Number,
            StackValue::Bytes(_) => TypeName::String,
            StackValue::Table(_) => TypeName::Table,
            StackValue::NativeFunction(_) => TypeName::Function,
            StackValue::Function(_) => TypeName::Function,
            StackValue::Coroutine(_) => TypeName::Thread,
            StackValue::Pointer(key) => heap
                .get_stack_value(key)
                .map(|value| {
                    if matches!(value, StackValue::Pointer(_)) {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        return TypeName::Nil;
                    }

                    value.type_name(heap)
                })
                .unwrap_or(TypeName::Nil),
        }
    }

    pub(crate) fn as_storage_key(self) -> Option<StorageKey> {
        match self {
            StackValue::Nil => None,
            StackValue::Bool(_) => None,
            StackValue::Integer(_) | StackValue::Float(_) => None,
            StackValue::Bytes(key) => Some(StorageKey::Bytes(key)),
            StackValue::Table(key) => Some(StorageKey::Table(key)),
            StackValue::NativeFunction(key) => Some(StorageKey::NativeFunction(key)),
            StackValue::Function(key) => Some(StorageKey::Function(key)),
            StackValue::Coroutine(key) => Some(StorageKey::Coroutine(key)),
            StackValue::Pointer(key) => Some(StorageKey::StackValue(key)),
        }
    }

    pub(crate) fn lives_in_heap(&self) -> bool {
        match self {
            StackValue::Nil
            | StackValue::Bool(_)
            | StackValue::Integer(_)
            | StackValue::Float(_) => false,
            StackValue::Bytes(_)
            | StackValue::Table(_)
            | StackValue::NativeFunction(_)
            | StackValue::Function(_)
            | StackValue::Coroutine(_)
            | StackValue::Pointer(_) => true,
        }
    }
}

impl From<StorageKey> for StackValue {
    fn from(value: StorageKey) -> Self {
        match value {
            StorageKey::StackValue(key) => StackValue::Pointer(key),
            StorageKey::Bytes(key) => StackValue::Bytes(key),
            StorageKey::Table(key) => StackValue::Table(key),
            StorageKey::NativeFunction(key) => StackValue::NativeFunction(key),
            StorageKey::Function(key) => StackValue::Function(key),
            StorageKey::Coroutine(key) => StackValue::Coroutine(key),
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

    pub(crate) fn copy_within(&mut self, src: Range<usize>, dest: usize) {
        let min_len = if src.start > dest {
            src.end
        } else {
            dest + src.len()
        };

        if self.values.len() < min_len {
            self.values.resize(min_len, Default::default());
        }

        self.values.copy_within(src, dest);
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
