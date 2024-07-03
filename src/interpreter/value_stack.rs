use super::heap::HeapKey;
use std::ops::Range;

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub enum Primitive {
    #[default]
    Nil,
    Bool(bool),
    Integer(i64),
    Float(f64),
}

impl std::hash::Hash for Primitive {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Primitive::Nil => core::mem::discriminant(self).hash(state),
            Primitive::Bool(b) => b.hash(state),
            Primitive::Integer(i) => i.hash(state),
            Primitive::Float(f) => f.to_bits().hash(state),
        }
    }
}

impl Eq for Primitive {}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum StackValue {
    Primitive(Primitive),
    HeapValue(HeapKey),
}

impl Default for StackValue {
    #[inline]
    fn default() -> Self {
        Self::Primitive(Primitive::Nil)
    }
}

impl From<Primitive> for StackValue {
    fn from(value: Primitive) -> Self {
        StackValue::Primitive(value)
    }
}

impl From<HeapKey> for StackValue {
    fn from(value: HeapKey) -> Self {
        StackValue::HeapValue(value)
    }
}

#[derive(Default, Clone)]
pub(crate) struct ValueStack {
    values: Vec<StackValue>,
}

impl ValueStack {
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
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
            StackValue::Primitive(Primitive::Nil)
        }
    }

    pub(crate) fn get_slice(&self, mut range: Range<usize>) -> &[StackValue] {
        if range.end > self.values.len() {
            range.end = self.values.len();
        }

        &self.values[range]
    }

    pub(crate) fn is_truthy(&self, index: usize) -> bool {
        let stack_value = self.get(index);

        !matches!(
            stack_value,
            StackValue::Primitive(Primitive::Nil | Primitive::Bool(false))
        )
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

        self.values.splice(start..end, std::iter::empty());

        debug_assert_eq!(self.len(), start + keep)
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
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
