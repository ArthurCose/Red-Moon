use super::{ForEachValue, FromValues, Value};
use crate::errors::RuntimeError;
use crate::interpreter::VmContext;
use crate::interpreter::cache_pools::CachePools;
use crate::interpreter::heap::Heap;
use crate::interpreter::value_stack::{StackValue, ValueStack};
use crate::tag_native_type;
use thin_vec::ThinVec;

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct MultiValue<T = Value> {
    pub(crate) values: ThinVec<T>,
}

tag_native_type!(MultiValue);

impl<T> Default for MultiValue<T> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}

impl<T> From<Vec<T>> for MultiValue<T> {
    #[inline]
    fn from(mut values: Vec<T>) -> Self {
        values.reverse();

        Self {
            values: values.into(),
        }
    }
}

impl<T> From<ThinVec<T>> for MultiValue<T> {
    #[inline]
    fn from(mut values: ThinVec<T>) -> Self {
        values.reverse();
        Self { values }
    }
}

impl<T> From<MultiValue<T>> for Vec<T> {
    #[inline]
    fn from(multi: MultiValue<T>) -> Self {
        let mut values = multi.values;
        values.reverse();
        values.into()
    }
}

impl<T> From<MultiValue<T>> for ThinVec<T> {
    #[inline]
    fn from(multi: MultiValue<T>) -> Self {
        let mut values = multi.values;
        values.reverse();
        values
    }
}

impl MultiValue<Value> {
    pub fn pack<T: ForEachValue>(values: T, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        let mut multi = ctx.create_multi();

        values.for_each_value(ctx, |result, _| {
            let value = result?;
            multi.values.push(value);
            Ok(())
        })?;

        multi.values.reverse();

        Ok(multi)
    }

    pub fn unpack<T: FromValues>(mut self, ctx: &mut VmContext) -> Result<T, RuntimeError> {
        let result = T::from_values(ctx, |_| self.pop_front());
        ctx.store_multi(self);
        result
    }

    pub(crate) fn from_value_stack(
        cache_pools: &CachePools,
        heap: &mut Heap,
        value_stack: &ValueStack,
    ) -> Self {
        let mut multi = cache_pools.create_multi();
        multi.values.extend(
            value_stack
                .iter()
                .rev()
                .map(|&value| Value::from_stack_value(heap, value)),
        );

        multi
    }

    /// Pushes values into a ValueStack, places an integer storing the length first
    pub(crate) fn push_stack_multi(&self, value_stack: &mut ValueStack) {
        value_stack.push(StackValue::Integer(self.len() as _));
        self.extend_stack(value_stack);
    }

    /// Pushes values into a ValueStack
    pub(crate) fn extend_stack(&self, value_stack: &mut ValueStack) {
        value_stack.extend(self.values.iter().rev().map(|value| value.to_stack_value()));
    }
}

impl<T> MultiValue<T> {
    pub fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value using a zero based index.
    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index < self.values.len() {
            return self.values.get(self.values.len() - index);
        }
        None
    }

    #[inline]
    pub fn push_front(&mut self, value: T) {
        self.values.push(value);
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<T> {
        self.values.pop()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.values.clear();
    }

    #[inline]
    pub fn drain_all(&mut self) -> impl DoubleEndedIterator<Item = T> + '_ {
        self.values.drain(..).rev()
    }
}
