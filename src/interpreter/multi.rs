use super::heap::Heap;
use super::value::{FromValue, IntoValue};
use super::value_stack::{StackValue, ValueStack};
use super::vm::VmContext;
use super::Value;
use crate::errors::{IllegalInstruction, RuntimeError};

#[derive(Clone)]
pub struct MultiValue {
    pub(crate) values: Vec<Value>,
}

impl MultiValue {
    pub fn pack<T: IntoMulti>(value: T, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        T::into_multi(value, ctx)
    }

    pub fn unpack<T: FromMulti>(self, ctx: &mut VmContext) -> Result<T, RuntimeError> {
        T::from_multi(self, ctx)
    }

    pub fn unpack_args<T: FromArgs>(self, ctx: &mut VmContext) -> Result<T, RuntimeError> {
        T::from_args(self, 1, ctx)
    }

    /// `position` is the argument position of the first value, starts at 1
    ///
    /// `position` should be incremented for every value taken from the multivalue before calling this function
    pub fn unpack_modified_args<T: FromArgs>(
        self,
        ctx: &mut VmContext,
        position: usize,
    ) -> Result<T, RuntimeError> {
        T::from_args(self, position, ctx)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&Value> {
        if index < self.values.len() {
            return self.values.get(self.values.len() - index - 1);
        }
        None
    }

    #[inline]
    pub fn push_front(&mut self, value: Value) {
        self.values.push(value);
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<Value> {
        self.values.pop()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.values.clear();
    }

    #[inline]
    pub fn to_vec(mut self) -> Vec<Value> {
        self.values.reverse();
        self.values
    }

    #[inline]
    pub fn drain_all(&mut self) -> impl DoubleEndedIterator<Item = Value> + '_ {
        self.values.drain(..).rev()
    }

    /// Copies values from a ValueStack
    pub(crate) fn copy_stack_multi(
        &mut self,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        len_index: usize,
        missing_count_err: IllegalInstruction,
    ) -> Result<(), IllegalInstruction> {
        let StackValue::Integer(arg_count) = value_stack.get(len_index) else {
            return Err(missing_count_err);
        };

        // relying on the function_rev_index calculation as the verification for this
        let arg_count = arg_count as usize;

        let start = len_index + 1;
        let end = start + arg_count;

        self.values.extend(
            value_stack
                .get_slice(start..end)
                .iter()
                .rev()
                .map(|&value| Value::from_stack_value(heap, value)),
        );

        Ok(())
    }

    /// Pushes values into a ValueStack
    pub(crate) fn push_stack_multi(&self, value_stack: &mut ValueStack) {
        value_stack.push(StackValue::Integer(self.len() as _));

        value_stack.extend(self.values.iter().rev().map(|value| value.to_stack_value()));
    }
}

pub trait IntoMulti {
    fn into_multi(self, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError>;
}

impl IntoMulti for MultiValue {
    #[inline]
    fn into_multi(self, _: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        Ok(self)
    }
}

impl IntoMulti for () {
    #[inline]
    fn into_multi(self, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        Ok(ctx.create_multi())
    }
}

impl<T: IntoValue> IntoMulti for T {
    #[inline]
    fn into_multi(self, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
        let mut multi = ctx.create_multi();
        multi.push_front(self.into_value(ctx)?);
        Ok(multi)
    }
}

macro_rules! impl_into_multi {
  ($($name:ident)+) => (
      impl<$($name: IntoValue),*> IntoMulti for ($($name,)*) {
          #[allow(non_snake_case)]
          #[inline]
          fn into_multi(self, ctx: &mut VmContext) -> Result<MultiValue, RuntimeError> {
              let mut multi = ctx.create_multi();
              let ($($name,)*) = self;
              $(multi.values.push($name.into_value(ctx)?);)*
              multi.values.reverse();
              Ok(multi)
          }
      }
  );
}

impl_into_multi! { A }
impl_into_multi! { A B }
impl_into_multi! { A B C }
impl_into_multi! { A B C D }
impl_into_multi! { A B C D E }
impl_into_multi! { A B C D E F }
impl_into_multi! { A B C D E F G }
impl_into_multi! { A B C D E F G H }
impl_into_multi! { A B C D E F G H I }
impl_into_multi! { A B C D E F G H I J }
impl_into_multi! { A B C D E F G H I J K }
impl_into_multi! { A B C D E F G H I J K L }

pub trait FromMulti: Sized {
    fn from_multi(multi: MultiValue, ctx: &mut VmContext) -> Result<Self, RuntimeError>;
}

impl FromMulti for MultiValue {
    #[inline]
    fn from_multi(multi: MultiValue, _: &mut VmContext) -> Result<Self, RuntimeError> {
        Ok(multi)
    }
}

impl FromMulti for () {
    #[inline]
    fn from_multi(multi: MultiValue, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        ctx.store_multi(multi);
        Ok(())
    }
}

impl<T: FromValue> FromMulti for T {
    #[inline]
    fn from_multi(mut multi: MultiValue, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        let result = T::from_value(multi.pop_front().unwrap_or(Value::Nil), ctx);
        ctx.store_multi(multi);
        result
    }
}

macro_rules! impl_from_multi {
    ($last:ident $($name:ident)+) => (
        impl<$($name: FromValue,)* $last: FromMulti> FromMulti for ($($name,)* $last,) {
            #[allow(non_snake_case)]
            #[inline]
            fn from_multi(mut multi: MultiValue, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
                $(let $name = $name::from_value(multi.pop_front().unwrap_or(Value::Nil), ctx)?;)*
                let $last = $last::from_multi( multi,ctx)?;
                Ok(($($name,)* $last,))
            }
        }
    );
}

impl_from_multi! { A B }
impl_from_multi! { A B C }
impl_from_multi! { A B C D }
impl_from_multi! { A B C D E }
impl_from_multi! { A B C D E F }
impl_from_multi! { A B C D E F G }
impl_from_multi! { A B C D E F G H }
impl_from_multi! { A B C D E F G H I }
impl_from_multi! { A B C D E F G H I J }
impl_from_multi! { A B C D E F G H I J K }
impl_from_multi! { A B C D E F G H I J K L }

pub trait FromArg: Sized {
    /// `position` is the argument position, starts at 1
    fn from_arg(value: Value, position: usize, ctx: &mut VmContext) -> Result<Self, RuntimeError>;
}

impl<T: FromValue> FromArg for T {
    #[inline]
    fn from_arg(value: Value, position: usize, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        let _ = position;
        Self::from_value(value, ctx).map_err(|err| RuntimeError::new_bad_argument(position, err))
    }
}

pub trait FromArgs: Sized {
    /// `position` is the argument position of the first value, starts at 1
    ///
    /// `position` should be incremented for every value taken from the multivalue before calling this function
    fn from_args(
        args: MultiValue,
        position: usize,
        ctx: &mut VmContext,
    ) -> Result<Self, RuntimeError>;
}

impl FromArgs for MultiValue {
    #[inline]
    fn from_args(args: MultiValue, _: usize, _: &mut VmContext) -> Result<Self, RuntimeError> {
        Ok(args)
    }
}

impl FromArgs for () {
    #[inline]
    fn from_args(args: MultiValue, _: usize, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        ctx.store_multi(args);
        Ok(())
    }
}

impl<T: FromArg> FromArgs for T {
    #[inline]
    fn from_args(
        mut multi: MultiValue,
        position: usize,
        ctx: &mut VmContext,
    ) -> Result<Self, RuntimeError> {
        let result = T::from_arg(multi.pop_front().unwrap_or(Value::Nil), position, ctx);

        ctx.store_multi(multi);
        result
    }
}

macro_rules! impl_from_args {
    ($last:ident $($name:ident)+) => (
        impl<$($name: FromArg,)* $last: FromArgs> FromArgs for ($($name,)* $last,) {
            #[allow(non_snake_case)]
            #[inline]
            fn from_args(
                mut multi: MultiValue,
                mut position: usize,
                ctx: &mut VmContext,
            ) -> Result<Self, RuntimeError> {
                $(let $name =
                    match $name::from_arg(multi.pop_front().unwrap_or(Value::Nil), position, ctx) {
                        Ok(value) => value,
                        Err(err) => {
                            ctx.store_multi(multi);
                            return Err(err);
                        }
                    };
                position += 1;)*

                let $last = $last::from_args(multi, position, ctx)?;

                Ok(($($name,)* $last,))
            }
        }
    );
}

impl_from_args! { A B }
impl_from_args! { A B C }
impl_from_args! { A B C D }
impl_from_args! { A B C D E }
impl_from_args! { A B C D E F }
impl_from_args! { A B C D E F G }
impl_from_args! { A B C D E F G H }
impl_from_args! { A B C D E F G H I }
impl_from_args! { A B C D E F G H I J }
impl_from_args! { A B C D E F G H I J K }
impl_from_args! { A B C D E F G H I J K L }
