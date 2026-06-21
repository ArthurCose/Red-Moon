use thin_vec::ThinVec;

use super::{FromValue, IntoValue, MultiValue, Value};
use crate::errors::RuntimeError;
use crate::interpreter::VmContext;

pub trait ForEachValue {
    fn for_each_value(
        self,
        ctx: &mut VmContext,
        callback: impl FnMut(Result<Value, RuntimeError>, &mut VmContext) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError>;
}

impl ForEachValue for () {
    #[inline]
    fn for_each_value(
        self,
        _: &mut VmContext,
        _: impl FnMut(Result<Value, RuntimeError>, &mut VmContext) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        Ok(())
    }
}

impl<T: IntoValue> ForEachValue for MultiValue<T> {
    #[inline]
    fn for_each_value(
        mut self,
        ctx: &mut VmContext,
        mut callback: impl FnMut(
            Result<Value, RuntimeError>,
            &mut VmContext,
        ) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        while let Some(value) = self.pop_front() {
            callback(value.into_value(ctx), ctx)?;
        }

        Ok(())
    }
}

impl<T: IntoValue> ForEachValue for T {
    #[inline]
    fn for_each_value(
        self,
        ctx: &mut VmContext,
        mut callback: impl FnMut(
            Result<Value, RuntimeError>,
            &mut VmContext,
        ) -> Result<(), RuntimeError>,
    ) -> Result<(), RuntimeError> {
        callback(self.into_value(ctx), ctx)?;
        Ok(())
    }
}

macro_rules! impl_for_each_value {
    ($($name:ident)*, $last:ident) => (
        #[allow(unused_parens)]
        impl<$($name: IntoValue,)* $last: ForEachValue> ForEachValue for ($($name,)* $last) {
            #[allow(non_snake_case)]
            #[inline]
            fn for_each_value(
                self,
                ctx: &mut VmContext,
                mut callback: impl FnMut(Result<Value, RuntimeError>, &mut VmContext) -> Result<(), RuntimeError>,
            ) -> Result<(), RuntimeError> {
                    let ($($name,)* $last) = self;
                    $(callback($name.into_value(ctx), ctx)?;)*
                    $last.for_each_value(ctx, |result, ctx| callback(result, ctx))?;
                    Ok(())
            }
        }
    );
}

impl_for_each_value! { A, B }
impl_for_each_value! { A B, C }
impl_for_each_value! { A B C, D }
impl_for_each_value! { A B C D, E }
impl_for_each_value! { A B C D E, F }
impl_for_each_value! { A B C D E F, G }
impl_for_each_value! { A B C D E F G, H }
impl_for_each_value! { A B C D E F G H, I }
impl_for_each_value! { A B C D E F G H I, J }
impl_for_each_value! { A B C D E F G H I J, K }
impl_for_each_value! { A B C D E F G H I J K, L }

pub trait FromValues: Sized {
    fn from_values(
        ctx: &mut VmContext,
        pull_value: impl FnMut(&mut VmContext) -> Option<Value>,
    ) -> Result<Self, RuntimeError>;
}

impl FromValues for () {
    #[inline]
    fn from_values(
        _: &mut VmContext,
        _: impl FnMut(&mut VmContext) -> Option<Value>,
    ) -> Result<Self, RuntimeError> {
        Ok(())
    }
}

impl<T: FromValue> FromValues for T {
    #[inline]
    fn from_values(
        ctx: &mut VmContext,
        mut pull_value: impl FnMut(&mut VmContext) -> Option<Value>,
    ) -> Result<Self, RuntimeError> {
        let value = pull_value(ctx).unwrap_or_default();
        Self::from_value(value, ctx)
    }
}

impl<T: FromValue> FromValues for MultiValue<T> {
    #[inline]
    fn from_values(
        ctx: &mut VmContext,
        mut pull_value: impl FnMut(&mut VmContext) -> Option<Value>,
    ) -> Result<Self, RuntimeError> {
        let mut values = ThinVec::new();

        while let Some(value) = pull_value(ctx) {
            values.push(T::from_value(value, ctx)?);
        }

        Ok(values.into())
    }
}

macro_rules! impl_from_values {
    ($($name:ident)*, $last:ident) => (
        impl<$($name: FromValue,)* $last: FromValues> FromValues for ($($name,)* $last,) {
            #[inline]
            fn from_values(
                ctx: &mut VmContext,
                #[allow(unused)]
                mut pull_value: impl FnMut(&mut VmContext) -> Option<Value>,
            ) -> Result<Self, RuntimeError> {
                  Ok((
                    $($name::from_value(pull_value(ctx).unwrap_or_default(), ctx)?,)*
                    $last::from_values(ctx, pull_value)?,
                ))
            }
        }
    );
}

impl_from_values! { , A }
impl_from_values! { A, B }
impl_from_values! { A B, C }
impl_from_values! { A B C, D }
impl_from_values! { A B C D, E }
impl_from_values! { A B C D E, F }
impl_from_values! { A B C D E F, G }
impl_from_values! { A B C D E F G, H }
impl_from_values! { A B C D E F G H, I }
impl_from_values! { A B C D E F G H I, J }
impl_from_values! { A B C D E F G H I J, K }
impl_from_values! { A B C D E F G H I J K, L }
