use super::execution::ExecutionContext;
use super::heap::{Heap, HeapKey, HeapValue};
use super::value_stack::StackValue;
use super::vm::VmContext;
use super::{
    ByteString, CoroutineRef, FromMulti, FunctionRef, IntoMulti, Number, StringRef, TableRef,
};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::languages::lua::parse_number;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TypeName {
    Nil,
    Bool,
    Number,
    String,
    Table,
    Function,
    Thread,
}

impl TypeName {
    pub fn as_str(self) -> &'static str {
        match self {
            TypeName::Nil => "nil",
            TypeName::Bool => "boolean",
            TypeName::Number => "number",
            TypeName::String => "string",
            TypeName::Table => "table",
            TypeName::Function => "function",
            TypeName::Thread => "thread",
        }
    }
}

impl std::fmt::Display for TypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Nil,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(StringRef),
    Table(TableRef),
    Function(FunctionRef),
    Coroutine(CoroutineRef),
}

impl Eq for Value {}

impl Default for Value {
    #[inline]
    fn default() -> Self {
        Self::Nil
    }
}

impl Value {
    #[inline]
    pub fn type_name(&self) -> TypeName {
        match self {
            Value::Nil => TypeName::Nil,
            Value::Bool(_) => TypeName::Bool,
            Value::Integer(_) | Value::Float(_) => TypeName::Number,
            Value::String(_) => TypeName::String,
            Value::Table(_) => TypeName::Table,
            Value::Function(_) => TypeName::Function,
            Value::Coroutine(_) => TypeName::Thread,
        }
    }

    #[inline]
    pub fn is_nil(&self) -> bool {
        matches!(self, Self::Nil)
    }

    #[inline]
    pub fn as_number(&self) -> Option<Number> {
        match self {
            Value::Integer(i) => Some(Number::Integer(*i)),
            Value::Float(f) => Some(Number::Float(*f)),
            _ => None,
        }
    }

    #[inline]
    pub fn as_string_ref(&self) -> Option<&StringRef> {
        if let Value::String(string_ref) = self {
            Some(string_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_table_ref(&self) -> Option<&TableRef> {
        if let Value::Table(table_ref) = self {
            Some(table_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_function_ref(&self) -> Option<&FunctionRef> {
        if let Value::Function(function_ref) = self {
            Some(function_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_coroutine_ref(&self) -> Option<&CoroutineRef> {
        if let Value::Coroutine(coroutine_ref) = self {
            Some(coroutine_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn into_string_ref(self) -> Option<StringRef> {
        if let Value::String(string_ref) = self {
            Some(string_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn into_table_ref(self) -> Option<TableRef> {
        if let Value::Table(table_ref) = self {
            Some(table_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn into_function_ref(self) -> Option<FunctionRef> {
        if let Value::Function(function_ref) = self {
            Some(function_ref)
        } else {
            None
        }
    }

    #[inline]
    pub fn into_coroutine_ref(self) -> Option<CoroutineRef> {
        if let Value::Coroutine(coroutine_ref) = self {
            Some(coroutine_ref)
        } else {
            None
        }
    }

    pub fn call<A: IntoMulti, R: FromMulti>(
        &self,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<R, RuntimeError> {
        let args = args.into_multi(ctx)?;

        // must test validity of every arg, since invalid keys in the ctx will cause a panic
        for value in &args.values {
            value.test_validity(&ctx.vm.execution_data.heap)?;
        }

        let execution = ExecutionContext::new_value_call(self.to_stack_value(), args, ctx.vm)?;

        ctx.vm.execution_stack.push(execution);
        let multi = ExecutionContext::resume(ctx.vm)?;

        R::from_multi(multi, ctx)
    }

    #[inline]
    pub fn is_greater_than(
        &self,
        other: &Value,
        ctx: &mut VmContext,
    ) -> Result<bool, RuntimeError> {
        other.is_less_than(self, ctx)
    }

    #[inline]
    pub fn is_greater_than_eq(
        &self,
        other: &Value,
        ctx: &mut VmContext,
    ) -> Result<bool, RuntimeError> {
        other.is_less_than_eq(self, ctx)
    }

    pub fn is_less_than(&self, other: &Value, ctx: &mut VmContext) -> Result<bool, RuntimeError> {
        let lt = ctx.metatable_keys().lt.0.key();

        if let Some(result) = self.try_binary_metamethods(other, lt, ctx) {
            return result;
        };

        let value = match (self, other) {
            (Value::String(l), _) => {
                let Value::String(r) = other else {
                    return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare(
                        self.type_name(),
                        other.type_name(),
                    )));
                };

                return Ok(l.fetch(ctx)?.as_bytes() < r.fetch(ctx)?.as_bytes());
            }
            (Value::Float(l), Value::Float(r)) => *l < *r,
            (Value::Integer(l), Value::Integer(r)) => *l < *r,
            (Value::Float(l), Value::Integer(r)) => *l < (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) < *r,
            _ => {
                return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare(
                    self.type_name(),
                    other.type_name(),
                )))
            }
        };

        Ok(value)
    }

    pub fn is_less_than_eq(
        &self,
        other: &Value,
        ctx: &mut VmContext,
    ) -> Result<bool, RuntimeError> {
        let le = ctx.metatable_keys().le.0.key();

        if let Some(result) = self.try_binary_metamethods(other, le, ctx) {
            return result;
        };

        let value = match (self, other) {
            (Value::String(l), _) => {
                let Value::String(r) = other else {
                    return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare(
                        self.type_name(),
                        other.type_name(),
                    )));
                };

                return Ok(l.fetch(ctx)?.as_bytes() <= r.fetch(ctx)?.as_bytes());
            }
            (Value::Float(l), Value::Float(r)) => *l <= *r,
            (Value::Integer(l), Value::Integer(r)) => *l <= *r,
            (Value::Float(l), Value::Integer(r)) => *l <= (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) <= *r,
            _ => {
                return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare(
                    self.type_name(),
                    other.type_name(),
                )))
            }
        };

        Ok(value)
    }

    pub fn is_eq(&self, other: &Value, ctx: &mut VmContext) -> Result<bool, RuntimeError> {
        let eq = ctx.metatable_keys().eq.0.key();

        if let Some(result) = self.try_binary_metamethods(other, eq, ctx) {
            return result;
        };

        let value = match (self, other) {
            (Value::Float(l), Value::Float(r)) => *l == *r,
            (Value::Integer(l), Value::Integer(r)) => *l == *r,
            (Value::Float(l), Value::Integer(r)) => *l == (*r as f64),
            (Value::Integer(l), Value::Float(r)) => (*l as f64) <= *r,
            _ => return Ok(self == other),
        };

        Ok(value)
    }

    fn try_binary_metamethods<T: FromMulti>(
        &self,
        other: &Value,
        method_name: HeapKey,
        ctx: &mut VmContext,
    ) -> Option<Result<T, RuntimeError>> {
        let method_name = method_name.into();

        if let Some(key) = self.heap_key() {
            let heap = &ctx.vm.execution_data.heap;
            if let Some(key) = heap.get_metamethod(key, method_name) {
                return Some(ctx.call_function_key(key, (self.clone(), other.clone())));
            }
        }

        if let Some(key) = other.heap_key() {
            let heap = &ctx.vm.execution_data.heap;
            if let Some(key) = heap.get_metamethod(key, method_name) {
                return Some(ctx.call_function_key(key, (self.clone(), other.clone())));
            }
        }

        None
    }

    pub(crate) fn to_stack_value(&self) -> StackValue {
        match self {
            Value::Nil => StackValue::Nil,
            Value::Bool(b) => StackValue::Bool(*b),
            Value::Integer(i) => StackValue::Integer(*i),
            Value::Float(f) => StackValue::Float(*f),
            Value::String(heap_ref) => heap_ref.0.key().into(),
            Value::Table(heap_ref) => heap_ref.0.key().into(),
            Value::Function(heap_ref) => heap_ref.0.key().into(),
            Value::Coroutine(heap_ref) => heap_ref.0.key().into(),
        }
    }

    /// Expects stack values to made from within the same ctx as the heap
    pub(crate) fn from_stack_value(heap: &mut Heap, value: StackValue) -> Value {
        match value {
            StackValue::Nil => Value::Nil,
            StackValue::Bool(b) => Value::Bool(b),
            StackValue::Integer(i) => Value::Integer(i),
            StackValue::Float(f) => Value::Float(f),
            StackValue::HeapValue(key) => {
                // stack values should be made from within the same ctx as the heap
                let value = heap.get(key).unwrap();

                match value {
                    HeapValue::Bytes(_) => Value::String(StringRef(heap.create_ref(key))),
                    HeapValue::Table(_) => Value::Table(TableRef(heap.create_ref(key))),
                    HeapValue::Function(_) | HeapValue::NativeFunction(_) => {
                        Value::Function(FunctionRef(heap.create_ref(key)))
                    }
                    HeapValue::Coroutine(_) => Value::Coroutine(CoroutineRef(heap.create_ref(key))),
                    HeapValue::StackValue(_) => {
                        // only StackValue::Pointer should point to StackValues
                        unreachable!()
                    }
                }
            }
            StackValue::Pointer(key) => {
                let HeapValue::StackValue(value) = heap.get(key).unwrap() else {
                    unreachable!()
                };

                Self::from_stack_value(heap, *value)
            }
        }
    }

    pub(crate) fn test_validity(&self, heap: &Heap) -> Result<(), RuntimeErrorData> {
        let valid = match self {
            Value::String(r) => matches!(heap.get(r.0.key()), Some(HeapValue::Bytes(_))),
            Value::Table(r) => matches!(heap.get(r.0.key()), Some(HeapValue::Table(_))),
            Value::Function(r) => matches!(
                heap.get(r.0.key()),
                Some(HeapValue::Function(_) | HeapValue::NativeFunction(_))
            ),
            Value::Coroutine(r) => matches!(heap.get(r.0.key()), Some(HeapValue::Coroutine(_))),
            Value::Nil | Value::Bool(_) | Value::Integer(_) | Value::Float(_) => return Ok(()),
        };

        if valid {
            Ok(())
        } else {
            Err(RuntimeErrorData::InvalidRef)
        }
    }

    pub(crate) fn heap_key(&self) -> Option<HeapKey> {
        match self {
            Value::Nil => None,
            Value::Bool(_) => None,
            Value::Integer(_) => None,
            Value::Float(_) => None,
            Value::String(string_ref) => Some(string_ref.0.key()),
            Value::Table(table_ref) => Some(table_ref.0.key()),
            Value::Function(function_ref) => Some(function_ref.0.key()),
            Value::Coroutine(coroutine_ref) => Some(coroutine_ref.0.key()),
        }
    }
}

impl From<StringRef> for Value {
    #[inline]
    fn from(value: StringRef) -> Self {
        Self::String(value)
    }
}

impl From<TableRef> for Value {
    #[inline]
    fn from(value: TableRef) -> Self {
        Self::Table(value)
    }
}

impl From<FunctionRef> for Value {
    #[inline]
    fn from(value: FunctionRef) -> Self {
        Self::Function(value)
    }
}

impl From<CoroutineRef> for Value {
    #[inline]
    fn from(value: CoroutineRef) -> Self {
        Self::Coroutine(value)
    }
}

pub trait IntoValue {
    fn into_value(self, ctx: &mut VmContext) -> Result<Value, RuntimeError>;
}

impl IntoValue for Value {
    #[inline]
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(self)
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    #[inline]
    fn into_value(self, ctx: &mut VmContext) -> Result<Value, RuntimeError> {
        match self {
            Some(v) => v.into_value(ctx),
            None => Ok(Value::Nil),
        }
    }
}

impl IntoValue for StringRef {
    #[inline]
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::String(self))
    }
}

impl IntoValue for TableRef {
    #[inline]
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::Table(self))
    }
}

impl IntoValue for FunctionRef {
    #[inline]
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::Function(self))
    }
}

impl IntoValue for CoroutineRef {
    #[inline]
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::Coroutine(self))
    }
}

impl IntoValue for String {
    #[inline]
    fn into_value(self, ctx: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::String(ctx.intern_string(self.as_bytes())))
    }
}

impl<'a> IntoValue for &'a str {
    #[inline]
    fn into_value(self, ctx: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::String(ctx.intern_string(self.as_bytes())))
    }
}

impl IntoValue for bool {
    #[inline]
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        Ok(Value::Bool(self))
    }
}

macro_rules! into_int_value {
    ($name:ty) => {
        impl IntoValue for $name {
            #[inline]
            fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
                Ok(Value::Integer(self as _))
            }
        }
    };
}

macro_rules! into_float_value {
    ($name:ty) => {
        impl IntoValue for $name {
            #[inline]
            fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
                Ok(Value::Float(self as _))
            }
        }
    };
}

into_int_value!(i8);
into_int_value!(i16);
into_int_value!(i32);
into_int_value!(i64);
into_int_value!(isize);
into_int_value!(u8);
into_int_value!(u16);
into_int_value!(u32);
into_int_value!(u64);
into_int_value!(usize);
into_float_value!(f32);
into_float_value!(f64);

pub trait FromValue: Sized {
    fn from_value(value: Value, ctx: &mut VmContext) -> Result<Self, RuntimeError>;
}

impl FromValue for Value {
    #[inline]
    fn from_value(value: Value, _: &mut VmContext) -> Result<Self, RuntimeError> {
        Ok(value)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    #[inline]
    fn from_value(value: Value, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        match value {
            Value::Nil => Ok(None),
            _ => Ok(Some(T::from_value(value, ctx)?)),
        }
    }
}

impl FromValue for StringRef {
    #[inline]
    fn from_value(value: Value, _: &mut VmContext) -> Result<Self, RuntimeError> {
        if let Value::String(string_ref) = value {
            Ok(string_ref)
        } else {
            Err(RuntimeErrorData::ExpectedType {
                expected: TypeName::String,
                received: value.type_name(),
            }
            .into())
        }
    }
}

impl FromValue for TableRef {
    #[inline]
    fn from_value(value: Value, _: &mut VmContext) -> Result<Self, RuntimeError> {
        if let Value::Table(table_ref) = value {
            Ok(table_ref)
        } else {
            Err(RuntimeErrorData::ExpectedType {
                expected: TypeName::Table,
                received: value.type_name(),
            }
            .into())
        }
    }
}

impl FromValue for FunctionRef {
    #[inline]
    fn from_value(value: Value, _: &mut VmContext) -> Result<Self, RuntimeError> {
        if let Value::Function(function_ref) = value {
            Ok(function_ref)
        } else {
            Err(RuntimeErrorData::ExpectedType {
                expected: TypeName::Function,
                received: value.type_name(),
            }
            .into())
        }
    }
}

impl FromValue for CoroutineRef {
    #[inline]
    fn from_value(value: Value, _: &mut VmContext) -> Result<Self, RuntimeError> {
        if let Value::Coroutine(coroutine_ref) = value {
            Ok(coroutine_ref)
        } else {
            Err(RuntimeErrorData::ExpectedType {
                expected: TypeName::Thread,
                received: value.type_name(),
            }
            .into())
        }
    }
}

impl FromValue for String {
    #[inline]
    fn from_value(value: Value, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        let Value::String(string_ref) = value else {
            return Err(RuntimeErrorData::ExpectedType {
                expected: TypeName::String,
                received: value.type_name(),
            }
            .into());
        };

        let s = string_ref.fetch(ctx)?.to_string_lossy();
        Ok(s.into_owned())
    }
}

impl FromValue for ByteString {
    #[inline]
    fn from_value(value: Value, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
        let Value::String(string_ref) = value else {
            return Err(RuntimeErrorData::ExpectedType {
                expected: TypeName::String,
                received: value.type_name(),
            }
            .into());
        };

        Ok(string_ref.fetch(ctx)?.clone())
    }
}

impl FromValue for bool {
    #[inline]
    fn from_value(value: Value, _: &mut VmContext) -> Result<Self, RuntimeError> {
        Ok(!matches!(value, Value::Nil | Value::Bool(false)))
    }
}

macro_rules! number_from_value {
    ($name:ty) => {
        impl FromValue for $name {
            #[inline]
            fn from_value(value: Value, ctx: &mut VmContext) -> Result<Self, RuntimeError> {
                match &value {
                    Value::Integer(i) => return Ok(*i as _),
                    Value::Float(f) => return Ok(*f as _),
                    Value::String(s) => {
                        if let Some(number) = parse_number(&*s.fetch(ctx)?.to_string_lossy()) {
                            match number {
                                Number::Integer(i) => return Ok(i as _),
                                Number::Float(f) => return Ok(f as _),
                            }
                        }
                    }
                    _ => {}
                };

                Err(RuntimeErrorData::ExpectedType {
                    expected: TypeName::Number,
                    received: value.type_name(),
                }
                .into())
            }
        }
    };
}

number_from_value!(i8);
number_from_value!(i16);
number_from_value!(i32);
number_from_value!(i64);
number_from_value!(isize);
number_from_value!(u8);
number_from_value!(u16);
number_from_value!(u32);
number_from_value!(u64);
number_from_value!(usize);
number_from_value!(f32);
number_from_value!(f64);
