use super::execution::ExecutionContext;
use super::heap::{Heap, HeapKey, HeapValue};
use super::value_stack::{Primitive, StackValue};
use super::{ByteString, FromMulti, FunctionRef, IntoMulti, StringRef, TableRef, Vm};
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData};
use crate::languages::lua::parse_number;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Primitive(Primitive),
    String(StringRef),
    Table(TableRef),
    Function(FunctionRef),
}

impl Default for Value {
    #[inline]
    fn default() -> Self {
        Self::Primitive(Primitive::Nil)
    }
}

impl Value {
    #[inline]
    pub fn is_nil(&self) -> bool {
        matches!(self, Self::Primitive(Primitive::Nil))
    }

    #[inline]
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Primitive(primitive) => match primitive {
                Primitive::Nil => "nil",
                Primitive::Bool(_) => "boolean",
                Primitive::Integer(_) => "number",
                Primitive::Float(_) => "number",
            },
            Value::String(_) => "string",
            Value::Table(_) => "table",
            Value::Function(_) => "function",
        }
    }

    #[inline]
    pub fn as_primitive(&self) -> Option<Primitive> {
        if let Value::Primitive(primitive) = self {
            Some(*primitive)
        } else {
            None
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

    pub fn call<A: IntoMulti, R: FromMulti>(
        &self,
        args: A,
        vm: &mut Vm,
    ) -> Result<R, RuntimeError> {
        let old_stack_size = vm.execution_data.tracked_stack_size;

        let args = args.into_multi(vm)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        for value in &args.values {
            value.test_validity(&vm.execution_data.heap)?;
        }

        let result = ExecutionContext::new_value_call(self.to_stack_value(), args, vm)
            .map_err(RuntimeError::from)
            .and_then(|context| {
                vm.execution_stack.push(context);
                ExecutionContext::resume(vm)
            });

        vm.execution_data.tracked_stack_size = old_stack_size;

        let multi = result?;
        R::from_multi(multi, vm)
    }

    #[inline]
    pub fn is_greater_than(&self, other: &Value, vm: &mut Vm) -> Result<bool, RuntimeError> {
        other.is_less_than(self, vm)
    }

    #[inline]
    pub fn is_greater_than_eq(&self, other: &Value, vm: &mut Vm) -> Result<bool, RuntimeError> {
        other.is_less_than_eq(self, vm)
    }

    pub fn is_less_than(&self, other: &Value, vm: &mut Vm) -> Result<bool, RuntimeError> {
        let lt = vm.metatable_keys().lt.0.key();

        if let Some(result) = self.try_binary_metamethods(other, lt, vm) {
            return result;
        };

        let primitive_l = match self {
            Value::Primitive(primitive) => primitive,
            Value::String(l) => {
                let Value::String(r) = other else {
                    return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare));
                };

                return Ok(l.fetch(vm)?.as_bytes() < r.fetch(vm)?.as_bytes());
            }
            _ => return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare)),
        };

        let primitive_r = match other {
            Value::Primitive(primitive) => primitive,
            _ => return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare)),
        };

        let value = match (primitive_l, primitive_r) {
            (Primitive::Float(l), Primitive::Float(r)) => *l < *r,
            (Primitive::Integer(l), Primitive::Integer(r)) => *l < *r,
            (Primitive::Float(l), Primitive::Integer(r)) => *l < (*r as f64),
            (Primitive::Integer(l), Primitive::Float(r)) => (*l as f64) < *r,
            _ => return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare)),
        };

        Ok(value)
    }

    pub fn is_less_than_eq(&self, other: &Value, vm: &mut Vm) -> Result<bool, RuntimeError> {
        let le = vm.metatable_keys().le.0.key();

        if let Some(result) = self.try_binary_metamethods(other, le, vm) {
            return result;
        };

        let primitive_l = match self {
            Value::Primitive(primitive) => primitive,
            Value::String(l) => {
                let Value::String(r) = other else {
                    return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare));
                };

                return Ok(l.fetch(vm)?.as_bytes() <= r.fetch(vm)?.as_bytes());
            }
            _ => return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare)),
        };

        let primitive_r = match other {
            Value::Primitive(primitive) => primitive,
            _ => return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare)),
        };

        let value = match (primitive_l, primitive_r) {
            (Primitive::Float(l), Primitive::Float(r)) => *l <= *r,
            (Primitive::Integer(l), Primitive::Integer(r)) => *l <= *r,
            (Primitive::Float(l), Primitive::Integer(r)) => *l <= (*r as f64),
            (Primitive::Integer(l), Primitive::Float(r)) => (*l as f64) <= *r,
            _ => return Err(RuntimeError::from(RuntimeErrorData::InvalidCompare)),
        };

        Ok(value)
    }

    pub fn is_eq(&self, other: &Value, vm: &mut Vm) -> Result<bool, RuntimeError> {
        let eq = vm.metatable_keys().eq.0.key();

        if let Some(result) = self.try_binary_metamethods(other, eq, vm) {
            return result;
        };

        let primitive_l = match self {
            Value::Primitive(primitive) => primitive,
            _ => return Ok(self == other),
        };

        let primitive_r = match other {
            Value::Primitive(primitive) => primitive,
            _ => return Ok(self == other),
        };

        let value = match (primitive_l, primitive_r) {
            (Primitive::Float(l), Primitive::Float(r)) => *l == *r,
            (Primitive::Integer(l), Primitive::Integer(r)) => *l == *r,
            (Primitive::Float(l), Primitive::Integer(r)) => *l == (*r as f64),
            (Primitive::Integer(l), Primitive::Float(r)) => (*l as f64) <= *r,
            _ => return Ok(self == other),
        };

        Ok(value)
    }

    fn try_binary_metamethods<T: FromMulti>(
        &self,
        other: &Value,
        method_name: HeapKey,
        vm: &mut Vm,
    ) -> Option<Result<T, RuntimeError>> {
        let method_name = method_name.into();

        if let Some(key) = self.heap_key() {
            let heap = &vm.execution_data.heap;
            if let Some(key) = heap.get_metamethod(key, method_name) {
                return Some(vm.call_function_key(key, (self.clone(), other.clone())));
            }
        }

        if let Some(key) = other.heap_key() {
            let heap = &vm.execution_data.heap;
            if let Some(key) = heap.get_metamethod(key, method_name) {
                return Some(vm.call_function_key(key, (self.clone(), other.clone())));
            }
        }

        None
    }

    pub(crate) fn to_stack_value(&self) -> StackValue {
        match self {
            Value::Primitive(primitive) => (*primitive).into(),
            Value::String(heap_ref) => heap_ref.0.key().into(),
            Value::Table(heap_ref) => heap_ref.0.key().into(),
            Value::Function(heap_ref) => heap_ref.0.key().into(),
        }
    }

    /// Expects stack values to made from within the same vm as the heap
    pub(crate) fn from_stack_value(heap: &mut Heap, value: StackValue) -> Value {
        match value {
            StackValue::Primitive(primitive) => Value::Primitive(primitive),
            StackValue::HeapValue(key) => {
                // stack values should be made from within the same vm as the heap
                let value = heap.get(key).unwrap();

                match value {
                    HeapValue::Bytes(_) => Value::String(StringRef(heap.create_ref(key))),
                    HeapValue::Table(_) => Value::Table(TableRef(heap.create_ref(key))),
                    HeapValue::Function(_) | HeapValue::NativeFunction(_) => {
                        Value::Function(FunctionRef(heap.create_ref(key)))
                    }
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
        let Some(key) = self.heap_key() else {
            return Ok(());
        };

        if heap.get(key).is_none() {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey));
        }

        Ok(())
    }

    pub(crate) fn heap_key(&self) -> Option<HeapKey> {
        match self {
            Value::Primitive(_) => None,
            Value::String(string_ref) => Some(string_ref.0.key()),
            Value::Table(table_ref) => Some(table_ref.0.key()),
            Value::Function(function_ref) => Some(function_ref.0.key()),
        }
    }
}

impl From<Primitive> for Value {
    #[inline]
    fn from(value: Primitive) -> Self {
        Self::Primitive(value)
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

pub trait IntoValue {
    fn into_value(self, vm: &mut Vm) -> Result<Value, RuntimeError>;
}

impl IntoValue for Value {
    #[inline]
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(self)
    }
}

impl IntoValue for Primitive {
    #[inline]
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(self.into())
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    #[inline]
    fn into_value(self, vm: &mut Vm) -> Result<Value, RuntimeError> {
        match self {
            Some(v) => v.into_value(vm),
            None => Ok(Value::Primitive(Primitive::Nil)),
        }
    }
}

impl IntoValue for StringRef {
    #[inline]
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(Value::String(self))
    }
}

impl IntoValue for TableRef {
    #[inline]
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(Value::Table(self))
    }
}

impl IntoValue for FunctionRef {
    #[inline]
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(Value::Function(self))
    }
}

impl IntoValue for String {
    #[inline]
    fn into_value(self, vm: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(Value::String(vm.intern_string(self.as_bytes())))
    }
}

impl<'a> IntoValue for &'a str {
    #[inline]
    fn into_value(self, vm: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(Value::String(vm.intern_string(self.as_bytes())))
    }
}

impl IntoValue for bool {
    #[inline]
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        Ok(Value::Primitive(Primitive::Bool(self)))
    }
}

macro_rules! into_int_value {
    ($name:ty) => {
        impl IntoValue for $name {
            #[inline]
            fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
                Ok(Value::Primitive(Primitive::Integer(self as _)))
            }
        }
    };
}

macro_rules! into_float_value {
    ($name:ty) => {
        impl IntoValue for $name {
            #[inline]
            fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
                Ok(Value::Primitive(Primitive::Float(self as _)))
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
    fn from_value(value: Value, vm: &mut Vm) -> Result<Self, RuntimeError>;
}

impl FromValue for Value {
    #[inline]
    fn from_value(value: Value, _: &mut Vm) -> Result<Self, RuntimeError> {
        Ok(value)
    }
}

impl FromValue for Primitive {
    #[inline]
    fn from_value(value: Value, _: &mut Vm) -> Result<Self, RuntimeError> {
        if let Value::Primitive(primitive) = value {
            Ok(primitive)
        } else {
            Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "Primitive",
                message: None,
            }
            .into())
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    #[inline]
    fn from_value(value: Value, vm: &mut Vm) -> Result<Self, RuntimeError> {
        match value {
            Value::Primitive(Primitive::Nil) => Ok(None),
            _ => Ok(Some(T::from_value(value, vm)?)),
        }
    }
}

impl FromValue for StringRef {
    #[inline]
    fn from_value(value: Value, _: &mut Vm) -> Result<Self, RuntimeError> {
        if let Value::String(string_ref) = value {
            Ok(string_ref)
        } else {
            Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "StringRef",
                message: None,
            }
            .into())
        }
    }
}

impl FromValue for TableRef {
    #[inline]
    fn from_value(value: Value, _: &mut Vm) -> Result<Self, RuntimeError> {
        if let Value::Table(table_ref) = value {
            Ok(table_ref)
        } else {
            Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "TableRef",
                message: None,
            }
            .into())
        }
    }
}

impl FromValue for FunctionRef {
    #[inline]
    fn from_value(value: Value, _: &mut Vm) -> Result<Self, RuntimeError> {
        if let Value::Function(function_ref) = value {
            Ok(function_ref)
        } else {
            Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "FunctionRef",
                message: None,
            }
            .into())
        }
    }
}

impl FromValue for String {
    #[inline]
    fn from_value(value: Value, vm: &mut Vm) -> Result<Self, RuntimeError> {
        let Value::String(string_ref) = value else {
            return Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "String",
                message: None,
            }
            .into());
        };

        let s = string_ref.fetch(vm)?.to_string_lossy();
        Ok(s.into_owned())
    }
}

impl FromValue for ByteString {
    #[inline]
    fn from_value(value: Value, vm: &mut Vm) -> Result<Self, RuntimeError> {
        let Value::String(string_ref) = value else {
            return Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "String",
                message: None,
            }
            .into());
        };

        Ok(string_ref.fetch(vm)?.clone())
    }
}

impl FromValue for bool {
    #[inline]
    fn from_value(value: Value, _: &mut Vm) -> Result<Self, RuntimeError> {
        Ok(!matches!(
            value,
            Value::Primitive(Primitive::Nil | Primitive::Bool(false))
        ))
    }
}

macro_rules! number_from_value {
    ($name:ty) => {
        impl FromValue for $name {
            #[inline]
            fn from_value(value: Value, vm: &mut Vm) -> Result<Self, RuntimeError> {
                match &value {
                    Value::Primitive(Primitive::Integer(i)) => return Ok(*i as _),
                    Value::Primitive(Primitive::Float(f)) => return Ok(*f as _),
                    Value::String(s) => {
                        let number = parse_number(&*s.fetch(vm)?.to_string_lossy());

                        match number {
                            Primitive::Integer(i) => return Ok(i as _),
                            Primitive::Float(f) => return Ok(f as _),
                            _ => {}
                        }
                    }
                    _ => {}
                };

                Err(RuntimeErrorData::FromLuaConversionError {
                    from: value.type_name(),
                    to: stringify!($name),
                    message: None,
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
