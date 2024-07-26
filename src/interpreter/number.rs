use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{FromValue, IntoValue, Value, VmContext};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Number {
    Integer(i64),
    Float(f64),
}

impl From<Number> for Value {
    fn from(value: Number) -> Self {
        match value {
            Number::Integer(i) => Value::Integer(i),
            Number::Float(f) => Value::Float(f),
        }
    }
}

impl IntoValue for Number {
    fn into_value(self, _: &mut VmContext) -> Result<Value, RuntimeError> {
        match self {
            Number::Integer(i) => Ok(Value::Integer(i)),
            Number::Float(f) => Ok(Value::Float(f)),
        }
    }
}

impl FromValue for Number {
    fn from_value(value: Value, _: &mut VmContext) -> Result<Number, RuntimeError> {
        match value {
            Value::Integer(i) => Ok(Number::Integer(i)),
            Value::Float(f) => Ok(Number::Float(f)),
            _ => Err(RuntimeErrorData::ExpectedType {
                expected: "number",
                received: value.type_name(),
            }
            .into()),
        }
    }
}
