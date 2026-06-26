use super::RuntimeErrorData;
use super::stack_trace::StackTrace;
use crate::tag_native_type;
use crate::values::ByteString;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RuntimeError {
    pub trace: StackTrace,
    pub data: RuntimeErrorData,
}

tag_native_type!(RuntimeError);

impl RuntimeError {
    pub fn new_bad_argument(position: usize, mut error: RuntimeError) -> Self {
        error.data = RuntimeErrorData::BadArgument {
            position: position as _,
            reason: error.data.into(),
        };

        error
    }

    pub fn new_string(message: String) -> Self {
        RuntimeError::from(RuntimeErrorData::ByteString(message.as_str().into()))
    }

    pub fn new_static_string(message: &'static str) -> Self {
        RuntimeError::from(RuntimeErrorData::ByteString(message.into()))
    }

    pub fn new_byte_string(message: ByteString) -> RuntimeError {
        RuntimeError::from(RuntimeErrorData::ByteString(message))
    }
}

impl<T: Into<RuntimeErrorData>> From<T> for RuntimeError {
    #[inline]
    fn from(data: T) -> Self {
        Self {
            trace: Default::default(),
            data: data.into(),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\n{}", self.data, self.trace)
    }
}
