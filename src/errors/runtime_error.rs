use super::stack_trace::StackTrace;
use super::RuntimeErrorData;
use crate::interpreter::ByteString;
use std::borrow::Cow;

#[derive(Debug)]
pub struct RuntimeError {
    pub trace: StackTrace,
    pub data: RuntimeErrorData,
}

impl RuntimeError {
    pub fn new_bad_argument(position: usize, mut error: RuntimeError) -> Self {
        error.data = RuntimeErrorData::BadArgument {
            position,
            reason: error.data.into(),
        };

        error
    }

    pub fn new_string(message: String) -> Self {
        RuntimeError::from(RuntimeErrorData::String(Cow::Owned(message)))
    }

    pub fn new_static_string(message: &'static str) -> Self {
        RuntimeError::from(RuntimeErrorData::String(Cow::Borrowed(message)))
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
        let frames = self.trace.frames();

        write!(f, "error: {:?}", self.data)?;

        for frame in frames {
            write!(
                f,
                "\n  at {}:{}:{}",
                frame.source_name, frame.line, frame.col
            )?;
        }

        Ok(())
    }
}
