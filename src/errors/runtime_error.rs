use super::RuntimeErrorData;
use super::stack_trace::StackTrace;
use crate::tag_native_type;
use crate::values::ByteString;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

    /// Returns an error containing [RuntimeErrorData::InvalidInternalState] in release builds, panics in debug builds.
    #[track_caller]
    pub fn new_invalid_internal_state() -> RuntimeError {
        RuntimeErrorData::new_invalid_internal_state().into()
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
        write!(f, "{}", self.data)?;

        if !self.trace.frames().is_empty() {
            write!(f, "\n{}", self.trace)?;
        }

        Ok(())
    }
}
