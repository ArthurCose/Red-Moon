use super::stack_trace::StackTrace;
use super::RuntimeErrorData;
use std::borrow::Cow;

#[derive(Debug)]
pub struct RuntimeError {
    pub data: RuntimeErrorData,
    pub trace: StackTrace,
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
}

impl From<RuntimeErrorData> for RuntimeError {
    #[inline]
    fn from(data: RuntimeErrorData) -> Self {
        Self {
            trace: Default::default(),
            data,
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
