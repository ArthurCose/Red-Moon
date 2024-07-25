use super::{IllegalInstruction, NativeError};
use crate::interpreter::{ByteString, MultiValue};
use std::borrow::Cow;
use std::rc::Rc;

#[derive(Debug, PartialEq, Clone)]
pub enum RuntimeErrorData {
    IllegalInstruction(IllegalInstruction),
    InvalidRef,
    MissingMain,
    StackOverflow,
    NotAFunction,
    MetatableChainTooLong,
    AttemptToIndexInvalid,
    AttemptToConcatInvalid,
    NoLength,
    InvalidCompare,
    InvalidArithmetic,
    DivideByZero,
    NoIntegerRepresentation(f64),
    InitialValueMustBeNumber,
    LimitMustBeNumber,
    StepMustBeNumber,
    OutOfBounds,
    DeadCoroutine,
    ResumedNonSuspendedCoroutine,
    Yield(MultiValue),
    InvalidYield,
    UnhandledYield,
    YieldMissingContinuation,
    /// Bad argument received from Lua (usually when calling a function).
    ///
    /// This error can help to identify the argument that caused the error
    /// (which is stored in the corresponding field).
    BadArgument {
        /// Argument position (usually starts from 1).
        position: usize,
        /// Underlying error returned when converting argument to a Lua value.
        reason: Rc<RuntimeErrorData>,
    },
    /// A Lua value could not be converted to the expected Rust type.
    FromLuaConversionError {
        /// Name of the Lua type that could not be converted.
        from: &'static str,
        /// Name of the Rust type that could not be created.
        to: &'static str,
        /// A string containing more detailed error information.
        message: Option<String>,
    },
    NativeError(NativeError),
    String(Cow<'static, str>),
    ByteString(ByteString),
}

impl From<IllegalInstruction> for RuntimeErrorData {
    fn from(value: IllegalInstruction) -> Self {
        Self::IllegalInstruction(value)
    }
}
