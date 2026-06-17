use super::{SourcePosition, SyntaxError, SyntaxErrorData};
use crate::languages::lua::LuaTokenLabel;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct LuaCompilationError {
    pub source_position: SourcePosition,
    pub data: LuaCompilationErrorData,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum LuaCompilationErrorData {
    SyntaxError(SyntaxErrorData<LuaTokenLabel>),
    // semantic errors
    UnexpectedVariadic,
    UnexpectedBreak,
    UnresolvedGoto,
    RedefinedLabel,
    GotoSkipsLocalDeclaration,
    ReachedLocalsLimit,
    ReachedCaptureLimit,
    ReachedFunctionLimit,
    ReachedNumberLimit,
    ReachedRegisterLimit,
    InvalidNumber,
}

impl std::error::Error for LuaCompilationError {}

impl std::fmt::Display for LuaCompilationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let line = self.source_position.line;
        let col = self.source_position.col;

        write!(f, "{line}:{col}: {}", self.data)
    }
}

impl std::fmt::Display for LuaCompilationErrorData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::SyntaxError(err) => std::fmt::Display::fmt(err, f),
            Self::UnexpectedVariadic => {
                write!(f, "cannot use variadic used outside of a vararg function")
            }
            Self::UnexpectedBreak => {
                write!(f, "break used outside of a loop")
            }
            Self::UnresolvedGoto => {
                write!(f, "label isn't visible")
            }
            Self::RedefinedLabel => {
                write!(f, "label is already defined")
            }
            Self::GotoSkipsLocalDeclaration => {
                write!(f, "goto skips local declaration")
            }
            Self::ReachedLocalsLimit => {
                write!(f, "too many local variables (limit is 200)")
            }
            Self::ReachedCaptureLimit => {
                write!(f, "too many local variables (limit is 200)")
            }
            Self::ReachedFunctionLimit => {
                write!(f, "too many functions (limit is 2^16-1)")
            }
            Self::ReachedNumberLimit => {
                write!(f, "too many numbers (limit is 2^16-1 per function)")
            }
            Self::ReachedRegisterLimit => {
                write!(f, "out of registers, reduce loops or simplify expressions")
            }
            Self::InvalidNumber => {
                write!(f, "malformed number")
            }
        }
    }
}

impl From<SyntaxError<LuaTokenLabel>> for LuaCompilationError {
    fn from(value: SyntaxError<LuaTokenLabel>) -> Self {
        Self {
            source_position: value.source_position,
            data: LuaCompilationErrorData::SyntaxError(value.data),
        }
    }
}

impl From<SyntaxErrorData<LuaTokenLabel>> for LuaCompilationErrorData {
    fn from(value: SyntaxErrorData<LuaTokenLabel>) -> Self {
        LuaCompilationErrorData::SyntaxError(value)
    }
}
