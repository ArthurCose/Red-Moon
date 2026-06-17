use super::SyntaxError;
use crate::languages::{line_and_col, lua::LuaTokenLabel};

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum LuaCompilationError {
    SyntaxError(SyntaxError<LuaTokenLabel>),
    // semantic errors
    UnexpectedVariadic {
        offset: usize,
        line: usize,
        col: usize,
    },
    UnexpectedBreak {
        offset: usize,
        line: usize,
        col: usize,
    },
    UnresolvedGoto {
        offset: usize,
        line: usize,
        col: usize,
    },
    RedefinedLabel {
        offset: usize,
        line: usize,
        col: usize,
    },
    GotoSkipsLocalDeclaration {
        offset: usize,
        line: usize,
        col: usize,
    },
    ReachedLocalsLimit {
        offset: usize,
        line: usize,
        col: usize,
    },
    ReachedCaptureLimit {
        offset: usize,
        line: usize,
        col: usize,
    },
    ReachedFunctionLimit,
    ReachedNumberLimit {
        offset: usize,
        line: usize,
        col: usize,
    },
    ReachedRegisterLimit {
        offset: usize,
        line: usize,
        col: usize,
    },
    InvalidNumber {
        offset: usize,
        line: usize,
        col: usize,
    },
}

impl LuaCompilationError {
    pub fn new_unexpected_variadic(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::UnexpectedVariadic { offset, line, col }
    }

    pub fn new_unexpected_break(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::UnexpectedBreak { offset, line, col }
    }

    pub fn new_unresolved_goto(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::UnresolvedGoto { offset, line, col }
    }

    pub fn new_redefined_label(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::RedefinedLabel { offset, line, col }
    }

    pub fn new_goto_skips_local_declaration(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::GotoSkipsLocalDeclaration { offset, line, col }
    }

    pub fn new_too_many_locals(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::ReachedLocalsLimit { offset, line, col }
    }

    pub fn new_reached_capture_limit(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::ReachedCaptureLimit { offset, line, col }
    }

    pub fn new_reached_number_limit(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::ReachedNumberLimit { offset, line, col }
    }

    pub fn new_reached_register_limit(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::ReachedRegisterLimit { offset, line, col }
    }

    pub fn new_invalid_number(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::InvalidNumber { offset, line, col }
    }
}

impl std::fmt::Display for LuaCompilationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::SyntaxError(err) => std::fmt::Display::fmt(err, f),
            Self::UnexpectedVariadic { line, col, .. } => write!(
                f,
                "{}:{}: cannot use variadic used outside of a vararg function",
                line, col
            ),
            Self::UnexpectedBreak { line, col, .. } => {
                write!(f, "{}:{}: break used outside of a loop", line, col)
            }
            Self::UnresolvedGoto { line, col, .. } => {
                write!(f, "{}:{}: label isn't visible", line, col)
            }
            Self::RedefinedLabel { line, col, .. } => {
                write!(f, "{}:{}: label is already defined", line, col)
            }
            Self::GotoSkipsLocalDeclaration { line, col, .. } => {
                write!(f, "{}:{}: goto skips local declaration", line, col)
            }
            Self::ReachedLocalsLimit { line, col, .. } => {
                write!(
                    f,
                    "{}:{}: too many local variables (limit is 200)",
                    line, col
                )
            }
            Self::ReachedCaptureLimit { line, col, .. } => {
                write!(
                    f,
                    "{}:{}: too many local variables (limit is 200)",
                    line, col
                )
            }
            Self::ReachedFunctionLimit => {
                write!(f, "too many functions (limit is 2^16-1)")
            }
            Self::ReachedNumberLimit { line, col, .. } => {
                write!(
                    f,
                    "{}:{}: too many numbers (limit is 2^16-1 per function)",
                    line, col
                )
            }
            Self::ReachedRegisterLimit { line, col, .. } => {
                write!(
                    f,
                    "{}:{}: out of registers, reduce loops or simplify expressions",
                    line, col
                )
            }
            Self::InvalidNumber { line, col, .. } => {
                write!(f, "{}:{}: malformed number", line, col)
            }
        }
    }
}

impl std::error::Error for LuaCompilationError {}

impl From<SyntaxError<LuaTokenLabel>> for LuaCompilationError {
    fn from(value: SyntaxError<LuaTokenLabel>) -> Self {
        Self::SyntaxError(value)
    }
}
