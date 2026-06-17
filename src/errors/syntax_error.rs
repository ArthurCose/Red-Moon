use crate::errors::SourcePosition;
use crate::languages::Token;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxError<Label> {
    pub source_position: SourcePosition,
    pub data: SyntaxErrorData<Label>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyntaxErrorData<Label> {
    // lexer
    UnexpectedCharacter,
    BadLexer { label: Label, len: usize },
    BadIgnorer { len: usize },
    // parser
    UnexpectedToken { label: Label },
    UnexpectedEOF,
}

impl<Label> SyntaxError<Label> {
    pub fn new_unexpected_token(source: &str, token: Token<Label>) -> Self {
        SyntaxError {
            source_position: SourcePosition::new(source, token.offset),
            data: SyntaxErrorData::UnexpectedToken { label: token.label },
        }
    }
}

impl<Label: std::fmt::Debug> std::error::Error for SyntaxError<Label> {}

impl<Label: std::fmt::Debug> std::fmt::Display for SyntaxError<Label> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let line = self.source_position.line;
        let col = self.source_position.col;

        write!(f, "{line}:{col}: {}", self.data)
    }
}

impl<Label: std::fmt::Debug> std::fmt::Display for SyntaxErrorData<Label> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::UnexpectedCharacter => {
                write!(f, "unexpected character")
            }
            Self::BadLexer { label, .. } => write!(
                f,
                "a lexer creating {label:?} tokens returned a length that would include characters past end",
            ),
            Self::BadIgnorer { .. } => write!(
                f,
                "an ignorer returned a length that would include characters past end",
            ),
            Self::UnexpectedToken { label } => {
                write!(f, "unexpected {label:?}")
            }
            Self::UnexpectedEOF => write!(f, "unexpected eof"),
        }
    }
}
