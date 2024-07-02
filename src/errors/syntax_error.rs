use crate::languages::{line_and_col, Token};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyntaxError<Label> {
    // lexer
    UnexpectedCharacter {
        offset: usize,
        line: usize,
        col: usize,
    },
    BadLexer {
        label: Label,
        offset: usize,
        line: usize,
        col: usize,
        final_offset: usize,
    },
    BadIgnorer {
        offset: usize,
        line: usize,
        col: usize,
        final_offset: usize,
    },
    // parser
    UnexpectedToken {
        label: Label,
        offset: usize,
        line: usize,
        col: usize,
    },
    UnexpectedEOF,
}

impl<Label> SyntaxError<Label> {
    pub fn new_unexpected_character(source: &str, offset: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::UnexpectedCharacter { offset, line, col }
    }

    pub fn new_bad_lexer(source: &str, label: Label, offset: usize, len: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::BadLexer {
            label,
            offset,
            line,
            col,
            final_offset: offset + len,
        }
    }

    pub fn new_bad_ignorer(source: &str, offset: usize, len: usize) -> Self {
        let (line, col) = line_and_col(source, offset);

        Self::BadIgnorer {
            offset,
            line,
            col,
            final_offset: offset + len,
        }
    }

    pub fn new_unexpected_token(source: &str, token: Token<Label>) -> Self {
        let (line, col) = line_and_col(source, token.offset);

        Self::UnexpectedToken {
            label: token.label,
            offset: token.offset,
            line,
            col,
        }
    }
}

impl<Label: std::fmt::Debug> std::fmt::Display for SyntaxError<Label> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::UnexpectedCharacter { line, col, .. } => write!(f, "{}:{}: unexpected character", line, col),
            Self::BadLexer { label, line, col, .. } => write!(f, "{}:{}: a lexer creating {:?} tokens returned a length that would include characters past end", line, col, label),
            Self::BadIgnorer { line, col, .. } => write!(f, "{}:{}: an ignorer returned a length that would include characters past end", line, col),
            Self::UnexpectedToken {
                label,
                line,
                col,
                ..
            } => write!(f, "{}:{}: unexpected {:?}", line, col, label),
            Self::UnexpectedEOF => write!(f, "unexpected eof"),
        }
    }
}

impl<Label: std::fmt::Debug> std::error::Error for SyntaxError<Label> {}
