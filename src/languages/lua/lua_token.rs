use crate::languages::Token;

pub type LuaToken<'source> = Token<'source, LuaTokenLabel>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LuaTokenLabel {
    StringLiteral,
    Numeral,
    Name,
    // reserved words
    And,
    Break,
    Do,
    Else,
    ElseIf,
    End,
    False,
    For,
    Function,
    GoTo,
    If,
    In,
    Local,
    Nil,
    Not,
    Or,
    Repeat,
    Return,
    Then,
    True,
    Until,
    While,
    // misc tokens
    Plus,                // +
    Minus,               // -
    Star,                // *
    Slash,               // /
    Percent,             // %
    Caret,               // ^
    Hash,                // #
    Ampersand,           // &
    Tilde,               // ~
    Pipe,                // |
    BitShiftLeft,        // <<
    BitShiftRight,       // >>
    DoubleSlash,         // //
    CmpEqual,            // ==
    CmpNotEqual,         // ~=
    CmpLessThanEqual,    // <=
    CmpGreaterThanEqual, // >=
    CmpLessThan,         // <
    CmpGreaterThan,      // >
    Assign,              // =
    OpenParen,           // (
    CloseParen,          // )
    OpenCurly,           // {
    CloseCurly,          // }
    OpenBracket,         // [
    CloseBracket,        // ]
    DoubleColon,         // :: used by goto labels
    SemiColon,           // ;
    Colon,               // :
    Comma,               // ,
    Dot,                 // .
    DoubleDot,           // .. concat
    TripleDot,           // ...
}
