use super::{LuaToken, LuaTokenLabel};
use crate::errors::SyntaxError;
use crate::languages::lexer::Lexer;
use std::collections::HashMap;

pub struct LuaLexer {
    lexer: Lexer<LuaTokenLabel>,
}

impl Default for LuaLexer {
    fn default() -> Self {
        let reserved_words = HashMap::from([
            ("and", LuaTokenLabel::And),
            ("break", LuaTokenLabel::Break),
            ("do", LuaTokenLabel::Do),
            ("else", LuaTokenLabel::Else),
            ("elseif", LuaTokenLabel::ElseIf),
            ("end", LuaTokenLabel::End),
            ("false", LuaTokenLabel::False),
            ("for", LuaTokenLabel::For),
            ("function", LuaTokenLabel::Function),
            ("goto", LuaTokenLabel::GoTo),
            ("if", LuaTokenLabel::If),
            ("in", LuaTokenLabel::In),
            ("local", LuaTokenLabel::Local),
            ("nil", LuaTokenLabel::Nil),
            ("not", LuaTokenLabel::Not),
            ("or", LuaTokenLabel::Or),
            ("repeat", LuaTokenLabel::Repeat),
            ("return", LuaTokenLabel::Return),
            ("then", LuaTokenLabel::Then),
            ("true", LuaTokenLabel::True),
            ("until", LuaTokenLabel::Until),
            ("while", LuaTokenLabel::While),
        ]);

        let other_tokens = [
            ("+", LuaTokenLabel::Plus),
            ("-", LuaTokenLabel::Minus),
            ("*", LuaTokenLabel::Star),
            ("/", LuaTokenLabel::Slash),
            ("%", LuaTokenLabel::Percent),
            ("^", LuaTokenLabel::Caret),
            ("#", LuaTokenLabel::Hash),
            ("&", LuaTokenLabel::Ampersand),
            ("~", LuaTokenLabel::Tilde),
            ("|", LuaTokenLabel::Pipe),
            ("<<", LuaTokenLabel::BitShiftLeft),
            (">>", LuaTokenLabel::BitShiftRight),
            ("//", LuaTokenLabel::DoubleSlash),
            ("==", LuaTokenLabel::CmpEqual),
            ("~=", LuaTokenLabel::CmpNotEqual),
            ("<=", LuaTokenLabel::CmpLessThanEqual),
            (">=", LuaTokenLabel::CmpGreaterThanEqual),
            ("<", LuaTokenLabel::CmpLessThan),
            (">", LuaTokenLabel::CmpGreaterThan),
            ("=", LuaTokenLabel::Assign),
            ("(", LuaTokenLabel::OpenParen),
            (")", LuaTokenLabel::CloseParen),
            ("{", LuaTokenLabel::OpenCurly),
            ("}", LuaTokenLabel::CloseCurly),
            ("[", LuaTokenLabel::OpenBracket),
            ("]", LuaTokenLabel::CloseBracket),
            ("::", LuaTokenLabel::DoubleColon),
            (";", LuaTokenLabel::SemiColon),
            (":", LuaTokenLabel::Colon),
            (",", LuaTokenLabel::Comma),
            (".", LuaTokenLabel::Dot),
            ("..", LuaTokenLabel::DoubleDot),
            ("...", LuaTokenLabel::TripleDot),
        ];

        let mut lexer = Lexer::default();

        for (token, label) in other_tokens {
            lexer.add_token(label, token.into());
        }

        lexer.add_lexer(move |_, source, start| {
            let source_substr = &source[start..];

            let valid_start = source_substr.starts_with(|c: char| c.is_ascii_digit())
                || (source_substr.starts_with('.')
                    && source_substr
                        .as_bytes()
                        .get(1)
                        .is_some_and(|c| c.is_ascii_digit()));

            if !valid_start {
                return None;
            }

            let mut last_char = b' ';

            let len = source_substr
                .bytes()
                .take_while(|&b| {
                    let result = match b {
                        b'.' => true,
                        b'-' | b'+' => matches!(last_char, b'e' | b'E' | b'p' | b'P'),
                        _ => b.is_ascii_alphanumeric(),
                    };
                    last_char = b;
                    result
                })
                .count();

            Some((LuaTokenLabel::Numeral, len))
        });

        // reserved words and names
        lexer.add_lexer(move |_, source, start| {
            let first_char = source[start..].chars().next().unwrap();

            if !first_char.is_alphabetic() && first_char != '_' {
                return None;
            }

            let word_len = source[start + 1..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .count()
                + 1;

            let word = &source[start..start + word_len];

            if let Some(label) = reserved_words.get(word) {
                // use the reserved word as the name of the token
                Some((*label, word_len))
            } else {
                Some((LuaTokenLabel::Name, word_len))
            }
        });

        // strings
        lexer.add_lexer(|_, source, start| {
            let source_substr = &source[start..];

            let first_char = source_substr.chars().next().unwrap();

            if !(first_char == '"' || first_char == '\'') {
                return None;
            }

            let mut previous_char = '"';

            let string_length = &source_substr[1..]
                .chars()
                .take_while(|c| {
                    let c = *c;
                    let is_end = (c == '\n' || c == first_char) && previous_char != '\\';

                    previous_char = c;

                    !is_end
                })
                .count();

            let last_char = source_substr[string_length + 1..].chars().next();

            if let Some(last_char) = last_char {
                if last_char != first_char {
                    return None;
                }
            } else {
                return None;
            }

            Some((LuaTokenLabel::StringLiteral, string_length + 2))
        });

        // multiline string
        lexer.add_lexer(move |_, source, start| {
            let source_substr = &source[start..];

            if let Some(substr) = source_substr.strip_prefix("[[") {
                return substr
                    .find("]]")
                    .map(|index| (LuaTokenLabel::StringLiteral, index + 4));
            }

            // see if this uses `[=` format
            if !source_substr.starts_with("[=") {
                return None;
            }

            // count `=`
            let mut search_substr = &source_substr[1..];
            let eq_count = search_substr.bytes().take_while(|&b| b == b'=').count();
            search_substr = &search_substr[eq_count..];

            if !search_substr.starts_with('[') {
                return None;
            }

            // search for the end
            search_substr = &search_substr[1..];
            let mut len = eq_count + 2;

            loop {
                let first_byte = search_substr.bytes().next()?;

                // increment len and search position
                len += 1;
                search_substr = &search_substr[1..];

                if first_byte != b']' {
                    continue;
                }

                let end_eq_count = search_substr.bytes().take_while(|&b| b == b'=').count();
                len += end_eq_count;
                search_substr = &search_substr[eq_count..];

                if search_substr.starts_with(']') && eq_count == end_eq_count {
                    len += 1;
                    break;
                }
            }

            Some((LuaTokenLabel::StringLiteral, len))
        });

        // whitespace
        lexer.add_ignorer(|source, start| {
            source[start..]
                .chars()
                .take_while(|c| c.is_whitespace())
                .count()
        });

        // comments
        lexer.add_ignorer(|source, start| {
            let source_substr = &source[start..];

            if source_substr.starts_with("--[[") {
                source_substr
                    .find("]]")
                    .map(|index| index + 2)
                    .unwrap_or(source.len() - start)
            } else if source_substr.starts_with("--") {
                source_substr
                    .find(['\r', '\n'])
                    .unwrap_or(source.len() - start)
            } else {
                0
            }
        });

        Self { lexer }
    }
}

impl LuaLexer {
    pub fn lex<'lexer: 'iter, 'source: 'iter, 'iter>(
        &'lexer self,
        source: &'source str,
    ) -> impl Iterator<Item = Result<LuaToken<'source>, SyntaxError<LuaTokenLabel>>> + 'iter {
        self.lexer.lex(source)
    }
}

#[cfg(test)]
mod test {
    use super::LuaLexer;
    use crate::languages::lua::LuaTokenLabel;

    #[test]
    fn numbers() {
        let numbers = [
            "3",
            "345",
            "0xff",
            "0xBEBADA",
            "3.",
            "3.0",
            "3.1416",
            "314.16e-2",
            "0.31416E1",
            "34e1",
            "0x0.1E",
            "0xA23p-4",
            "0X1.921FB54442D18P+1",
        ];

        let lexer = LuaLexer::default();

        for s in numbers {
            let token = lexer.lex(s).next().unwrap().unwrap();
            assert_eq!((token.label, token.content), (LuaTokenLabel::Numeral, s));
        }
    }

    #[test]
    fn false_numbers() {
        let tests = [(".", LuaTokenLabel::Dot)];

        let lexer = LuaLexer::default();

        for (s, label) in tests {
            let token = lexer.lex(s).next().unwrap().unwrap();
            assert_eq!((token.label, token.content), (label, s));
        }
    }

    #[test]
    fn strings() {
        let strings = [
            // escaped new line
            "\"\\\n\"",
            // multiline strings
            "[[abc\n]]",
            "[==[\nde\nf]==]",
            // single quote mixed
            "'[==[d\"ef]==]'",
            // used by another test
            r#""\a \b \f \n \r \t \v \\ \" \' \z    b""#,
        ];

        let lexer = LuaLexer::default();

        for s in strings {
            let token = lexer.lex(s).next().unwrap().unwrap();
            assert_eq!(
                (token.label, token.content),
                (LuaTokenLabel::StringLiteral, s)
            );
        }
    }
}
