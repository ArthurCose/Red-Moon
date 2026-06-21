use super::token::Token;
use crate::FastHashMap;
use crate::errors::{SourcePosition, SyntaxError, SyntaxErrorData};
use std::borrow::Cow;

trait SubLexerFn<Label>: Fn(&Lexer<Label>, &str, usize) -> Option<(Label, usize)> + Sync {}
impl<T, Label> SubLexerFn<Label> for T where
    T: Fn(&Lexer<Label>, &str, usize) -> Option<(Label, usize)> + Sync
{
}

trait IgnorerFn: Fn(&str, usize) -> usize + Sync {}
impl<T> IgnorerFn for T where T: Fn(&str, usize) -> usize + Sync {}

type SubLexer<Label> = Box<dyn SubLexerFn<Label>>;
type Ignorer = Box<dyn IgnorerFn>;

pub struct Lexer<Label> {
    lexers: Vec<SubLexer<Label>>,
    ignorers: Vec<Ignorer>,
    token_lexer: SubLexer<Label>,
    tokens: FastHashMap<Cow<'static, str>, Label>,
    longest_token: usize,
}

impl<Label: Copy> Default for Lexer<Label> {
    fn default() -> Self {
        Self {
            lexers: Vec::new(),
            ignorers: Vec::new(),
            token_lexer: Box::new(|lexer, source, start| Self::lex_token(lexer, source, start)),
            tokens: Default::default(),
            longest_token: 0,
        }
    }
}

impl<Label: Copy> Lexer<Label> {
    /// ignorer takes source str, and start index, returns the length to skip
    pub fn add_ignorer<F>(&mut self, lexer: F)
    where
        F: 'static + Fn(&str, usize) -> usize + Sync,
    {
        self.ignorers.push(Box::new(lexer))
    }

    /// takes source str, and start index, returns the length of the token
    pub fn add_lexer<F>(&mut self, lexer: F)
    where
        F: 'static + Fn(&Lexer<Label>, &str, usize) -> Option<(Label, usize)> + Sync,
    {
        self.lexers.push(Box::new(lexer));
    }

    #[allow(unused)]
    pub fn add_char_lexer<F>(&mut self, lexer: F)
    where
        F: 'static + Fn(char) -> (Label, bool) + Sync,
    {
        self.add_lexer(move |_, source, start| {
            let char = source[start..].chars().next().unwrap();
            let (label, pass) = lexer(char);

            if pass { Some((label, 1)) } else { None }
        });
    }

    /// Lowest priority
    pub fn add_token(&mut self, label: Label, value: Cow<'static, str>) {
        self.longest_token = self.longest_token.max(value.len());
        self.tokens.insert(value, label);
    }

    fn lex_token(&self, source: &str, start: usize) -> Option<(Label, usize)> {
        let max_test_len = self.longest_token.min(source.len() - start);
        for len in (1..=max_test_len).rev() {
            if let Some(label) = self.tokens.get(&source[start..start + len]) {
                return Some((*label, len));
            }
        }

        None
    }

    pub fn lex<'lexer: 'iter, 'source: 'iter, 'iter>(
        &'lexer self,
        source: &'source str,
    ) -> impl Iterator<Item = Result<Token<'source, Label>, SyntaxError<Label>>> + 'iter {
        let mut skip = 0;

        std::iter::from_fn(move || {
            loop {
                if skip >= source.len() {
                    return None;
                }

                let length = self
                    .ignorers
                    .iter()
                    .map(|ignorer| ignorer(source, skip))
                    .find(|length| *length > 0);

                if let Some(length) = length {
                    if length + skip > source.len() {
                        return Some(Err(SyntaxError {
                            source_position: SourcePosition::new(source, skip),
                            data: SyntaxErrorData::BadIgnorer { len: length },
                        }));
                    }

                    skip += length;
                    continue;
                }

                let lexer_result = self
                    .lexers
                    .iter()
                    .chain(std::iter::once(&self.token_lexer))
                    .flat_map(|sub_lexer| sub_lexer(self, source, skip))
                    .next();

                if let Some((label, length)) = lexer_result {
                    if length + skip > source.len() {
                        return Some(Err(SyntaxError {
                            source_position: SourcePosition::new(source, skip),
                            data: SyntaxErrorData::BadLexer { label, len: length },
                        }));
                    }

                    let offset = skip;
                    skip += length;

                    return Some(Ok(Token {
                        label,
                        content: &source[offset..offset + length],
                        offset,
                    }));
                }

                return Some(Err(SyntaxError {
                    source_position: SourcePosition::new(source, skip),
                    data: SyntaxErrorData::UnexpectedCharacter,
                }));
            }
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn lexer() {
        let mut lexer = Lexer::default();

        let lexemes = ["<", "<=", ">", ">=", "=="];

        for lexeme in lexemes {
            lexer.add_token(lexeme, lexeme.into());
        }

        // numbers
        lexer.add_lexer(|_, source, start| {
            let len = source
                .chars()
                .skip(start)
                .take_while(|c| c.is_numeric())
                .count();

            if len == 0 {
                return None;
            }

            Some(("number", len))
        });

        // whitespace
        lexer.add_ignorer(|source, start| {
            source
                .chars()
                .skip(start)
                .take_while(|c| c.is_whitespace())
                .count()
        });

        assert_eq!(
            &lexer
                .lex("12 >= 3")
                .collect::<Result<Vec<_>, SyntaxError<_>>>()
                .unwrap(),
            &[
                Token {
                    label: "number",
                    content: "12",
                    offset: 0
                },
                Token {
                    label: ">=",
                    content: ">=",
                    offset: 3
                },
                Token {
                    label: "number",
                    content: "3",
                    offset: 6
                }
            ]
        );
    }

    #[test]
    fn bad_lexer() {
        let mut lexer = Lexer::default();

        lexer.add_lexer(|_, _source, _start| Some(("faulty", 1000)));

        assert_eq!(
            lexer
                .lex("12 >= 3")
                .collect::<Result<Vec<_>, SyntaxError<_>>>()
                .unwrap_err(),
            SyntaxError {
                source_position: SourcePosition {
                    offset: 0,
                    line: 1,
                    col: 1,
                },
                data: SyntaxErrorData::BadLexer {
                    label: "faulty",
                    len: 1000
                },
            }
        );
    }
}
