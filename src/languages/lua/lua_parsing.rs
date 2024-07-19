use super::{LuaToken, LuaTokenLabel};
use crate::errors::{RuntimeError, RuntimeErrorData, SyntaxError};
use crate::interpreter::{FromValue, IntoValue, Value, Vm};
use std::borrow::Cow;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Number {
    Integer(i64),
    Float(f64),
}

impl From<Number> for Value {
    fn from(value: Number) -> Self {
        match value {
            Number::Integer(i) => Value::Integer(i),
            Number::Float(f) => Value::Float(f),
        }
    }
}

impl IntoValue for Number {
    fn into_value(self, _: &mut Vm) -> Result<Value, RuntimeError> {
        match self {
            Number::Integer(i) => Ok(Value::Integer(i)),
            Number::Float(f) => Ok(Value::Float(f)),
        }
    }
}

impl FromValue for Number {
    fn from_value(value: Value, _: &mut Vm) -> Result<Number, RuntimeError> {
        match value {
            Value::Integer(i) => Ok(Number::Integer(i)),
            Value::Float(f) => Ok(Number::Float(f)),
            _ => Err(RuntimeErrorData::FromLuaConversionError {
                from: value.type_name(),
                to: "Number",
                message: None,
            }
            .into()),
        }
    }
}

// private as we have assumptions that can cause panics
// to make this public it would be preferable to return a syntax error instead
pub(crate) fn parse_string<'source>(
    source: &'source str,
    token: LuaToken<'source>,
) -> Result<Cow<'source, [u8]>, SyntaxError<LuaTokenLabel>> {
    let mut bytes_slice = token.content.as_bytes();

    match bytes_slice[0] {
        b'"' | b'\'' => {
            bytes_slice = &bytes_slice[1..bytes_slice.len() - 1];
        }
        b'[' => {
            let len = bytes_slice[1..].iter().position(|b| *b == b'[').unwrap() + 2;
            bytes_slice = &bytes_slice[len..bytes_slice.len() - len];

            // skip the first new line
            if bytes_slice.starts_with(&[b'\n']) {
                bytes_slice = &bytes_slice[1..];
            } else if bytes_slice.starts_with(&[b'\r', b'\n']) {
                bytes_slice = &bytes_slice[2..];
            }

            // only quoted strings handle escape characters
            return Ok(Cow::Borrowed(bytes_slice));
        }
        _ => {
            // only quoted strings handle escape characters
            return Ok(Cow::Borrowed(bytes_slice));
        }
    }

    let mut bytes_vec = Vec::new();
    let mut last_index = 0;

    loop {
        let Some(mut i) = bytes_slice
            .iter()
            .skip(last_index)
            .position(|b| *b == b'\\')
        else {
            break;
        };

        if last_index == 0 {
            bytes_vec.reserve(bytes_slice.len() - 1);
        }

        // copy up to the slash
        i += last_index;
        bytes_vec.extend(bytes_slice[last_index..i].iter().cloned());

        // skip past the slash
        i += 1;

        if let Some(b) = bytes_slice.get(i) {
            match b {
                // bell
                b'a' => {
                    bytes_vec.push(7);
                    i += 1;
                }
                // backspace
                b'b' => {
                    bytes_vec.push(8);
                    i += 1;
                }
                // form feed
                b'f' => {
                    bytes_vec.push(12);
                    i += 1;
                }
                // newline
                b'n' => {
                    bytes_vec.push(b'\n');
                    i += 1;
                }
                // carriage return
                b'r' => {
                    bytes_vec.push(b'\r');
                    i += 1;
                }
                // horizontal tab
                b't' => {
                    bytes_vec.push(b'\t');
                    i += 1;
                }
                // vertical tab
                b'v' => {
                    bytes_vec.push(11);
                    i += 1;
                }
                b'\\' | b'\"' | b'\'' => {
                    bytes_vec.push(*b);
                    i += 1;
                }
                // skip any following whitespace characters
                b'z' => {
                    i += 1;
                    i += bytes_slice
                        .iter()
                        .skip(i)
                        .position(|b| !b.is_ascii_whitespace())
                        .unwrap_or(bytes_slice.len() - i);
                }
                _ => {
                    return Err(SyntaxError::new_unexpected_character(
                        source,
                        token.offset + i,
                    ));
                }
            }
        }

        last_index = i;
    }

    Ok(if bytes_vec.is_empty() {
        Cow::Borrowed(bytes_slice)
    } else {
        bytes_vec.extend(bytes_slice[last_index..].iter().cloned());
        Cow::Owned(bytes_vec)
    })
}

pub(crate) fn parse_unsigned_number(s: &str) -> Option<Number> {
    if !s.starts_with("0x") && !s.starts_with("0X") {
        return if s.contains(['.', 'e', 'E']) {
            if let Ok(i) = s.parse() {
                Some(Number::Float(i))
            } else {
                None
            }
        } else if let Ok(i) = s.parse() {
            Some(Number::Integer(i))
        } else {
            None
        };
    }

    let mut a: i64 = 0;
    let mut b = 0;
    let mut decimal_offset = None;
    let mut p = None;
    let mut p_offset = None;

    for (i, byte) in s[2..].bytes().enumerate() {
        match byte {
            b'0'..=b'9' => {
                a *= 16;
                a += (byte - b'0') as i64;
            }
            b'a'..=b'f' => {
                a *= 16;
                a += (byte - b'a' + 10) as i64;
            }
            b'A'..=b'F' => {
                a *= 16;
                a += (byte - b'A' + 10) as i64;
            }
            b'.' => {
                if decimal_offset.is_some() {
                    return None;
                }

                b = a;
                a = 0;
                decimal_offset = Some(i + 1);
            }
            b'p' | b'P' => {
                // try to parse the remaining characters as an i32 for p
                let Ok(int) = s[2 + i + 1..].parse::<i32>() else {
                    return None;
                };

                p = Some(int);
                p_offset = Some(i);
                break;
            }
            _ => return None,
        }
    }

    if p.is_none() && decimal_offset.is_none() {
        return Some(Number::Integer(a));
    }

    let mut float = a as f64;

    if let Some(decimal_offset) = decimal_offset {
        let decimal_len = if let Some(p_offset) = p_offset {
            p_offset - decimal_offset
        } else {
            s.len() - 2 - decimal_offset
        };

        float /= 16.0f64.powf(decimal_len as _);
        float += b as f64;
    }

    if let Some(p) = p {
        float *= 2f64.powi(p);
    }

    Some(Number::Float(float))
}

/// Trims input
pub fn parse_number(mut s: &str) -> Option<Number> {
    s = s.trim();

    if s.starts_with('-') {
        s = &s[1..];

        match parse_unsigned_number(s) {
            Some(Number::Float(f)) => Some(Number::Float(-f)),
            Some(Number::Integer(i)) => Some(Number::Integer(-i)),
            p => p,
        }
    } else {
        if s.starts_with('+') {
            s = &s[1..];
        }

        parse_unsigned_number(s)
    }
}

pub fn coerce_integer(float: f64) -> Option<i64> {
    if float.fract() != 0.0 {
        return None;
    }

    let int = float as i64;

    if float.trunc() != int as f64 {
        return None;
    }

    Some(int)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::languages::lua::lua_lexer::LuaLexer;

    #[test]
    fn string_parsing() {
        let source = r#""\a \b \f \n \r \t \v \\ \" \' \z    b""#;
        let expected = b"\x07 \x08 \x0C \x0A \x0D \x09 \x0B \\ \" ' b";

        let lexer = LuaLexer::default();
        let mut token_iter = lexer.lex(source);
        let string = parse_string(source, token_iter.next().unwrap().unwrap()).unwrap();

        assert_eq!(&*string, expected);
    }

    #[test]
    fn number_parsing() {
        // integer
        assert_eq!(parse_unsigned_number("3"), Some(Number::Integer(3)));
        assert_eq!(parse_unsigned_number("345"), Some(Number::Integer(345)));
        assert_eq!(parse_unsigned_number("0xff"), Some(Number::Integer(0xff)));
        assert_eq!(
            parse_unsigned_number("0xBEBADA"),
            Some(Number::Integer(0xBEBADA))
        );

        // floats
        assert_eq!(parse_unsigned_number("3."), Some(Number::Float(3.0)));
        assert_eq!(parse_unsigned_number("3.0"), Some(Number::Float(3.0)));
        assert_eq!(
            parse_unsigned_number("3.1416"),
            Some(Number::Float(
                #[allow(clippy::approx_constant)]
                3.1416
            ))
        );
        assert_eq!(
            parse_unsigned_number("314.16e-2"),
            Some(Number::Float(314.16e-2))
        );
        assert_eq!(
            parse_unsigned_number("0.31416E1"),
            Some(Number::Float(0.31416E1))
        );
        assert_eq!(parse_unsigned_number("34e1"), Some(Number::Float(34e1)));
        assert_eq!(
            parse_unsigned_number("0x0.1E"),
            Some(Number::Float(0.1171875))
        );
        assert_eq!(
            parse_unsigned_number("0xA23p-4"),
            Some(Number::Float(162.1875))
        );

        // todo: we differ from lua on this currently
        // lua: 3.1415926535898
        assert_eq!(
            parse_unsigned_number("0X1.921FB54442D18P+1"),
            Some(Number::Float(
                #[allow(clippy::approx_constant)]
                3.141592653589793
            ))
        );

        assert_eq!(parse_number(" -1  "), Some(Number::Integer(-1)));
        assert_eq!(parse_number(" -1.0 "), Some(Number::Float(-1.0)));
        assert_eq!(parse_number(" 1  "), Some(Number::Integer(1)));
        assert_eq!(parse_number(" 1.0 "), Some(Number::Float(1.0)));
        assert_eq!(parse_number("  +1  "), Some(Number::Integer(1)));
        assert_eq!(parse_number(" +1.0 "), Some(Number::Float(1.0)));
    }
}
