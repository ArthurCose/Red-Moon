use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::VmContext;
use crate::languages::lua::parse_number;
use crate::languages::lua::std::{BytePattern, PatternMatcher};
use crate::tag_native_type;
use crate::values::{ByteString, FromValue, FunctionRef, Number, StringRef, TableRef, Value};
use std::ops::Range;

pub fn load_string(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // byte
    let byte = ctx.create_function(|call_ctx, ctx| {
        let (string, start, end): (StringRef, Option<i64>, Option<i64>) = call_ctx.get_args(ctx)?;

        let mut multi = ctx.create_multi();

        let byte_string = string.fetch(ctx)?;
        let bytes = byte_string.as_bytes();

        let range = remap_range(bytes, start, end, |_, start| start);

        for byte in bytes[range].iter().rev() {
            multi.push_front(Value::Integer(*byte as _));
        }

        call_ctx.return_values(multi, ctx)
    });
    let rehydrating = byte.rehydrate("str.byte", ctx)?;

    // char
    let char = ctx.create_function(|call_ctx, ctx| {
        let mut bytes = Vec::with_capacity(call_ctx.arg_count);

        for i in 0..call_ctx.arg_count() {
            let b: u8 = call_ctx.get_arg(i, ctx)?;
            bytes.push(b);
        }

        let string = ctx.intern_string(&bytes);
        call_ctx.return_values(string, ctx)
    });
    char.rehydrate("str.char", ctx)?;

    // find
    let find = ctx.create_function(|call_ctx, ctx| {
        let (string, pattern_string, init, plain): (
            ByteString,
            ByteString,
            Option<i64>,
            Option<bool>,
        ) = call_ctx.get_args(ctx)?;

        let plain = plain.unwrap_or(false);
        let bytes = string.as_bytes();
        let start = init.map(|i| remap_index(bytes, i)).unwrap_or(0);

        if start >= bytes.len() {
            // return explicit nil
            return call_ctx.return_values(Value::Nil, ctx);
        }

        if plain {
            let pattern_bytes = pattern_string.as_bytes();

            for window in bytes[start..].windows(pattern_bytes.len()) {
                if window == pattern_bytes {
                    return call_ctx.return_values((start + 1, start + pattern_bytes.len()), ctx);
                }
            }

            // return explicit nil
            return call_ctx.return_values(Value::Nil, ctx);
        }

        let pattern = BytePattern::from_byte_string(pattern_string)
            .map_err(|err| RuntimeError::new_string(err.to_string()))?;
        let mut pattern_matcher = PatternMatcher::default();

        for i in start..bytes.len() {
            let Some(len) = pattern_matcher.try_match(&pattern, bytes, i) else {
                continue;
            };

            call_ctx.return_values((i + 1, i + len), ctx)?;

            for range in pattern_matcher.captures() {
                let string_ref = ctx.intern_string(&bytes[range.clone()]);
                call_ctx.return_values(string_ref, ctx)?;
            }

            return Ok(());
        }

        // return explicit nil
        call_ctx.return_values(Value::Nil, ctx)
    });
    find.rehydrate("str.find", ctx)?;

    // gmatch
    #[derive(Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    struct RedMoonGmatch {
        matcher: PatternMatcher,
        pattern: BytePattern,
        string: ByteString,
        i: usize,
        last_read: usize,
    }

    tag_native_type!(RedMoonGmatch);

    let gmatch_iter = ctx.create_function(|call_ctx, ctx| {
        let Some(gmatch_state) = call_ctx.get_capture_mut::<RedMoonGmatch>(ctx) else {
            return Err(RuntimeError::new_static_string(
                "str.gmatch.iter capture removed?",
            ));
        };

        let RedMoonGmatch {
            matcher,
            pattern,
            string,
            i,
            last_read,
        } = gmatch_state;

        let bytes = string.as_bytes();

        while *i < string.len() {
            let Some(read) = matcher.try_match(pattern, bytes, *i) else {
                *i += 1;
                continue;
            };

            let should_return = read > 0 || *last_read == 0;
            let range_start = *i;

            *last_read = read;
            *i += read.max(1);

            if !should_return {
                continue;
            }

            let s = string.clone();
            let bytes = s.as_bytes();

            if matcher.captures().is_empty() {
                let range = range_start..range_start + read;
                let capture_ref = ctx.intern_string(&bytes[range.clone()]);
                call_ctx.return_values(capture_ref, ctx)?;
            } else {
                for capture_range in matcher.captures().to_vec() {
                    let capture_ref = ctx.intern_string(&bytes[capture_range.clone()]);
                    call_ctx.return_values(capture_ref, ctx)?;
                }
            };

            break;
        }

        Ok(())
    });
    gmatch_iter.rehydrate("str.gmatch.iter", ctx)?;

    let gmatch = ctx
        .create_function(|call_ctx, ctx| {
            let (string, pattern_string, init): (ByteString, ByteString, Option<i64>) =
                call_ctx.get_args(ctx)?;

            let Some(gmatch_iter) = call_ctx.get_capture::<FunctionRef>(ctx) else {
                return Err(RuntimeError::new_static_string(
                    "str.gmatch capture removed?",
                ));
            };

            let bytes = string.as_bytes();
            let start = init.map(|i| remap_index(bytes, i)).unwrap_or(0);
            let pattern = BytePattern::from_byte_string(pattern_string)
                .map_err(|err| RuntimeError::new_string(err.to_string()))?;

            let capture = RedMoonGmatch {
                matcher: PatternMatcher::default(),
                pattern,
                string,
                i: start,
                last_read: 0,
            };

            let closure = gmatch_iter.clone().create_closure(capture, ctx)?;
            call_ctx.return_values(closure, ctx)
        })
        .create_closure(gmatch_iter, ctx)?;
    gmatch.rehydrate("str.gmatch", ctx)?;

    // gsub
    let gsub = ctx.create_function(|call_ctx, ctx| {
        let (s, pattern, repl, n): (ByteString, ByteString, Value, Option<usize>) =
            call_ctx.get_args(ctx)?;

        if n == Some(0) {
            return call_ctx.return_values((s, 0), ctx);
        }

        let bytes = s.as_bytes();
        let pattern = BytePattern::from_byte_string(pattern)
            .map_err(|err| RuntimeError::new_string(err.to_string()))?;

        fn push_replacement(
            value: Value,
            buffer: &mut Vec<u8>,
            ctx: &mut VmContext,
        ) -> Result<(), RuntimeErrorData> {
            if !value.is_truthy() {
                return Ok(());
            }

            let type_name = value.type_name();
            let Ok(replacement_string) = ByteString::from_value(value, ctx) else {
                return Err(RuntimeErrorData::ByteString(
                    format!("invalid replacement value (a {})", type_name).into(),
                ));
            };

            buffer.extend_from_slice(replacement_string.as_bytes());

            Ok(())
        }

        fn process(
            bytes: &[u8],
            pattern: BytePattern,
            n: Option<usize>,
            ctx: &mut VmContext,
            callback: impl Fn(
                Range<usize>,
                &PatternMatcher,
                &mut Vec<u8>,
                &mut VmContext,
            ) -> Result<(), RuntimeError>,
        ) -> Result<(Vec<u8>, usize), RuntimeError> {
            let mut buffer = Vec::new();
            let mut matcher = PatternMatcher::default();
            let mut i = 0;
            let mut last_read = 0;
            let mut last_push = 0;
            let mut total_matches = 0;

            while i <= bytes.len() {
                let Some(read) = matcher.try_match(&pattern, bytes, i) else {
                    i += 1;
                    continue;
                };

                if read > 0 || last_read == 0 {
                    buffer.extend_from_slice(&bytes[last_push..i]);
                    last_push = i + read;

                    callback(i..i + read, &matcher, &mut buffer, ctx)?;

                    total_matches += 1;

                    if n == Some(total_matches) {
                        break;
                    }
                }

                last_read = read;
                i += read.max(1);
            }

            buffer.extend_from_slice(&bytes[last_push..]);

            Ok((buffer, total_matches))
        }

        let (buffer, matches) = match repl {
            Value::Table(table_ref) => {
                process(bytes, pattern, n, ctx, |range, matcher, buffer, ctx| {
                    let capture_range = matcher.captures().first().unwrap_or(&range);
                    let key = ctx.intern_string(&bytes[capture_range.clone()]);
                    let replacement_value = table_ref.get(key, ctx)?;
                    push_replacement(replacement_value, buffer, ctx)?;
                    Ok(())
                })?
            }
            Value::Function(function_ref) => {
                process(bytes, pattern, n, ctx, |range, matcher, buffer, ctx| {
                    let replacement_value = if matcher.captures().is_empty() {
                        let string_ref = ctx.intern_string(&bytes[range]);
                        function_ref.call(string_ref, ctx)?
                    } else {
                        let mut args = ctx.create_multi();

                        for capture in matcher.captures().iter().rev() {
                            let string_ref = ctx.intern_string(&bytes[capture.clone()]);
                            args.push_front(string_ref.into());
                        }

                        function_ref.call(args, ctx)?
                    };

                    push_replacement(replacement_value, buffer, ctx)?;

                    Ok(())
                })?
            }
            _ if let Ok(replacement) = ByteString::from_value(repl.clone(), ctx) => {
                let replacement_bytes = replacement.as_bytes();

                process(bytes, pattern, n, ctx, |range, matcher, buffer, _| {
                    let captures = if matcher.captures().is_empty() {
                        std::slice::from_ref(&range)
                    } else {
                        matcher.captures()
                    };

                    let mut last_push = 0;
                    let mut iter = replacement_bytes.iter().enumerate();

                    while let Some((i, &b)) = iter.next() {
                        if b != b'%' {
                            continue;
                        }

                        buffer.extend_from_slice(&replacement_bytes[last_push..i]);
                        last_push = i + 2;

                        match iter.next() {
                            Some((_, b @ b'%')) => {
                                buffer.push(*b);
                            }
                            Some((_, b'0')) => {
                                buffer.extend_from_slice(&bytes[range.clone()]);
                            }
                            Some((_, b @ b'1'..=b'9')) => {
                                let index = b - b'1';
                                let Some(capture_range) = captures.get(index as usize) else {
                                    let message = format!(
                                        "invalid capture index %{}",
                                        char::from_u32(*b as _).unwrap()
                                    );
                                    return Err(RuntimeError::new_string(message));
                                };
                                buffer.extend_from_slice(&bytes[capture_range.clone()]);
                            }
                            _ => {
                                return Err(RuntimeError::new_static_string(
                                    "invalid use of '%' in replacement string",
                                ));
                            }
                        };
                    }

                    buffer.extend_from_slice(&replacement_bytes[last_push..]);

                    Ok(())
                })?
            }
            _ => {
                let error_message =
                    format!("string/function/table expected, got {}", repl.type_name());

                return Err(RuntimeErrorData::BadArgument {
                    position: 3,
                    reason: RuntimeErrorData::ByteString(error_message.into()).into(),
                }
                .into());
            }
        };

        let string = ctx.intern_string(&buffer);
        call_ctx.return_values((string, matches), ctx)
    });
    gsub.rehydrate("str.gsub", ctx)?;

    // len
    let len = ctx.create_function(|call_ctx, ctx| {
        let string: StringRef = call_ctx.get_args(ctx)?;
        call_ctx.return_values(string.fetch(ctx)?.len(), ctx)
    });
    len.rehydrate("str.len", ctx)?;

    // lower
    let lower = ctx.create_function(|call_ctx, ctx| {
        let string: StringRef = call_ctx.get_args(ctx)?;
        let bytes = string.fetch(ctx)?.as_bytes().to_ascii_lowercase();
        let final_string = ctx.intern_string(&bytes);
        call_ctx.return_values(final_string, ctx)
    });
    lower.rehydrate("str.lower", ctx)?;

    // match
    let match_func = ctx.create_function(|call_ctx, ctx| {
        let (string, pattern_string, init): (ByteString, ByteString, Option<i64>) =
            call_ctx.get_args(ctx)?;

        let bytes = string.as_bytes();
        let start = init.map(|i| remap_index(bytes, i)).unwrap_or(0);

        let pattern = BytePattern::from_byte_string(pattern_string)
            .map_err(|err| RuntimeError::new_string(err.to_string()))?;
        let mut pattern_matcher = PatternMatcher::default();

        for i in start..bytes.len() {
            let Some(len) = pattern_matcher.try_match(&pattern, bytes, i) else {
                continue;
            };

            if pattern_matcher.captures().is_empty() {
                let string_ref = ctx.intern_string(&bytes[i..i + len]);
                call_ctx.return_values(string_ref, ctx)?;
            } else {
                for range in pattern_matcher.captures() {
                    let string_ref = ctx.intern_string(&bytes[range.clone()]);
                    call_ctx.return_values(string_ref, ctx)?;
                }
            }

            return Ok(());
        }

        call_ctx.return_values(Value::Nil, ctx)
    });
    match_func.rehydrate("str.match", ctx)?;

    // repeat / rep
    let rep = ctx.create_function(|call_ctx, ctx| {
        let (string, n, separator): (ByteString, i64, Option<ByteString>) =
            call_ctx.get_args(ctx)?;

        let n = n.max(0) as usize;

        if n == 0 {
            return call_ctx.return_values("", ctx);
        }

        let separator = separator.as_ref().map(|b| b.as_bytes()).unwrap_or(&[]);
        let mut buffer = Vec::with_capacity(string.len() * n + separator.len() * (n - 1));

        buffer.extend_from_slice(string.as_bytes());

        for _ in 0..(n - 1) {
            buffer.extend_from_slice(separator);
            buffer.extend_from_slice(string.as_bytes());
        }

        let final_string = ctx.intern_string(&buffer);
        call_ctx.return_values(final_string, ctx)
    });
    rep.rehydrate("str.rep", ctx)?;

    // reverse
    let reverse = ctx.create_function(|call_ctx, ctx| {
        let string: ByteString = call_ctx.get_args(ctx)?;

        let mut buffer = string.as_bytes().to_vec();
        buffer.reverse();

        let final_string = ctx.intern_string(&buffer);
        call_ctx.return_values(final_string, ctx)
    });
    reverse.rehydrate("str.reverse", ctx)?;

    // sub
    let sub = ctx.create_function(|call_ctx, ctx| {
        let (string, start, end): (ByteString, i64, Option<i64>) = call_ctx.get_args(ctx)?;
        let bytes = string.as_bytes();

        let range = remap_range(bytes, Some(start), end, |bytes, _| bytes.len() as _);
        let final_string = ctx.intern_string(&bytes[range]);
        call_ctx.return_values(final_string, ctx)
    });
    sub.rehydrate("str.sub", ctx)?;

    // upper
    let upper = ctx.create_function(|call_ctx, ctx| {
        let string: StringRef = call_ctx.get_args(ctx)?;
        let bytes = string.fetch(ctx)?.as_bytes().to_ascii_uppercase();
        let final_string = ctx.intern_string(&bytes);
        call_ctx.return_values(final_string, ctx)
    });
    upper.rehydrate("str.upper", ctx)?;

    let string_metatable = ctx.string_metatable();

    if !rehydrating {
        let string = ctx.create_table();
        string.set("byte", byte, ctx)?;
        string.set("char", char, ctx)?;
        string.set("find", find, ctx)?;
        string.set("gmatch", gmatch, ctx)?;
        string.set("gsub", gsub, ctx)?;
        string.set("len", len, ctx)?;
        string.set("lower", lower, ctx)?;
        string.set("match", match_func, ctx)?;
        string.set("rep", rep, ctx)?;
        string.set("reverse", reverse, ctx)?;
        string.set("sub", sub, ctx)?;
        string.set("upper", upper, ctx)?;

        // set __index
        let index_metakey = ctx.metatable_keys().index.clone();
        string_metatable.raw_set(index_metakey, string.clone(), ctx)?;

        let env = ctx.default_environment();
        env.set("string", string, ctx)?;
    }

    load_string_metamethods(string_metatable, ctx)?;

    Ok(())
}

macro_rules! impl_binary_number_op {
    ($ctx:ident, $metatable:ident, $metamethod:ident, $fn_name:ident, $op:tt) => {
        let $metamethod = $ctx.metatable_keys().$metamethod.clone();
        let $fn_name = $ctx.create_function(|call_ctx, ctx| {
            let (a, b): (Value, Value) = call_ctx.get_args(ctx)?;

            let a = coerce_number(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
            let b = coerce_number(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

            let value = match (a, b) {
                (Number::Integer(a), Number::Integer(b)) => Value::Integer(a $op b),
                (Number::Float(a), Number::Float(b)) => Value::Float(a $op b),
                (Number::Integer(a), Number::Float(b)) => Value::Float(a as f64 $op b),
                (Number::Float(a), Number::Integer(b)) => Value::Float(a $op b as f64),
            };

            call_ctx.return_values(value, ctx)
        });
    };
}

fn load_string_metamethods(metatable: TableRef, ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // basic arithmetic
    impl_binary_number_op!(ctx, metatable, add, add_fn, +);
    impl_binary_number_op!(ctx, metatable, sub, sub_fn, -);
    impl_binary_number_op!(ctx, metatable, mul, mul_fn, *);
    impl_binary_number_op!(ctx, metatable, modulus, modulus_fn, %);

    // unary minus
    let unm = ctx.metatable_keys().unm.clone();
    let unm_fn = ctx.create_function(|call_ctx, ctx| {
        let a: Value = call_ctx.get_args(ctx)?;
        let a = coerce_number(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;

        let a = match a {
            Number::Integer(i) => Value::Integer(-i),
            Number::Float(f) => Value::Float(-f),
        };

        call_ctx.return_values(a, ctx)
    });

    // division
    let div = ctx.metatable_keys().div.clone();
    let div_fn = ctx.create_function(|call_ctx, ctx| {
        let (a, b): (Value, Value) = call_ctx.get_args(ctx)?;

        let a = coerce_float(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
        let b = coerce_float(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

        call_ctx.return_values(a / b, ctx)
    });

    // integer division
    let idiv = ctx.metatable_keys().idiv.clone();
    let idiv_fn = ctx.create_function(|call_ctx, ctx| {
        let (a, b): (Value, Value) = call_ctx.get_args(ctx)?;

        let a = coerce_number(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
        let b = coerce_number(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

        let value = match (a, b) {
            (Number::Integer(a), Number::Integer(b)) => {
                if b == 0 {
                    return Err(RuntimeErrorData::DivideByZero.into());
                }

                Value::Integer(a / b)
            }
            // lua seems to preserve floats for integer division, unlike bitwise operators
            (Number::Float(a), Number::Float(b)) => Value::Float((a / b).trunc()),
            (Number::Integer(a), Number::Float(b)) => Value::Float((a as f64 / b).trunc()),
            (Number::Float(a), Number::Integer(b)) => Value::Float((a / b as f64).trunc()),
        };

        call_ctx.return_values(value, ctx)
    });

    // power
    let pow = ctx.metatable_keys().pow.clone();
    let pow_fn = ctx.create_function(|call_ctx, ctx| {
        let (a, b): (Value, Value) = call_ctx.get_args(ctx)?;

        let a = coerce_float(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
        let b = coerce_float(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

        call_ctx.return_values(a.powf(b), ctx)
    });

    let rehydrating = add_fn.rehydrate("str.__add", ctx)?;
    sub_fn.rehydrate("str.__sub", ctx)?;
    mul_fn.rehydrate("str.__mul", ctx)?;
    modulus_fn.rehydrate("str.__mod", ctx)?;
    unm_fn.rehydrate("str.__unm", ctx)?;
    div_fn.rehydrate("str.__div", ctx)?;
    idiv_fn.rehydrate("str.__idiv", ctx)?;
    pow_fn.rehydrate("str.__pow", ctx)?;

    if !rehydrating {
        metatable.raw_set(add, add_fn, ctx)?;
        metatable.raw_set(sub, sub_fn, ctx)?;
        metatable.raw_set(mul, mul_fn, ctx)?;
        metatable.raw_set(modulus, modulus_fn, ctx)?;
        metatable.raw_set(unm, unm_fn, ctx)?;
        metatable.raw_set(div, div_fn, ctx)?;
        metatable.raw_set(idiv, idiv_fn, ctx)?;
        metatable.raw_set(pow, pow_fn, ctx)?;
    }

    Ok(())
}

fn string_to_number(string_ref: &StringRef, ctx: &mut VmContext) -> Option<Number> {
    let byte_string = string_ref.fetch(ctx).ok()?;
    let s = std::str::from_utf8(byte_string.as_bytes()).ok()?;
    parse_number(s)
}

fn coerce_number(value: &Value, ctx: &mut VmContext) -> Option<Number> {
    match value {
        Value::Integer(i) => Some(Number::Integer(*i)),
        Value::Float(f) => Some(Number::Float(*f)),
        Value::String(string_ref) => string_to_number(string_ref, ctx),
        _ => None,
    }
}

fn coerce_float(value: &Value, ctx: &mut VmContext) -> Option<f64> {
    match value {
        Value::Integer(i) => Some(*i as _),
        Value::Float(f) => Some(*f),
        Value::String(string_ref) => match string_to_number(string_ref, ctx)? {
            Number::Integer(i) => Some(i as _),
            Number::Float(f) => Some(f),
        },
        _ => None,
    }
}

fn remap_index(bytes: &[u8], i: i64) -> usize {
    match i.cmp(&0) {
        std::cmp::Ordering::Less => bytes.len().saturating_sub(-i as usize),
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => (i - 1) as usize,
    }
}

fn remap_range(
    bytes: &[u8],
    start: Option<i64>,
    end: Option<i64>,
    default_end: fn(&[u8], i64) -> i64,
) -> std::ops::Range<usize> {
    let mut start = start.unwrap_or(1);

    if start == 0 {
        start = 1;
    }

    let mut end = end.unwrap_or(if start == -1 {
        bytes.len() as _
    } else {
        default_end(bytes, start)
    });

    if start < 0 {
        start = (bytes.len() as i64).saturating_add(start) + 1;
    } else if start == 0 {
        start = 1;
    }

    if end < 0 {
        end = (bytes.len() as i64).saturating_add(end) + 1;
    } else if end == 0 {
        end = 1;
    }

    // lua uses inclusive bounds and starts at 1
    start -= 1;

    // keep within bounds
    let start = start.clamp(0, bytes.len() as i64) as usize;
    let end = end.clamp(0, bytes.len() as i64) as usize;

    if start < end { start..end } else { 0..0 }
}
