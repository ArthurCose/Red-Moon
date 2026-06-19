use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{ByteString, Number, StringRef, TableRef, Value, VmContext};
use crate::languages::lua::parse_number;

pub fn impl_string(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // byte
    let byte = ctx.create_function(|call_ctx, ctx| {
        let (string, start, end): (StringRef, Option<isize>, Option<isize>) =
            call_ctx.get_args(ctx)?;

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
        let (string, start, end): (ByteString, isize, Option<isize>) = call_ctx.get_args(ctx)?;
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
        string.set("len", len, ctx)?;
        string.set("lower", lower, ctx)?;
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

    impl_string_metamethods(string_metatable, ctx)?;

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

fn impl_string_metamethods(metatable: TableRef, ctx: &mut VmContext) -> Result<(), RuntimeError> {
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

fn remap_range(
    bytes: &[u8],
    start: Option<isize>,
    end: Option<isize>,
    default_end: fn(&[u8], isize) -> isize,
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
        start = (bytes.len() as isize).saturating_add(start) + 1;
    } else if start == 0 {
        start = 1;
    }

    if end < 0 {
        end = (bytes.len() as isize).saturating_add(end) + 1;
    } else if end == 0 {
        end = 1;
    }

    // lua uses inclusive bounds and starts at 1
    start -= 1;

    // keep within bounds
    let start = start.clamp(0, bytes.len() as isize) as usize;
    let end = end.clamp(0, bytes.len() as isize) as usize;

    if start < end { start..end } else { 0..0 }
}
