use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{MultiValue, StringRef, TableRef, Value, VmContext};
use crate::languages::lua::{parse_number, Number};

pub fn impl_string(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    let string = ctx.create_table();

    let string_metatable = ctx.string_metatable();
    string_metatable.raw_set(ctx.metatable_keys().index.clone(), string.clone(), ctx)?;

    let env = ctx.default_environment();
    env.set("string", string, ctx)?;

    impl_string_metamethods(string_metatable, ctx)?;

    Ok(())
}

macro_rules! impl_binary_number_op {
    ($ctx:ident, $metatable:ident, $metamethod:ident, $op:tt) => {{
        let key = $ctx.metatable_keys().$metamethod.clone();
        let func = $ctx.create_native_function(|args, ctx| {
            let (a, b): (Value, Value) = args.unpack(ctx)?;

            let a = coerce_number(a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;
            let b = coerce_number(b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;

            let value = match (a, b) {
                (Number::Integer(a), Number::Integer(b)) => Value::Integer(a $op b),
                (Number::Float(a), Number::Float(b)) => Value::Float(a $op b),
                (Number::Integer(a), Number::Float(b)) => Value::Float(a as f64 $op b),
                (Number::Float(a), Number::Integer(b)) => Value::Float(a $op b as f64),
            };

            MultiValue::pack(value, ctx)
        });

        $metatable.raw_set(key, func, $ctx)?;
    }};
}

fn impl_string_metamethods(metatable: TableRef, ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // index
    let key = ctx.metatable_keys().index.clone();
    metatable.raw_set(key, metatable.clone(), ctx)?;

    // basic arithmetic
    impl_binary_number_op!(ctx, metatable, add, +);
    impl_binary_number_op!(ctx, metatable, sub, -);
    impl_binary_number_op!(ctx, metatable, mul, *);
    impl_binary_number_op!(ctx, metatable, modulus, %);

    // unary minus
    let key = ctx.metatable_keys().unm.clone();
    let func = ctx.create_native_function(|args, ctx| {
        let a: Value = args.unpack(ctx)?;
        let a = coerce_number(a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;

        let a = match a {
            Number::Integer(i) => Value::Integer(-i),
            Number::Float(f) => Value::Float(-f),
        };

        MultiValue::pack(a, ctx)
    });

    metatable.raw_set(key, func, ctx)?;

    // division
    let key = ctx.metatable_keys().div.clone();
    let func = ctx.create_native_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack(ctx)?;

        let a = coerce_float(a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;
        let b = coerce_float(b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;

        MultiValue::pack(a / b, ctx)
    });

    metatable.raw_set(key, func, ctx)?;

    // integer division
    let key = ctx.metatable_keys().idiv.clone();
    let func = ctx.create_native_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack(ctx)?;

        let a = coerce_number(a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;
        let b = coerce_number(b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;

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

        MultiValue::pack(value, ctx)
    });

    metatable.raw_set(key, func, ctx)?;

    // power
    let key = ctx.metatable_keys().pow.clone();
    let func = ctx.create_native_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack(ctx)?;

        let a = coerce_float(a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;
        let b = coerce_float(b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic)?;

        MultiValue::pack(a.powf(b), ctx)
    });

    metatable.raw_set(key, func, ctx)?;

    Ok(())
}

fn string_to_number(string_ref: StringRef, ctx: &mut VmContext) -> Option<Number> {
    let byte_string = string_ref.fetch(ctx).ok()?;
    let s = std::str::from_utf8(byte_string.as_bytes()).ok()?;
    parse_number(s)
}

fn coerce_number(value: Value, ctx: &mut VmContext) -> Option<Number> {
    match value {
        Value::Integer(i) => Some(Number::Integer(i)),
        Value::Float(f) => Some(Number::Float(f)),
        Value::String(string_ref) => string_to_number(string_ref, ctx),
        _ => None,
    }
}

fn coerce_float(value: Value, ctx: &mut VmContext) -> Option<f64> {
    match value {
        Value::Integer(i) => Some(i as _),
        Value::Float(f) => Some(f),
        Value::String(string_ref) => match string_to_number(string_ref, ctx)? {
            Number::Integer(i) => Some(i as _),
            Number::Float(f) => Some(f),
        },
        _ => None,
    }
}
