use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{MultiValue, Number, StringRef, TableRef, Value, VmContext};
use crate::languages::lua::parse_number;

pub fn impl_string(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    let len = ctx.create_function(|args, ctx| {
        let string: StringRef = args.unpack(ctx)?;
        MultiValue::pack(string.fetch(ctx)?.len(), ctx)
    });
    let rehydrating = len.rehydrate("str.len", ctx)?;

    let string_metatable = ctx.string_metatable();

    if !rehydrating {
        let string = ctx.create_table();
        string.set("len", len, ctx)?;

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
        let $fn_name = $ctx.create_function(|args, ctx| {
            let (a, b): (Value, Value) = args.unpack(ctx)?;

            let a = coerce_number(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
            let b = coerce_number(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

            let value = match (a, b) {
                (Number::Integer(a), Number::Integer(b)) => Value::Integer(a $op b),
                (Number::Float(a), Number::Float(b)) => Value::Float(a $op b),
                (Number::Integer(a), Number::Float(b)) => Value::Float(a as f64 $op b),
                (Number::Float(a), Number::Integer(b)) => Value::Float(a $op b as f64),
            };

            MultiValue::pack(value, ctx)
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
    let unm_fn = ctx.create_function(|args, ctx| {
        let a: Value = args.unpack(ctx)?;
        let a = coerce_number(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;

        let a = match a {
            Number::Integer(i) => Value::Integer(-i),
            Number::Float(f) => Value::Float(-f),
        };

        MultiValue::pack(a, ctx)
    });

    // division
    let div = ctx.metatable_keys().div.clone();
    let div_fn = ctx.create_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack(ctx)?;

        let a = coerce_float(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
        let b = coerce_float(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

        MultiValue::pack(a / b, ctx)
    });

    // integer division
    let idiv = ctx.metatable_keys().idiv.clone();
    let idiv_fn = ctx.create_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack(ctx)?;

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

        MultiValue::pack(value, ctx)
    });

    // power
    let pow = ctx.metatable_keys().pow.clone();
    let pow_fn = ctx.create_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack(ctx)?;

        let a = coerce_float(&a, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(a.type_name()))?;
        let b = coerce_float(&b, ctx).ok_or(RuntimeErrorData::InvalidArithmetic(b.type_name()))?;

        MultiValue::pack(a.powf(b), ctx)
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
