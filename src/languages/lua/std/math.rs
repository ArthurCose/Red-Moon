use crate::errors::RuntimeError;
use crate::interpreter::{IntoValue, MultiValue, Value, VmContext};
use crate::languages::lua::{coerce_integer, parse_number, Number};

pub fn impl_math(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    let math = ctx.create_table();

    // abs
    let abs = ctx.create_native_function(|mut args, ctx| {
        let x = coerce_number(&mut args, 1, ctx)?;

        args.push_front(match x {
            Number::Integer(i) => i.abs().into_value(ctx)?,
            Number::Float(f) => f.abs().into_value(ctx)?,
        });

        Ok(args)
    });
    math.raw_set("abs", abs, ctx)?;

    // acos
    let acos = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.acos(), ctx)
    });
    math.raw_set("acos", acos, ctx)?;

    // asin
    let asin = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.asin(), ctx)
    });
    math.raw_set("asin", asin, ctx)?;

    // atan
    let atan = ctx.create_native_function(|args, ctx| {
        let (y, x): (f64, Option<f64>) = args.unpack_args(ctx)?;

        let output = if let Some(x) = x {
            y.atan2(x)
        } else {
            y.atan()
        };

        MultiValue::pack(output, ctx)
    });
    math.raw_set("atan", atan, ctx)?;

    // ceil
    let ceil = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.ceil(), ctx)
    });
    math.raw_set("ceil", ceil, ctx)?;

    // cos
    let cos = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.cos(), ctx)
    });
    math.raw_set("cos", cos, ctx)?;

    // deg
    let deg = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.to_degrees(), ctx)
    });
    math.raw_set("deg", deg, ctx)?;

    // exp
    let exp = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.exp(), ctx)
    });
    math.raw_set("exp", exp, ctx)?;

    // floor
    let floor = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.floor(), ctx)
    });
    math.raw_set("floor", floor, ctx)?;

    // fmod
    // todo: lua preserves integers
    let fmod = ctx.create_native_function(|args, ctx| {
        let (x, y): (f64, f64) = args.unpack_args(ctx)?;

        MultiValue::pack(x % y, ctx)
    });
    math.raw_set("fmod", fmod, ctx)?;

    // huge
    let huge = ctx.create_native_function(|args, ctx| {
        ctx.store_multi(args);
        MultiValue::pack(f64::INFINITY, ctx)
    });
    math.raw_set("huge", huge, ctx)?;

    // log
    let log = ctx.create_native_function(|args, ctx| {
        let (x, base): (f64, Option<f64>) = args.unpack_args(ctx)?;
        let base = base.unwrap_or(std::f64::consts::E);

        MultiValue::pack(x.log(base), ctx)
    });
    math.raw_set("log", log, ctx)?;

    // max
    let max = ctx.create_native_function(|mut args, ctx| {
        let Some(mut max) = args.pop_front() else {
            ctx.store_multi(args);

            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::new_static_string("value expected"),
            ));
        };

        while let Some(arg) = args.pop_front() {
            if arg.is_greater_than(&max, ctx)? {
                max = arg;
            }
        }

        args.push_front(max);
        Ok(args)
    });
    math.raw_set("max", max, ctx)?;

    // maxinteger
    let maxinteger = ctx.create_native_function(|args, ctx| {
        ctx.store_multi(args);
        MultiValue::pack(i64::MAX, ctx)
    });
    math.raw_set("maxinteger", maxinteger, ctx)?;

    // min
    let min = ctx.create_native_function(|mut args, ctx| {
        let Some(mut min) = args.pop_front() else {
            ctx.store_multi(args);

            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::new_static_string("value expected"),
            ));
        };

        while let Some(arg) = args.pop_front() {
            if arg.is_less_than(&min, ctx)? {
                min = arg;
            }
        }

        args.push_front(min);
        Ok(args)
    });
    math.raw_set("min", min, ctx)?;

    // mininteger
    let mininteger = ctx.create_native_function(|args, ctx| {
        ctx.store_multi(args);
        MultiValue::pack(i64::MIN, ctx)
    });
    math.raw_set("mininteger", mininteger, ctx)?;

    // modf
    let modf = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack((x.trunc(), x.fract()), ctx)
    });
    math.raw_set("modf", modf, ctx)?;

    math.raw_set("pi", std::f64::consts::PI, ctx)?;

    // rad
    let rad = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.to_radians(), ctx)
    });
    math.raw_set("rad", rad, ctx)?;

    // sin
    let sin = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.sin(), ctx)
    });
    math.raw_set("sin", sin, ctx)?;

    // sqrt
    let sqrt = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.sqrt(), ctx)
    });
    math.raw_set("sqrt", sqrt, ctx)?;

    // tan
    let tan = ctx.create_native_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.tan(), ctx)
    });
    math.raw_set("tan", tan, ctx)?;

    // tointeger
    let tointeger = ctx.create_native_function(|mut args, ctx| {
        let x = coerce_number(&mut args, 1, ctx)?;

        args.push_front(match x {
            Number::Integer(i) => i.into_value(ctx)?,
            Number::Float(f) => coerce_integer(f).into_value(ctx)?,
        });

        Ok(args)
    });
    math.raw_set("tointeger", tointeger, ctx)?;

    // type
    let integer_string_ref = ctx.intern_string(b"integer");
    let float_string_ref = ctx.intern_string(b"float");
    let r#type = ctx.create_native_function(move |mut args, ctx| {
        let x = coerce_number(&mut args, 1, ctx)?;

        args.push_front(match x {
            Number::Integer(_) => integer_string_ref.clone().into_value(ctx)?,
            Number::Float(_) => float_string_ref.clone().into_value(ctx)?,
        });

        Ok(args)
    });
    math.raw_set("type", r#type, ctx)?;

    // ult
    let ult = ctx.create_native_function(move |args, ctx| {
        let (m, n): (i64, i64) = args.unpack_args(ctx)?;

        MultiValue::pack(m < n, ctx)
    });
    math.raw_set("ult", ult, ctx)?;

    let env = ctx.default_environment();
    env.set("math", math, ctx)?;

    // todo: random, randomseed

    Ok(())
}

fn coerce_number(
    args: &mut MultiValue,
    position: usize,
    ctx: &mut VmContext,
) -> Result<Number, RuntimeError> {
    let Some(value) = args.pop_front() else {
        return Err(RuntimeError::new_bad_argument(
            position,
            RuntimeError::new_static_string("number expected, got no value"),
        ));
    };

    match value {
        Value::Integer(i) => Ok(Number::Integer(i)),
        Value::Float(f) => Ok(Number::Float(f)),
        Value::String(s) => parse_number(&s.fetch(ctx)?.to_string_lossy()).ok_or_else(|| {
            RuntimeError::new_bad_argument(
                position,
                RuntimeError::new_static_string("number expected, got string"),
            )
        }),
        _ => Err(RuntimeError::new_bad_argument(
            position,
            RuntimeError::new_string(format!("number expected, got {}", value.type_name())),
        )),
    }
}
