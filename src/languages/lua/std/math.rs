use crate::errors::RuntimeError;
use crate::interpreter::{IntoValue, MultiValue, Number, Value, VmContext};
use crate::languages::lua::{coerce_integer, parse_number};

pub fn impl_math(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // abs
    let abs = ctx.create_function(|mut args, ctx| {
        let x = coerce_number(&mut args, 1, ctx)?;

        args.push_front(match x {
            Number::Integer(i) => i.abs().into_value(ctx)?,
            Number::Float(f) => f.abs().into_value(ctx)?,
        });

        Ok(args)
    });
    let hydrating = abs.hydrate("math.abs", ctx)?;

    // acos
    let acos = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.acos(), ctx)
    });
    acos.hydrate("math.acos", ctx)?;

    // asin
    let asin = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.asin(), ctx)
    });
    asin.hydrate("math.asin", ctx)?;

    // atan
    let atan = ctx.create_function(|args, ctx| {
        let (y, x): (f64, Option<f64>) = args.unpack_args(ctx)?;

        let output = if let Some(x) = x {
            y.atan2(x)
        } else {
            y.atan()
        };

        MultiValue::pack(output, ctx)
    });
    atan.hydrate("math.atan", ctx)?;

    // ceil
    let ceil = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.ceil(), ctx)
    });
    ceil.hydrate("math.ceil", ctx)?;

    // cos
    let cos = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.cos(), ctx)
    });
    cos.hydrate("math.cos", ctx)?;

    // deg
    let deg = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.to_degrees(), ctx)
    });
    deg.hydrate("math.deg", ctx)?;

    // exp
    let exp = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.exp(), ctx)
    });
    exp.hydrate("math.exp", ctx)?;

    // floor
    let floor = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;

        MultiValue::pack(x.floor(), ctx)
    });
    floor.hydrate("math.floor", ctx)?;

    // fmod
    // todo: lua preserves integers
    let fmod = ctx.create_function(|args, ctx| {
        let (x, y): (f64, f64) = args.unpack_args(ctx)?;

        MultiValue::pack(x % y, ctx)
    });
    fmod.hydrate("math.fmod", ctx)?;

    // huge
    let huge = ctx.create_function(|args, ctx| {
        ctx.store_multi(args);
        MultiValue::pack(f64::INFINITY, ctx)
    });
    huge.hydrate("math.huge", ctx)?;

    // log
    let log = ctx.create_function(|args, ctx| {
        let (x, base): (f64, Option<f64>) = args.unpack_args(ctx)?;
        let base = base.unwrap_or(std::f64::consts::E);

        MultiValue::pack(x.log(base), ctx)
    });
    log.hydrate("math.log", ctx)?;

    // max
    let max = ctx.create_function(|mut args, ctx| {
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
    max.hydrate("math.max", ctx)?;

    // maxinteger
    let maxinteger = ctx.create_function(|args, ctx| {
        ctx.store_multi(args);
        MultiValue::pack(i64::MAX, ctx)
    });
    maxinteger.hydrate("math.maxinteger", ctx)?;

    // min
    let min = ctx.create_function(|mut args, ctx| {
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
    min.hydrate("math.min", ctx)?;

    // mininteger
    let mininteger = ctx.create_function(|args, ctx| {
        ctx.store_multi(args);
        MultiValue::pack(i64::MIN, ctx)
    });
    mininteger.hydrate("math.mininteger", ctx)?;

    // modf
    let modf = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack((x.trunc(), x.fract()), ctx)
    });
    modf.hydrate("math.modf", ctx)?;

    // rad
    let rad = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.to_radians(), ctx)
    });
    rad.hydrate("math.rad", ctx)?;

    // sin
    let sin = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.sin(), ctx)
    });
    sin.hydrate("math.sin", ctx)?;

    // sqrt
    let sqrt = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.sqrt(), ctx)
    });
    sqrt.hydrate("math.sqrt", ctx)?;

    // tan
    let tan = ctx.create_function(|args, ctx| {
        let x: f64 = args.unpack_args(ctx)?;
        MultiValue::pack(x.tan(), ctx)
    });
    tan.hydrate("math.tan", ctx)?;

    // tointeger
    let tointeger = ctx.create_function(|mut args, ctx| {
        let x = coerce_number(&mut args, 1, ctx)?;

        args.push_front(match x {
            Number::Integer(i) => i.into_value(ctx)?,
            Number::Float(f) => coerce_integer(f).into_value(ctx)?,
        });

        Ok(args)
    });
    tointeger.hydrate("math.tointeger", ctx)?;

    // type
    let integer_string_ref = ctx.intern_string(b"integer");
    let float_string_ref = ctx.intern_string(b"float");
    let r#type = ctx.create_function(move |mut args, ctx| {
        let x = coerce_number(&mut args, 1, ctx)?;

        args.push_front(match x {
            Number::Integer(_) => integer_string_ref.clone().into_value(ctx)?,
            Number::Float(_) => float_string_ref.clone().into_value(ctx)?,
        });

        Ok(args)
    });
    r#type.hydrate("math.type", ctx)?;

    // ult
    let ult = ctx.create_function(move |args, ctx| {
        let (m, n): (i64, i64) = args.unpack_args(ctx)?;

        MultiValue::pack(m < n, ctx)
    });
    ult.hydrate("math.ult", ctx)?;

    if !hydrating {
        let math = ctx.create_table();
        math.raw_set("abs", abs, ctx)?;
        math.raw_set("acos", acos, ctx)?;
        math.raw_set("asin", asin, ctx)?;
        math.raw_set("atan", atan, ctx)?;
        math.raw_set("ceil", ceil, ctx)?;
        math.raw_set("cos", cos, ctx)?;
        math.raw_set("deg", deg, ctx)?;
        math.raw_set("exp", exp, ctx)?;
        math.raw_set("floor", floor, ctx)?;
        math.raw_set("fmod", fmod, ctx)?;
        math.raw_set("huge", huge, ctx)?;
        math.raw_set("log", log, ctx)?;
        math.raw_set("max", max, ctx)?;
        math.raw_set("maxinteger", maxinteger, ctx)?;
        math.raw_set("min", min, ctx)?;
        math.raw_set("mininteger", mininteger, ctx)?;
        math.raw_set("modf", modf, ctx)?;
        math.raw_set("pi", std::f64::consts::PI, ctx)?;
        math.raw_set("rad", rad, ctx)?;
        math.raw_set("sin", sin, ctx)?;
        math.raw_set("sqrt", sqrt, ctx)?;
        math.raw_set("tan", tan, ctx)?;
        math.raw_set("tointeger", tointeger, ctx)?;
        math.raw_set("type", r#type, ctx)?;
        math.raw_set("ult", ult, ctx)?;

        let env = ctx.default_environment();
        env.set("math", math, ctx)?;
    }

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
