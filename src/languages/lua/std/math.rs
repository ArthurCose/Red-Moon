use crate::errors::RuntimeError;
use crate::interpreter::{IntoValue, NativeCallContext, Number, Value, VmContext};
use crate::languages::lua::{coerce_integer, parse_number};
use crate::tag_native_type;
use rand::RngExt;
use rand_xoshiro::Xoshiro256StarStar;
use rand_xoshiro::rand_core::SeedableRng;

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct RedMoonRng {
    rng: rand_xoshiro::Xoshiro256StarStar,
}

tag_native_type!(RedMoonRng);

pub fn load_math(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    ctx.set_singleton(RedMoonRng {
        rng: Xoshiro256StarStar::from_seed(Default::default()),
    });

    // abs
    let abs = ctx.create_function(|call_ctx, ctx| {
        let x = coerce_number(call_ctx, 1, ctx)?;

        call_ctx.return_values(
            match x {
                Number::Integer(i) => i.abs().into_value(ctx)?,
                Number::Float(f) => f.abs().into_value(ctx)?,
            },
            ctx,
        )
    });
    let rehydrating = abs.rehydrate("math.abs", ctx)?;

    // acos
    let acos = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(x.acos(), ctx)
    });
    acos.rehydrate("math.acos", ctx)?;

    // asin
    let asin = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(x.asin(), ctx)
    });
    asin.rehydrate("math.asin", ctx)?;

    // atan
    let atan = ctx.create_function(|call_ctx, ctx| {
        let (y, x): (f64, Option<f64>) = call_ctx.get_args(ctx)?;

        let output = if let Some(x) = x {
            y.atan2(x)
        } else {
            y.atan()
        };

        call_ctx.return_values(output, ctx)
    });
    atan.rehydrate("math.atan", ctx)?;

    // ceil
    let ceil = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(truncated_to_value(x.ceil()), ctx)
    });
    ceil.rehydrate("math.ceil", ctx)?;

    // cos
    let cos = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(x.cos(), ctx)
    });
    cos.rehydrate("math.cos", ctx)?;

    // deg
    let deg = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(x.to_degrees(), ctx)
    });
    deg.rehydrate("math.deg", ctx)?;

    // exp
    let exp = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(x.exp(), ctx)
    });
    exp.rehydrate("math.exp", ctx)?;

    // floor
    let floor = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;

        call_ctx.return_values(truncated_to_value(x.floor()), ctx)
    });
    floor.rehydrate("math.floor", ctx)?;

    // fmod
    let fmod = ctx.create_function(|call_ctx, ctx| {
        let (x, y): (Number, Number) = call_ctx.get_args(ctx)?;

        match (x, y) {
            (Number::Integer(x), Number::Integer(y)) => {
                if y == 0 {
                    return Err(RuntimeError::new_bad_argument(
                        2,
                        RuntimeError::new_static_string("zero"),
                    ));
                }

                call_ctx.return_values(x % y, ctx)
            }
            (Number::Integer(x), Number::Float(y)) => call_ctx.return_values(x as f64 % y, ctx),
            (Number::Float(x), Number::Integer(y)) => call_ctx.return_values(x % y as f64, ctx),
            (Number::Float(x), Number::Float(y)) => call_ctx.return_values(x % y, ctx),
        }
    });
    fmod.rehydrate("math.fmod", ctx)?;

    // log
    let log = ctx.create_function(|call_ctx, ctx| {
        let (x, base): (f64, Option<f64>) = call_ctx.get_args(ctx)?;
        let base = base.unwrap_or(std::f64::consts::E);

        call_ctx.return_values(x.log(base), ctx)
    });
    log.rehydrate("math.log", ctx)?;

    // max
    let max = ctx.create_function(|call_ctx, ctx| {
        let Some(mut max): Option<Value> = call_ctx.get_arg(0, ctx)? else {
            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::new_static_string("value expected"),
            ));
        };

        for i in 1..call_ctx.arg_count() {
            let arg: Value = call_ctx.get_arg(i, ctx)?;

            if arg.is_greater_than(&max, ctx)? {
                max = arg;
            }
        }

        call_ctx.return_values(max, ctx)
    });
    max.rehydrate("math.max", ctx)?;

    // min
    let min = ctx.create_function(|call_ctx, ctx| {
        let Some(mut min): Option<Value> = call_ctx.get_arg(0, ctx)? else {
            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::new_static_string("value expected"),
            ));
        };

        for i in 1..call_ctx.arg_count() {
            let arg: Value = call_ctx.get_arg(i, ctx)?;

            if arg.is_less_than(&min, ctx)? {
                min = arg;
            }
        }

        call_ctx.return_values(min, ctx)
    });
    min.rehydrate("math.min", ctx)?;

    // modf
    let modf = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;
        call_ctx.return_values((truncated_to_value(x.trunc()), x.fract()), ctx)
    });
    modf.rehydrate("math.modf", ctx)?;

    // rad
    let rad = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;
        call_ctx.return_values(x.to_radians(), ctx)
    });
    rad.rehydrate("math.rad", ctx)?;

    // random
    let random = ctx.create_function(|call_ctx, ctx| {
        let (n, m): (Option<i64>, Option<i64>) = call_ctx.get_args(ctx)?;

        let Some(rng) = ctx.singleton_mut::<RedMoonRng>() else {
            return Err(RuntimeError::new_static_string("missing rng struct"));
        };

        let rng = &mut rng.rng;

        let Some(n) = n else {
            return call_ctx.return_values(rng.random::<f64>(), ctx);
        };

        let Some(m) = m else {
            if n < 0 {
                return Err(RuntimeError::new_static_string(
                    "bad argument #1 to 'random' (interval is empty)",
                ));
            }

            if n == 0 {
                return call_ctx.return_values(rng.random::<u64>(), ctx);
            }

            return call_ctx.return_values(rng.random_range(1..=n), ctx);
        };

        if m < n {
            return Err(RuntimeError::new_static_string(
                "bad argument #1 to 'random' (interval is empty)",
            ));
        }

        call_ctx.return_values(rng.random_range(n..=m), ctx)
    });
    random.rehydrate("math.random", ctx)?;

    // randomseed
    let randomseed = ctx.create_function(|call_ctx, ctx| {
        let (n, m): (Option<i64>, Option<i64>) = call_ctx.get_args(ctx)?;

        let Some(rng) = ctx.singleton_mut::<RedMoonRng>() else {
            return Err(RuntimeError::new_static_string("missing rng struct"));
        };

        let (n, m) = if let Some(n) = n {
            (n, m.unwrap_or_default())
        } else {
            (0, 0)
        };

        // same seed logic as lua 5.4
        let mut seed = [0u8; 32];

        seed[..8].copy_from_slice(&n.to_le_bytes());
        seed[8..16].copy_from_slice(&(0xFFu64.to_le_bytes()));
        seed[16..24].copy_from_slice(&m.to_le_bytes());

        rng.rng = Xoshiro256StarStar::from_seed(seed);

        for _ in 0..16 {
            rng.rng.random::<i64>();
        }

        call_ctx.return_values((n, m), ctx)
    });
    randomseed.rehydrate("math.randomseed", ctx)?;

    // sin
    let sin = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;
        call_ctx.return_values(x.sin(), ctx)
    });
    sin.rehydrate("math.sin", ctx)?;

    // sqrt
    let sqrt = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;
        call_ctx.return_values(x.sqrt(), ctx)
    });
    sqrt.rehydrate("math.sqrt", ctx)?;

    // tan
    let tan = ctx.create_function(|call_ctx, ctx| {
        let x: f64 = call_ctx.get_args(ctx)?;
        call_ctx.return_values(x.tan(), ctx)
    });
    tan.rehydrate("math.tan", ctx)?;

    // tointeger
    let tointeger = ctx.create_function(|call_ctx, ctx| {
        let x = coerce_number(call_ctx, 1, ctx)?;

        call_ctx.return_values(
            match x {
                Number::Integer(i) => i.into_value(ctx)?,
                Number::Float(f) => coerce_integer(f).into_value(ctx)?,
            },
            ctx,
        )
    });
    tointeger.rehydrate("math.tointeger", ctx)?;

    // type
    let r#type = ctx.create_function(move |call_ctx, ctx| {
        let x = coerce_number(call_ctx, 1, ctx)?;

        call_ctx.return_values(
            match x {
                Number::Integer(_) => "integer",
                Number::Float(_) => "float",
            },
            ctx,
        )
    });
    r#type.rehydrate("math.type", ctx)?;

    // ult
    let ult = ctx.create_function(move |call_ctx, ctx| {
        let (m, n): (i64, i64) = call_ctx.get_args(ctx)?;

        call_ctx.return_values(m < n, ctx)
    });
    ult.rehydrate("math.ult", ctx)?;

    if !rehydrating {
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
        math.raw_set("huge", f64::INFINITY, ctx)?;
        math.raw_set("log", log, ctx)?;
        math.raw_set("max", max, ctx)?;
        math.raw_set("maxinteger", i64::MAX, ctx)?;
        math.raw_set("min", min, ctx)?;
        math.raw_set("mininteger", i64::MIN, ctx)?;
        math.raw_set("modf", modf, ctx)?;
        math.raw_set("pi", std::f64::consts::PI, ctx)?;
        math.raw_set("rad", rad, ctx)?;
        math.raw_set("random", random, ctx)?;
        math.raw_set("randomseed", randomseed, ctx)?;
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
    args: &mut NativeCallContext,
    position: usize,
    ctx: &mut VmContext,
) -> Result<Number, RuntimeError> {
    let Some(value): Option<Value> = args.get_arg(position - 1, ctx)? else {
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

fn truncated_to_value(f: f64) -> Value {
    const MAX_REPRESENTABLE: i64 = 9223372036854774784;

    if (i64::MIN as f64..=(MAX_REPRESENTABLE as f64)).contains(&f) {
        Value::Integer(f as _)
    } else {
        Value::Float(f)
    }
}
