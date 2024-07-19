use crate::errors::RuntimeError;
use crate::interpreter::{IntoValue, MultiValue, Value, Vm};
use crate::languages::lua::{coerce_integer, parse_number, Number};

pub fn impl_math(vm: &mut Vm) -> Result<(), RuntimeError> {
    let math = vm.create_table();

    // abs
    let abs = vm.create_native_function(|mut args, vm| {
        let x = coerce_number(&mut args, 1, vm)?;

        args.push_front(match x {
            Number::Integer(i) => i.abs().into_value(vm)?,
            Number::Float(f) => f.abs().into_value(vm)?,
        });

        Ok(args)
    });
    math.raw_set("abs", abs, vm)?;

    // acos
    let acos = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.acos(), vm)
    });
    math.raw_set("acos", acos, vm)?;

    // asin
    let asin = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.asin(), vm)
    });
    math.raw_set("asin", asin, vm)?;

    // atan
    let atan = vm.create_native_function(|args, vm| {
        let (y, x): (f64, Option<f64>) = args.unpack_args(vm)?;

        let output = if let Some(x) = x {
            y.atan2(x)
        } else {
            y.atan()
        };

        MultiValue::pack(output, vm)
    });
    math.raw_set("atan", atan, vm)?;

    // ceil
    let ceil = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.ceil(), vm)
    });
    math.raw_set("ceil", ceil, vm)?;

    // cos
    let cos = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.cos(), vm)
    });
    math.raw_set("cos", cos, vm)?;

    // deg
    let deg = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.to_degrees(), vm)
    });
    math.raw_set("deg", deg, vm)?;

    // exp
    let exp = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.exp(), vm)
    });
    math.raw_set("exp", exp, vm)?;

    // floor
    let floor = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;

        MultiValue::pack(x.floor(), vm)
    });
    math.raw_set("floor", floor, vm)?;

    // fmod
    // todo: lua preserves integers
    let fmod = vm.create_native_function(|args, vm| {
        let (x, y): (f64, f64) = args.unpack_args(vm)?;

        MultiValue::pack(x % y, vm)
    });
    math.raw_set("fmod", fmod, vm)?;

    // huge
    let huge = vm.create_native_function(|args, vm| {
        vm.store_multi(args);
        MultiValue::pack(f64::INFINITY, vm)
    });
    math.raw_set("huge", huge, vm)?;

    // log
    let log = vm.create_native_function(|args, vm| {
        let (x, base): (f64, Option<f64>) = args.unpack_args(vm)?;
        let base = base.unwrap_or(std::f64::consts::E);

        MultiValue::pack(x.log(base), vm)
    });
    math.raw_set("log", log, vm)?;

    // max
    let max = vm.create_native_function(|mut args, vm| {
        let Some(mut max) = args.pop_front() else {
            vm.store_multi(args);

            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::new_static_string("value expected"),
            ));
        };

        while let Some(arg) = args.pop_front() {
            if arg.is_greater_than(&max, vm)? {
                max = arg;
            }
        }

        args.push_front(max);
        Ok(args)
    });
    math.raw_set("max", max, vm)?;

    // maxinteger
    let maxinteger = vm.create_native_function(|args, vm| {
        vm.store_multi(args);
        MultiValue::pack(i64::MAX, vm)
    });
    math.raw_set("maxinteger", maxinteger, vm)?;

    // min
    let min = vm.create_native_function(|mut args, vm| {
        let Some(mut min) = args.pop_front() else {
            vm.store_multi(args);

            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::new_static_string("value expected"),
            ));
        };

        while let Some(arg) = args.pop_front() {
            if arg.is_less_than(&min, vm)? {
                min = arg;
            }
        }

        args.push_front(min);
        Ok(args)
    });
    math.raw_set("min", min, vm)?;

    // mininteger
    let mininteger = vm.create_native_function(|args, vm| {
        vm.store_multi(args);
        MultiValue::pack(i64::MIN, vm)
    });
    math.raw_set("mininteger", mininteger, vm)?;

    // modf
    let modf = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;
        MultiValue::pack((x.trunc(), x.fract()), vm)
    });
    math.raw_set("modf", modf, vm)?;

    math.raw_set("pi", std::f64::consts::PI, vm)?;

    // rad
    let rad = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;
        MultiValue::pack(x.to_radians(), vm)
    });
    math.raw_set("rad", rad, vm)?;

    // sin
    let sin = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;
        MultiValue::pack(x.sin(), vm)
    });
    math.raw_set("sin", sin, vm)?;

    // sqrt
    let sqrt = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;
        MultiValue::pack(x.sqrt(), vm)
    });
    math.raw_set("sqrt", sqrt, vm)?;

    // tan
    let tan = vm.create_native_function(|args, vm| {
        let x: f64 = args.unpack_args(vm)?;
        MultiValue::pack(x.tan(), vm)
    });
    math.raw_set("tan", tan, vm)?;

    // tointeger
    let tointeger = vm.create_native_function(|mut args, vm| {
        let x = coerce_number(&mut args, 1, vm)?;

        args.push_front(match x {
            Number::Integer(i) => i.into_value(vm)?,
            Number::Float(f) => coerce_integer(f).into_value(vm)?,
        });

        Ok(args)
    });
    math.raw_set("tointeger", tointeger, vm)?;

    // type
    let integer_string_ref = vm.intern_string(b"integer");
    let float_string_ref = vm.intern_string(b"float");
    let r#type = vm.create_native_function(move |mut args, vm| {
        let x = coerce_number(&mut args, 1, vm)?;

        args.push_front(match x {
            Number::Integer(_) => integer_string_ref.clone().into_value(vm)?,
            Number::Float(_) => float_string_ref.clone().into_value(vm)?,
        });

        Ok(args)
    });
    math.raw_set("type", r#type, vm)?;

    // ult
    let ult = vm.create_native_function(move |args, vm| {
        let (m, n): (i64, i64) = args.unpack_args(vm)?;

        MultiValue::pack(m < n, vm)
    });
    math.raw_set("ult", ult, vm)?;

    let env = vm.default_environment();
    env.set("math", math, vm)?;

    // todo: random, randomseed

    Ok(())
}

fn coerce_number(
    args: &mut MultiValue,
    position: usize,
    vm: &mut Vm,
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
        Value::String(s) => parse_number(&s.fetch(vm)?.to_string_lossy()).ok_or_else(|| {
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
