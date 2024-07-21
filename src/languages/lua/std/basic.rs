use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{ByteString, FromValue, LazyArg, MultiValue, TableRef, Value, VmContext};
use crate::languages::lua::parse_number;

pub fn impl_basic(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    let env = ctx.default_environment();

    env.set("_G", env.clone(), ctx)?;
    env.set("_VERSION", "Lua 5.3", ctx)?;

    // assert
    let assert = ctx.create_native_function(|args, ctx| {
        let (passed, message): (bool, LazyArg<Option<ByteString>>) = args.unpack_args(ctx)?;

        if !passed {
            if let Some(s) = message.into_arg(ctx)? {
                return Err(RuntimeError::new_byte_string(s));
            } else {
                return Err(RuntimeError::new_static_string("assertion failed!"));
            }
        }

        MultiValue::pack((), ctx)
    });
    env.set("assert", assert, ctx)?;

    // collectgarbage
    let assert = ctx.create_native_function(|args, ctx| {
        let (opt, mut rest): (Option<ByteString>, MultiValue) = args.unpack_args(ctx)?;

        let result = if let Some(opt) = opt {
            match opt.as_bytes() {
                b"count" => {
                    let kibi = ctx.gc_used_memory() as f64 / 1024.0;
                    MultiValue::pack(kibi, ctx)
                }
                b"isrunning" => {
                    let running = ctx.gc_is_running();
                    MultiValue::pack(running, ctx)
                }
                b"stop" => {
                    ctx.gc_stop();
                    MultiValue::pack((), ctx)
                }
                b"restart" => {
                    ctx.gc_restart();
                    MultiValue::pack((), ctx)
                }
                b"step" => {
                    let (kibi, new_rest): (Option<f64>, MultiValue) =
                        rest.unpack_modified_args(ctx, 2)?;
                    rest = new_rest;

                    ctx.gc_step(kibi.map(|kibi| (kibi * 1024.0) as _).unwrap_or_default());
                    MultiValue::pack((), ctx)
                }
                b"collect" => {
                    ctx.gc_collect();
                    MultiValue::pack((), ctx)
                }
                b"incremental" => {
                    let (pause, step_mul, step_size, new_rest): (
                        Option<usize>,
                        Option<usize>,
                        Option<u32>,
                        MultiValue,
                    ) = rest.unpack_modified_args(ctx, 2)?;

                    rest = new_rest;

                    let config = ctx.gc_config_mut();

                    if let Some(pause) = pause {
                        if pause > 0 {
                            config.pause = pause;
                        }
                    }

                    if let Some(step_mul) = step_mul {
                        if step_mul > 0 {
                            config.step_multiplier = step_mul;
                        }
                    }

                    if let Some(step_size) = step_size {
                        if step_size > 0 {
                            config.step_size = 2usize.pow(step_size);
                        }
                    }

                    MultiValue::pack((), ctx)
                }
                _ => {
                    let message = format!("invalid option '{opt}'");
                    let inner_error = RuntimeError::new_string(message);
                    Err(RuntimeError::new_bad_argument(1, inner_error))
                }
            }
        } else {
            ctx.gc_collect();
            MultiValue::pack((), ctx)
        };

        ctx.store_multi(rest);
        result
    });
    env.set("collectgarbage", assert, ctx)?;

    // error
    let error = ctx.create_native_function(|args, ctx| {
        // todo: level
        let message: Value = args.unpack_args(ctx)?;

        let err = match message {
            Value::Integer(i) => RuntimeError::new_string(i.to_string()),
            Value::Float(f) => RuntimeError::new_string(f.to_string()),
            Value::String(s) => RuntimeError::new_byte_string(s.fetch(ctx)?.clone()),
            _ => RuntimeError::new_string(format!("(error is a {} value)", message.type_name())),
        };

        Err(err)
    });
    env.set("error", error, ctx)?;

    // print
    let print = ctx.create_native_function(|mut args, ctx| {
        while let Some(arg) = args.pop_front() {
            print!("{}", to_string(arg, ctx)?);

            if !args.is_empty() {
                print!("\t");
            }
        }

        println!();

        Ok(args)
    });
    env.set("print", print, ctx)?;

    // tostring
    let tostring = ctx.create_native_function(|args, ctx| {
        let value: Value = args.unpack_args(ctx)?;
        let string = to_string(value, ctx)?;

        MultiValue::pack(string, ctx)
    });
    env.set("tostring", tostring, ctx)?;

    // type
    let type_name = ctx.create_native_function(|args, ctx| {
        let value: Value = args.unpack_args(ctx)?;
        let type_name = value.type_name();

        MultiValue::pack(type_name, ctx)
    });
    env.set("type", type_name, ctx)?;

    // getmetatable
    let getmetatable = ctx.create_native_function(|args, ctx| {
        let table: TableRef = args.unpack_args(ctx)?;
        let metatable = table.metatable(ctx)?;

        if let Some(metatable) = table.metatable(ctx)? {
            let metatable_key = ctx.metatable_keys().metatable.clone();
            let metatable_value = metatable.raw_get::<_, Option<Value>>(metatable_key, ctx)?;

            if let Some(metatable_value) = metatable_value {
                return MultiValue::pack(metatable_value, ctx);
            }
        }

        MultiValue::pack(metatable, ctx)
    });
    env.set("getmetatable", getmetatable, ctx)?;

    // setmetatable
    let setmetatable = ctx.create_native_function(|args, ctx| {
        let (table, metatable): (TableRef, Option<TableRef>) = args.unpack_args(ctx)?;

        if let Some(metatable) = table.metatable(ctx)? {
            let metatable_key = ctx.metatable_keys().metatable.clone();
            let is_protected = metatable
                .raw_get::<_, Option<Value>>(metatable_key, ctx)?
                .is_some();

            if is_protected {
                return Err(RuntimeError::new_static_string(
                    "cannot change a protected metatable",
                ));
            }
        }
        table.set_metatable(metatable.as_ref(), ctx)?;

        MultiValue::pack(table, ctx)
    });
    env.set("setmetatable", setmetatable, ctx)?;

    // rawequal
    let rawequal = ctx.create_native_function(|args, ctx| {
        let (a, b): (Value, Value) = args.unpack_args(ctx)?;

        MultiValue::pack(a == b, ctx)
    });
    env.set("rawequal", rawequal, ctx)?;

    // rawget
    let rawget = ctx.create_native_function(|args, ctx| {
        let (table, key): (TableRef, Value) = args.unpack_args(ctx)?;
        let value: Value = table.raw_get(key, ctx)?;

        MultiValue::pack(value, ctx)
    });
    env.set("rawget", rawget, ctx)?;

    // rawset
    let rawset = ctx.create_native_function(|args, ctx| {
        let (table, key, value): (TableRef, Value, Value) = args.unpack_args(ctx)?;
        table.raw_set(key, value, ctx)?;

        MultiValue::pack((), ctx)
    });
    env.set("rawset", rawset, ctx)?;

    // next
    let next = ctx.create_native_function(|args, ctx| {
        let (table, key): (TableRef, Value) = args.unpack_args(ctx)?;
        let Some((next_key, value)): Option<(Value, Value)> = table.next(key, ctx)? else {
            return MultiValue::pack((), ctx);
        };

        MultiValue::pack((next_key, value), ctx)
    });
    env.set("next", next, ctx)?;

    // ipairs
    let ipairs_iterator = ctx.create_native_function(|args, ctx| {
        let (table, mut index): (TableRef, i64) = args.unpack_args(ctx)?;
        index += 1;

        let value: Value = table.raw_get(index, ctx)?;

        if value.is_nil() {
            // lua returns a single nil, not zero values
            MultiValue::pack(value, ctx)
        } else {
            MultiValue::pack((index, value), ctx)
        }
    });

    let ipairs = ctx.create_native_function(move |args, ctx| {
        let table: TableRef = args.unpack_args(ctx)?;

        let iterator = if let Some(metatable) = table.metatable(ctx)? {
            // try metatable
            metatable
                .raw_get(ctx.metatable_keys().ipairs.clone(), ctx)
                .unwrap_or_else(|_| ipairs_iterator.clone())
        } else {
            ipairs_iterator.clone()
        };

        MultiValue::pack((iterator, table, 0), ctx)
    });
    env.set("ipairs", ipairs, ctx)?;

    // pairs
    let pairs_iterator = ctx.create_native_function(|args, ctx| {
        let (table, prev_key): (TableRef, Value) = args.unpack_args(ctx)?;

        let Some((key, value)): Option<(Value, Value)> = table.next(prev_key, ctx)? else {
            // lua returns a single nil, not zero values
            return MultiValue::pack(Value::default(), ctx);
        };

        MultiValue::pack((key, value), ctx)
    });

    let pairs = ctx.create_native_function(move |args, ctx| {
        let table: TableRef = args.unpack_args(ctx)?;

        let iterator = if let Some(metatable) = table.metatable(ctx)? {
            // try metatable
            metatable
                .raw_get(ctx.metatable_keys().pairs.clone(), ctx)
                .unwrap_or_else(|_| pairs_iterator.clone())
        } else {
            pairs_iterator.clone()
        };

        MultiValue::pack((iterator, table, Value::default()), ctx)
    });
    env.set("pairs", pairs, ctx)?;

    // select
    let select = ctx.create_native_function(|args, ctx| {
        let (arg, mut args): (Value, MultiValue) = args.unpack_args(ctx)?;

        if let Ok(s) = ByteString::from_value(arg.clone(), ctx) {
            let len = args.len();
            ctx.store_multi(args);

            if s.as_bytes() != b"#" {
                return Err(RuntimeError::new_bad_argument(
                    1,
                    RuntimeError::new_static_string("number expected, got string"),
                ));
            }

            return MultiValue::pack(len, ctx);
        }

        let mut index = match i64::from_value(arg, ctx) {
            Ok(index) => index,
            Err(err) => {
                ctx.store_multi(args);

                return Err(RuntimeError::new_bad_argument(1, err));
            }
        };

        if index < 0 {
            index += args.len() as i64 + 1;
        }

        if index <= 0 {
            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::from(RuntimeErrorData::OutOfBounds),
            ));
        }

        let index = index as usize - 1;

        if let Some(value) = args.get(index) {
            let value = value.clone();
            args.clear();
            args.push_front(value);
        } else {
            args.clear();
        }

        Ok(args)
    });
    env.set("select", select, ctx)?;

    // tonumber
    let tonumber = ctx.create_native_function(|args, ctx| {
        let (string, base): (Option<ByteString>, Option<i64>) = args.unpack_args(ctx)?;

        let Some(base) = base else {
            let Some(string) = string else {
                // lua allows nil only if no base is supplied
                return MultiValue::pack(Value::default(), ctx);
            };

            // normal parsing
            return MultiValue::pack(parse_number(&string.to_string_lossy()), ctx);
        };

        let Some(string) = string else {
            // lua does not allow nil if no base is supplied
            return Err(RuntimeError::new_bad_argument(
                2,
                RuntimeError::new_static_string("string expected, got nil"),
            ));
        };

        // check base range
        if !(2..=36).contains(&base) {
            return Err(RuntimeError::new_bad_argument(
                2,
                RuntimeError::new_static_string("base out of range"),
            ));
        }

        let mut bytes = string.as_bytes();
        let start = bytes.iter().take_while(|b| b.is_ascii_whitespace()).count();
        bytes = &bytes[start..];

        // check sign
        let mut negative = false;

        if bytes.starts_with(&[b'+']) {
            bytes = &bytes[1..];
        } else if bytes.starts_with(&[b'-']) {
            bytes = &bytes[1..];
            negative = true;
        }

        // resolve whole number
        let mut n = 0;
        let mut total_digits = 0;

        for b in bytes {
            total_digits += 1;

            let digit = match b {
                b'0'..=b'9' => (b - b'0') as i64,
                b'a'..=b'z' => (b - b'a' + 10) as i64,
                b'A'..=b'Z' => (b - b'A' + 10) as i64,

                _ => {
                    if b.is_ascii_whitespace() {
                        // stop if we've encountered whitespace
                        break;
                    }

                    return MultiValue::pack(Value::default(), ctx);
                }
            };

            if digit >= base {
                // invalid digit
                return MultiValue::pack(Value::default(), ctx);
            }

            n *= base;
            n += digit;
        }

        // apply sign
        if negative {
            n *= -1;
        }

        // fail if there's anything in the whitespace
        for b in &bytes[total_digits..] {
            if !b.is_ascii_whitespace() {
                return MultiValue::pack(Value::default(), ctx);
            }
        }

        MultiValue::pack(n, ctx)
    });
    env.set("tonumber", tonumber, ctx)?;

    // todo: pcall
    // todo: xpcall
    // todo: warn

    Ok(())
}

fn to_string(value: Value, ctx: &mut VmContext) -> Result<String, RuntimeError> {
    match value {
        Value::Nil => Ok("nil".to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        Value::String(s) => Ok(s.fetch(ctx)?.to_string_lossy().to_string()),
        Value::Table(table) => {
            if let Ok(Some(metatable)) = table.metatable(ctx) {
                let tostring_key = ctx.metatable_keys().tostring.clone();

                if let Ok(Some(function_value)) =
                    metatable.raw_get::<_, Option<Value>>(tostring_key, ctx)
                {
                    return function_value.call(table.clone(), ctx);
                }

                let name_key = ctx.metatable_keys().name.clone();

                if let Ok(name) = metatable.raw_get::<_, ByteString>(name_key, ctx) {
                    return Ok(format!("{name}: 0x{:x}", table.id()));
                }
            }

            Ok(format!("table: 0x{:x}", table.id()))
        }
        Value::Function(f) => Ok(format!("function: 0x{:x}", f.id())),
    }
}
