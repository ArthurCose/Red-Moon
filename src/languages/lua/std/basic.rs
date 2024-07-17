use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{
    ByteString, FromValue, LazyArg, MultiValue, Primitive, TableRef, Value, Vm,
};
use crate::languages::lua::parse_number;

pub fn impl_basic(vm: &mut Vm) -> Result<(), RuntimeError> {
    let env = vm.default_environment();

    env.set("_G", env.clone(), vm)?;
    env.set("_VERSION", "Lua 5.3", vm)?;

    // assert
    let assert = vm.create_native_function(|args, vm| {
        let (passed, message): (bool, LazyArg<Option<ByteString>>) = args.unpack_args(vm)?;

        if !passed {
            if let Some(s) = message.into_arg(vm)? {
                return Err(RuntimeError::new_byte_string(s));
            } else {
                return Err(RuntimeError::new_static_string("assertion failed!"));
            }
        }

        MultiValue::pack((), vm)
    });
    env.set("assert", assert, vm)?;

    // collectgarbage
    let assert = vm.create_native_function(|args, vm| {
        let opt: Option<ByteString> = args.unpack_args(vm)?;

        if let Some(opt) = opt {
            match opt.as_bytes() {
                b"collect" => {
                    vm.gc_collect();
                }
                b"count" => {
                    let kibi = vm.gc_used_memory() as f32 / 1024.0;
                    return MultiValue::pack(kibi, vm);
                }
                // todo: stop, restart, step, isrunning, incremental
                _ => {
                    let message = format!("invalid option '{opt}'");
                    let inner_error = RuntimeError::new_string(message);
                    return Err(RuntimeError::new_bad_argument(1, inner_error));
                }
            }
        } else {
            vm.gc_collect();
        }

        MultiValue::pack((), vm)
    });
    env.set("collectgarbage", assert, vm)?;

    // error
    let error = vm.create_native_function(|args, vm| {
        // todo: level
        let message: Value = args.unpack_args(vm)?;

        let err = match message {
            Value::Primitive(Primitive::Integer(i)) => RuntimeError::new_string(i.to_string()),
            Value::Primitive(Primitive::Float(f)) => RuntimeError::new_string(f.to_string()),
            Value::String(s) => RuntimeError::new_byte_string(s.fetch(vm)?.clone()),
            _ => RuntimeError::new_string(format!("(error is a {} value)", message.type_name())),
        };

        Err(err)
    });
    env.set("error", error, vm)?;

    // print
    let print = vm.create_native_function(|mut args, vm| {
        while let Some(arg) = args.pop_front() {
            print!("{}", to_string(arg, vm)?);

            if !args.is_empty() {
                print!("\t");
            }
        }

        println!();

        Ok(args)
    });
    env.set("print", print, vm)?;

    // tostring
    let tostring = vm.create_native_function(|args, vm| {
        let value: Value = args.unpack_args(vm)?;
        let string = to_string(value, vm)?;

        MultiValue::pack(string, vm)
    });
    env.set("tostring", tostring, vm)?;

    // type
    let type_name = vm.create_native_function(|args, vm| {
        let value: Value = args.unpack_args(vm)?;
        let type_name = value.type_name();

        MultiValue::pack(type_name, vm)
    });
    env.set("type", type_name, vm)?;

    // getmetatable
    let getmetatable = vm.create_native_function(|args, vm| {
        let table: TableRef = args.unpack_args(vm)?;
        let metatable = table.metatable(vm)?;

        if let Some(metatable) = table.metatable(vm)? {
            let metatable_key = vm.metatable_keys().metatable.clone();
            let metatable_value = metatable.raw_get::<_, Option<Value>>(metatable_key, vm)?;

            if let Some(metatable_value) = metatable_value {
                return MultiValue::pack(metatable_value, vm);
            }
        }

        MultiValue::pack(metatable, vm)
    });
    env.set("getmetatable", getmetatable, vm)?;

    // setmetatable
    let setmetatable = vm.create_native_function(|args, vm| {
        let (table, metatable): (TableRef, Option<TableRef>) = args.unpack_args(vm)?;

        if let Some(metatable) = table.metatable(vm)? {
            let metatable_key = vm.metatable_keys().metatable.clone();
            let is_protected = metatable
                .raw_get::<_, Option<Value>>(metatable_key, vm)?
                .is_some();

            if is_protected {
                return Err(RuntimeError::new_static_string(
                    "cannot change a protected metatable",
                ));
            }
        }
        table.set_metatable(metatable.as_ref(), vm)?;

        MultiValue::pack(table, vm)
    });
    env.set("setmetatable", setmetatable, vm)?;

    // rawequal
    let rawequal = vm.create_native_function(|args, vm| {
        let (a, b): (Value, Value) = args.unpack_args(vm)?;

        MultiValue::pack(a == b, vm)
    });
    env.set("rawequal", rawequal, vm)?;

    // rawget
    let rawget = vm.create_native_function(|args, vm| {
        let (table, key): (TableRef, Value) = args.unpack_args(vm)?;
        let value: Value = table.raw_get(key, vm)?;

        MultiValue::pack(value, vm)
    });
    env.set("rawget", rawget, vm)?;

    // rawset
    let rawset = vm.create_native_function(|args, vm| {
        let (table, key, value): (TableRef, Value, Value) = args.unpack_args(vm)?;
        table.raw_set(key, value, vm)?;

        MultiValue::pack((), vm)
    });
    env.set("rawset", rawset, vm)?;

    // next
    let next = vm.create_native_function(|args, vm| {
        let (table, key): (TableRef, Value) = args.unpack_args(vm)?;
        let Some((next_key, value)): Option<(Value, Value)> = table.next(key, vm)? else {
            return MultiValue::pack((), vm);
        };

        MultiValue::pack((next_key, value), vm)
    });
    env.set("next", next, vm)?;

    // ipairs
    let ipairs_iterator = vm.create_native_function(|args, vm| {
        let (table, mut index): (TableRef, i64) = args.unpack_args(vm)?;
        index += 1;

        let value: Value = table.raw_get(index, vm)?;

        if value.is_nil() {
            // lua returns a single nil, not zero values
            MultiValue::pack(value, vm)
        } else {
            MultiValue::pack((index, value), vm)
        }
    });

    let ipairs = vm.create_native_function(move |args, vm| {
        let table: TableRef = args.unpack_args(vm)?;

        let iterator = if let Some(metatable) = table.metatable(vm)? {
            // try metatable
            metatable
                .raw_get(vm.metatable_keys().ipairs.clone(), vm)
                .unwrap_or_else(|_| ipairs_iterator.clone())
        } else {
            ipairs_iterator.clone()
        };

        MultiValue::pack((iterator, table, 0), vm)
    });
    env.set("ipairs", ipairs, vm)?;

    // pairs
    let pairs_iterator = vm.create_native_function(|args, vm| {
        let (table, prev_key): (TableRef, Value) = args.unpack_args(vm)?;

        let Some((key, value)): Option<(Value, Value)> = table.next(prev_key, vm)? else {
            // lua returns a single nil, not zero values
            return MultiValue::pack(Value::default(), vm);
        };

        MultiValue::pack((key, value), vm)
    });

    let pairs = vm.create_native_function(move |args, vm| {
        let table: TableRef = args.unpack_args(vm)?;

        let iterator = if let Some(metatable) = table.metatable(vm)? {
            // try metatable
            metatable
                .raw_get(vm.metatable_keys().pairs.clone(), vm)
                .unwrap_or_else(|_| pairs_iterator.clone())
        } else {
            pairs_iterator.clone()
        };

        MultiValue::pack((iterator, table, Value::default()), vm)
    });
    env.set("pairs", pairs, vm)?;

    // select
    let select = vm.create_native_function(|args, vm| {
        let (arg, mut args): (Value, MultiValue) = args.unpack_args(vm)?;

        if let Ok(s) = ByteString::from_value(arg.clone(), vm) {
            let len = args.len();
            vm.store_multi(args);

            if s.as_bytes() != b"#" {
                return Err(RuntimeError::new_bad_argument(
                    1,
                    RuntimeError::new_static_string("number expected, got string"),
                ));
            }

            return MultiValue::pack(len, vm);
        }

        let mut index = match i64::from_value(arg, vm) {
            Ok(index) => index,
            Err(err) => {
                vm.store_multi(args);

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
    env.set("select", select, vm)?;

    // tonumber
    let tonumber = vm.create_native_function(|args, vm| {
        let (string, base): (Option<ByteString>, Option<i64>) = args.unpack_args(vm)?;

        let Some(base) = base else {
            let Some(string) = string else {
                // lua allows nil only if no base is supplied
                return MultiValue::pack(Value::default(), vm);
            };

            // normal parsing
            return MultiValue::pack(parse_number(&string.to_string_lossy()), vm);
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

                    return MultiValue::pack(Value::default(), vm);
                }
            };

            if digit >= base {
                // invalid digit
                return MultiValue::pack(Value::default(), vm);
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
                return MultiValue::pack(Value::default(), vm);
            }
        }

        MultiValue::pack(n, vm)
    });
    env.set("tonumber", tonumber, vm)?;

    // todo: pcall
    // todo: xpcall
    // todo: warn

    Ok(())
}

fn to_string(value: Value, vm: &mut Vm) -> Result<String, RuntimeError> {
    match value {
        Value::Primitive(p) => match p {
            Primitive::Nil => Ok("nil".to_string()),
            Primitive::Bool(b) => Ok(b.to_string()),
            Primitive::Integer(i) => Ok(i.to_string()),
            Primitive::Float(f) => Ok(f.to_string()),
        },
        Value::String(s) => Ok(s.fetch(vm)?.to_string_lossy().to_string()),
        Value::Table(table) => {
            if let Ok(Some(metatable)) = table.metatable(vm) {
                let tostring_key = vm.metatable_keys().tostring.clone();

                if let Ok(Some(function_value)) =
                    metatable.raw_get::<_, Option<Value>>(tostring_key, vm)
                {
                    return function_value.call(table.clone(), vm);
                }

                let name_key = vm.metatable_keys().name.clone();

                if let Ok(name) = metatable.raw_get::<_, ByteString>(name_key, vm) {
                    return Ok(format!("{name}: 0x{:x}", table.id()));
                }
            }

            Ok(format!("table: 0x{:x}", table.id()))
        }
        Value::Function(f) => Ok(format!("function: 0x{:x}", f.id())),
    }
}
