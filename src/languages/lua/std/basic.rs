use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::VmContext;
use crate::languages::lua::{compile, parse_number};
use crate::values::{ByteString, FromValue, FunctionRef, MultiValue, StringRef, TableRef, Value};

pub fn load_basic(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // assert
    let assert = ctx.create_function(|call_ctx, vm_ctx| {
        let passed: bool = call_ctx.get_arg(0, vm_ctx)?;

        if passed {
            return Ok(());
        }

        let message: Option<ByteString> = call_ctx.get_arg(1, vm_ctx)?;

        if let Some(s) = message {
            Err(RuntimeError::new_byte_string(s))
        } else {
            Err(RuntimeError::new_static_string("assertion failed!"))
        }
    });
    let rehydrating = assert.rehydrate("lua.assert", ctx)?;

    // collectgarbage
    let collectgarbage = ctx.create_function(|call_ctx, ctx| {
        let opt: Option<ByteString> = call_ctx.get_arg(0, ctx)?;

        if let Some(opt) = opt {
            match opt.as_bytes() {
                b"count" => {
                    let kibi = ctx.gc_used_memory() as f64 / 1024.0;
                    call_ctx.return_values(kibi, ctx)?;
                }
                b"isrunning" => {
                    let running = ctx.gc_is_running();
                    call_ctx.return_values(running, ctx)?;
                }
                b"stop" => {
                    ctx.gc_stop();
                }
                b"restart" => {
                    ctx.gc_restart();
                }
                b"step" => {
                    let kibi: Option<f64> = call_ctx.get_arg(1, ctx)?;

                    ctx.gc_step(kibi.map(|kibi| (kibi * 1024.0) as _).unwrap_or_default());
                }
                b"collect" => {
                    ctx.gc_collect();
                }
                b"incremental" => {
                    let (pause, step_mul, step_size): (Option<usize>, Option<usize>, Option<u32>) =
                        call_ctx.get_args_at(1, ctx)?;

                    let config = ctx.gc_config_mut();

                    if let Some(pause) = pause
                        && pause > 0
                    {
                        config.pause = pause;
                    }

                    if let Some(step_mul) = step_mul
                        && step_mul > 0
                    {
                        config.step_multiplier = step_mul;
                    }

                    if let Some(step_size) = step_size
                        && step_size > 0
                    {
                        config.step_size = 2usize.pow(step_size);
                    }
                }
                _ => {
                    let message = format!("invalid option '{opt}'");
                    let inner_error = RuntimeError::new_string(message);

                    return Err(RuntimeError::new_bad_argument(1, inner_error));
                }
            }
        } else {
            ctx.gc_collect();
        };

        Ok(())
    });
    collectgarbage.rehydrate("lua.collectgarbage", ctx)?;

    // error
    let error = ctx.create_function(|call_ctx, ctx| {
        // todo: level
        let message: Value = call_ctx.get_args(ctx)?;

        let err = match message {
            Value::Integer(i) => RuntimeError::new_string(i.to_string()),
            Value::Float(f) => RuntimeError::new_string(f.to_string()),
            Value::String(s) => RuntimeError::new_byte_string(s.fetch(ctx)?.clone()),
            _ => RuntimeError::new_string(format!("(error is a {} value)", message.type_name())),
        };

        Err(err)
    });
    error.rehydrate("lua.error", ctx)?;

    // print
    let print = ctx.create_function(|call_ctx, ctx| {
        let arg_count = call_ctx.arg_count();

        for i in 0..arg_count {
            let arg: Value = call_ctx.get_arg(i, ctx)?;
            print!("{}", to_string(arg, ctx)?);

            if i < arg_count - 1 {
                print!("\t");
            }
        }

        println!();

        Ok(())
    });
    print.rehydrate("lua.print", ctx)?;

    // tostring
    let tostring = ctx.create_function(|call_ctx, ctx| {
        let value: Value = call_ctx.get_args(ctx)?;
        let string = to_string(value, ctx)?;

        call_ctx.return_values(string, ctx)
    });
    tostring.rehydrate("lua.tostring", ctx)?;

    // type
    let type_name = ctx.create_function(|call_ctx, ctx| {
        let value: Value = call_ctx.get_args(ctx)?;
        let type_name = value.type_name();

        call_ctx.return_values(type_name.as_str(), ctx)
    });
    type_name.rehydrate("lua.type", ctx)?;

    // getmetatable
    let getmetatable = ctx.create_function(|call_ctx, ctx| {
        let value: Value = call_ctx.get_args(ctx)?;

        let metatable = match value {
            Value::String(_) => Some(ctx.string_metatable()),
            Value::Table(table) => table.metatable(ctx)?,
            _ => return Ok(()),
        };

        if let Some(metatable) = &metatable {
            let metatable_key = ctx.metatable_keys().metatable.clone();
            let metatable_value = metatable.raw_get::<_, Option<Value>>(metatable_key, ctx)?;

            if let Some(metatable_value) = metatable_value {
                return call_ctx.return_values(metatable_value, ctx);
            }
        }

        call_ctx.return_values(metatable, ctx)
    });
    getmetatable.rehydrate("lua.getmetatable", ctx)?;

    // setmetatable
    let setmetatable = ctx.create_function(|call_ctx, ctx| {
        let (table, metatable): (TableRef, Option<TableRef>) = call_ctx.get_args(ctx)?;

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

        call_ctx.return_values(table, ctx)
    });
    setmetatable.rehydrate("lua.setmetatable", ctx)?;

    // rawequal
    let rawequal = ctx.create_function(|call_ctx, ctx| {
        let (a, b): (Value, Value) = call_ctx.get_args(ctx)?;

        call_ctx.return_values(a == b, ctx)
    });
    rawequal.rehydrate("lua.rawequal", ctx)?;

    // rawget
    let rawget = ctx.create_function(|call_ctx, ctx| {
        let (table, key): (TableRef, Value) = call_ctx.get_args(ctx)?;
        let value: Value = table.raw_get(key, ctx)?;

        call_ctx.return_values(value, ctx)
    });
    rawget.rehydrate("lua.rawget", ctx)?;

    // rawlen
    let rawlen = ctx.create_function(|call_ctx, ctx| {
        let table: TableRef = call_ctx.get_args(ctx)?;
        let len = table.raw_len(ctx)?;

        call_ctx.return_values(len, ctx)
    });
    rawlen.rehydrate("lua.rawlen", ctx)?;

    // rawset
    let rawset = ctx.create_function(|call_ctx, ctx| {
        let (table, key, value): (TableRef, Value, Value) = call_ctx.get_args(ctx)?;
        table.raw_set(key, value, ctx)?;

        Ok(())
    });
    rawset.rehydrate("lua.rawset", ctx)?;

    // next
    let next = ctx.create_function(|call_ctx, ctx| {
        let (table, key): (TableRef, Value) = call_ctx.get_args(ctx)?;
        let Some((next_key, value)): Option<(Value, Value)> = table.next(key, ctx)? else {
            return Ok(());
        };

        call_ctx.return_values((next_key, value), ctx)
    });
    next.rehydrate("lua.next", ctx)?;

    // ipairs
    let ipairs_iterator = ctx.create_function(|call_ctx, ctx| {
        let (table, mut index): (TableRef, i64) = call_ctx.get_args(ctx)?;
        index += 1;

        let value: Value = table.raw_get(index, ctx)?;

        if value.is_nil() {
            // lua returns a single nil, not zero values
            call_ctx.return_values(value, ctx)
        } else {
            call_ctx.return_values((index, value), ctx)
        }
    });
    ipairs_iterator.rehydrate("lua.ipairs.iterator", ctx)?;

    let ipairs = ctx.create_function(|call_ctx, ctx| {
        let table: TableRef = call_ctx.get_args(ctx)?;

        let Some(ipairs_iterator) = call_ctx.read_capture::<FunctionRef>(ctx) else {
            return Ok(());
        };

        let ipairs_iterator = ipairs_iterator.clone();

        let iterator = if let Some(metatable) = table.metatable(ctx)? {
            // try metatable
            metatable
                .raw_get(ctx.metatable_keys().ipairs.clone(), ctx)
                .unwrap_or(ipairs_iterator)
        } else {
            ipairs_iterator
        };

        call_ctx.return_values((iterator, table, 0), ctx)
    });
    let ipairs = ipairs.create_closure(ipairs_iterator, ctx)?;
    ipairs.rehydrate("lua.ipairs", ctx)?;

    // pairs
    let pairs_iterator = ctx.create_function(|call_ctx, ctx| {
        let (table, prev_key): (TableRef, Value) = call_ctx.get_args(ctx)?;

        let Some((key, value)): Option<(Value, Value)> = table.next(prev_key, ctx)? else {
            // lua returns a single nil, not zero values
            return call_ctx.return_values(Value::default(), ctx);
        };

        call_ctx.return_values((key, value), ctx)
    });
    pairs_iterator.rehydrate("lua.pairs.iterator", ctx)?;

    let pairs = ctx.create_function(|call_ctx, ctx| {
        let table: TableRef = call_ctx.get_args(ctx)?;

        let Some(pairs_iterator) = call_ctx.read_capture::<FunctionRef>(ctx) else {
            return Ok(());
        };

        let pairs_iterator = pairs_iterator.clone();

        let iterator = if let Some(metatable) = table.metatable(ctx)? {
            // try metatable
            metatable
                .raw_get(ctx.metatable_keys().pairs.clone(), ctx)
                .unwrap_or(pairs_iterator)
        } else {
            pairs_iterator
        };

        call_ctx.return_values((iterator, table, Value::default()), ctx)
    });
    let pairs = pairs.create_closure(pairs_iterator, ctx)?;
    pairs.rehydrate("lua.pairs", ctx)?;

    // select
    let select = ctx.create_function(|call_ctx, ctx| {
        let arg: Value = call_ctx.get_arg(0, ctx)?;

        if let Ok(s) = ByteString::from_value(arg.clone(), ctx) {
            let len = call_ctx.arg_count() - 1;

            if s.as_bytes() != b"#" {
                return Err(RuntimeError::new_bad_argument(
                    1,
                    RuntimeError::new_static_string("number expected, got string"),
                ));
            }

            return call_ctx.return_values(len, ctx);
        }

        let mut index = match i64::from_value(arg, ctx) {
            Ok(index) => index,
            Err(err) => {
                return Err(RuntimeError::new_bad_argument(1, err));
            }
        };

        if index < 0 {
            index += call_ctx.arg_count() as i64 + 1;
        }

        if index <= 0 {
            return Err(RuntimeError::new_bad_argument(
                1,
                RuntimeError::from(RuntimeErrorData::OutOfBounds),
            ));
        }

        let index = index as usize;
        call_ctx.return_arg(index, ctx);
        Ok(())
    });
    select.rehydrate("lua.select", ctx)?;

    // tonumber
    let tonumber = ctx.create_function(|call_ctx, ctx| {
        let (string, base): (Option<ByteString>, Option<i64>) = call_ctx.get_args(ctx)?;

        let Some(base) = base else {
            let Some(string) = string else {
                // lua allows nil only if no base is supplied
                return call_ctx.return_values(Value::default(), ctx);
            };

            // normal parsing
            return call_ctx.return_values(parse_number(&string.to_string_lossy()), ctx);
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

        if bytes.starts_with(b"+") {
            bytes = &bytes[1..];
        } else if bytes.starts_with(b"-") {
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

                    return call_ctx.return_values(Value::default(), ctx);
                }
            };

            if digit >= base {
                // invalid digit
                return call_ctx.return_values(Value::default(), ctx);
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
                return call_ctx.return_values(Value::default(), ctx);
            }
        }

        call_ctx.return_values(n, ctx)
    });
    tonumber.rehydrate("lua.tonumber", ctx)?;

    // pcall
    let pcall = ctx.create_resumable_function(|(call_ctx, result, state), ctx| {
        let first_call = state.is_empty();
        ctx.store_multi(state);

        if first_call {
            result?;

            let (function, args): (FunctionRef, MultiValue) = call_ctx.get_args(ctx)?;

            ctx.resume_call_with_state(true)?;

            let values = function.call::<_, MultiValue>(args, ctx)?;
            call_ctx.return_values(values, ctx)?;
        } else {
            // handle the result of the call
            match result {
                Ok(values) => {
                    // return the success flag and pass the return values
                    call_ctx.return_values((true, values), ctx)?;
                }
                Err(err) => {
                    // return the success flag and the error as a value
                    call_ctx.return_values((false, err.to_string()), ctx)?;
                }
            }
        }

        Ok(())
    });
    pcall.rehydrate("lua.pcall", ctx)?;

    // xpcall
    let xpcall = ctx.create_resumable_function(|(call_ctx, result, state), ctx| {
        let handler: Option<FunctionRef> = state.unpack(ctx)?;

        if let Some(handler) = handler {
            // resumed
            if let Err(err) = result {
                let mut err_message = err.to_string();

                if let Err(handler_err) = handler.call::<_, ()>(err_message, ctx) {
                    err_message = handler_err.to_string();
                    // pass our handler's error into itself, give up on future errors (lua does not specify max retries)
                    let _ = handler.call::<_, ()>(err_message, ctx);
                }

                return Err(err);
            }

            call_ctx.return_args(.., ctx);
        } else {
            result?;

            // first call
            let (function, handler, args): (FunctionRef, FunctionRef, MultiValue) =
                call_ctx.get_args(ctx)?;

            ctx.resume_call_with_state(handler.clone())?;

            let values = function.call::<_, MultiValue>(args, ctx)?;
            call_ctx.return_values(values, ctx)?;
        }

        Ok(())
    });
    xpcall.rehydrate("lua.xpcall", ctx)?;

    // load
    let load = ctx.create_function(move |call_ctx, ctx| {
        let (chunkname, mode, env): (Option<String>, Option<ByteString>, Option<TableRef>) =
            call_ctx.get_args_at(1, ctx)?;

        // resolve source and label, allowing errors to bubble up
        let mut source_bytes = Vec::new();
        let label;

        if let Ok(byte_string) = call_ctx.get_arg::<ByteString>(0, ctx) {
            label = chunkname.unwrap_or_else(|| {
                format!(
                    "[string {:?}]",
                    str::from_utf8(byte_string.as_bytes()).unwrap_or_default()
                )
            });

            source_bytes.extend(byte_string.as_bytes());
        } else {
            label = chunkname.unwrap_or_else(|| String::from("(load)"));

            // build by calling until an empty string or nil is returned
            let chunk_builder: FunctionRef = call_ctx.get_arg(0, ctx)?;

            loop {
                let value = chunk_builder.call::<_, Value>((), ctx)?;

                if value.is_nil() {
                    break;
                }

                let string_ref = StringRef::from_value(value, ctx)?;
                let bytes = string_ref.fetch(ctx)?.as_bytes();

                if bytes.is_empty() {
                    break;
                }

                source_bytes.extend(bytes);
            }
        }

        // compilation errors are returned instead of thrown
        let try_block_ctx = &mut *ctx;
        let try_block = move || {
            let allows_text = mode
                .as_ref()
                .map(|mode| mode.as_bytes().contains(&b't'))
                .unwrap_or(true);

            if !allows_text {
                return Err(format!(
                    "attempt to load a text chunk (mode is '{}')",
                    mode.as_ref()
                        .map(|m| m.to_string_lossy())
                        .unwrap_or("".into()),
                ));
            }

            let source = String::from_utf8(source_bytes).map_err(|err| err.to_string())?;
            let module = compile(&source).map_err(|err| err.to_string())?;

            try_block_ctx
                .load_function(label, env, module)
                .map_err(|err| err.to_string())
        };

        match try_block() {
            Ok(function_ref) => call_ctx.return_values(function_ref, ctx),
            Err(err) => call_ctx.return_values((Value::Nil, err), ctx),
        }
    });
    load.rehydrate("lua.load", ctx)?;

    // loadfile
    let loadfile = ctx.create_function(|call_ctx, ctx| {
        let (path, mode, env): (String, Option<ByteString>, Option<TableRef>) =
            call_ctx.get_args(ctx)?;

        // compilation errors are returned instead of thrown
        let try_block_ctx = &mut *ctx;
        let try_block = move || {
            let source = std::fs::read_to_string(&path).map_err(|err| err.to_string())?;

            let allows_text = mode
                .as_ref()
                .map(|mode| mode.as_bytes().contains(&b't'))
                .unwrap_or(true);

            if !allows_text {
                return Err(format!(
                    "attempt to load a text chunk (mode is '{}')",
                    mode.as_ref()
                        .map(|m| m.to_string_lossy())
                        .unwrap_or("".into()),
                ));
            }

            let module = compile(&source).map_err(|err| err.to_string())?;

            try_block_ctx
                .load_function(path, env, module)
                .map_err(|err| err.to_string())
        };

        match try_block() {
            Ok(function_ref) => call_ctx.return_values(function_ref, ctx),
            Err(err) => call_ctx.return_values((Value::Nil, err), ctx),
        }
    });
    loadfile.rehydrate("lua.loadfile", ctx)?;

    // todo: warn

    if !rehydrating {
        let env = ctx.default_environment();
        env.set("_G", env.clone(), ctx)?;
        env.set("_VERSION", "Lua 5.3", ctx)?;
        env.set("assert", assert, ctx)?;
        env.set("collectgarbage", collectgarbage, ctx)?;
        env.set("error", error, ctx)?;
        env.set("print", print, ctx)?;
        env.set("tostring", tostring, ctx)?;
        env.set("type", type_name, ctx)?;
        env.set("getmetatable", getmetatable, ctx)?;
        env.set("setmetatable", setmetatable, ctx)?;
        env.set("rawequal", rawequal, ctx)?;
        env.set("rawget", rawget, ctx)?;
        env.set("rawlen", rawlen, ctx)?;
        env.set("rawset", rawset, ctx)?;
        env.set("next", next, ctx)?;
        env.set("ipairs", ipairs, ctx)?;
        env.set("pairs", pairs, ctx)?;
        env.set("select", select, ctx)?;
        env.set("tonumber", tonumber, ctx)?;
        env.set("pcall", pcall, ctx)?;
        env.set("xpcall", xpcall, ctx)?;
        env.set("load", load, ctx)?;
        env.set("loadfile", loadfile, ctx)?;
    }

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
        Value::Coroutine(f) => Ok(format!("thread: 0x{:x}", f.id())),
    }
}
