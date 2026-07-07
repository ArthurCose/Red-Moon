use crate::errors::RuntimeError;
use crate::interpreter::{HookMask, NativeCallContext, VmContext};
use crate::values::{ByteString, FunctionRef, TableRef, ThreadRef, Value};
use std::fmt::Write;

pub fn load_debug(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // getregistry
    let getregistry =
        ctx.create_function(|call_ctx, ctx| call_ctx.return_values(ctx.registry(), ctx));
    let rehydrating = getregistry.rehydrate("debug.getregistry", ctx)?;

    // getmetatable
    let getmetatable = ctx.create_function(|call_ctx, ctx| {
        let value: Value = call_ctx.get_args(ctx)?;

        let metatable = match value {
            Value::String(_) => Some(ctx.string_metatable()),
            Value::Table(table) => table.metatable(ctx)?,
            _ => return Ok(()),
        };

        call_ctx.return_values(metatable, ctx)
    });
    getmetatable.rehydrate("debug.getmetatable", ctx)?;

    // setmetatable
    let setmetatable = ctx.create_function(|call_ctx, ctx| {
        let (value, metatable): (Value, Option<TableRef>) = call_ctx.get_args(ctx)?;

        if let Value::Table(table) = &value {
            table.set_metatable(metatable.as_ref(), ctx)?;
        }

        call_ctx.return_values(value, ctx)
    });
    setmetatable.rehydrate("debug.setmetatable", ctx)?;

    // gethook
    let gethook = ctx.create_function(|call_ctx, ctx| {
        let (thread, _) = get_thread(call_ctx, ctx);

        // resolve hook
        let Some(fn_ref) = thread.hook(ctx)? else {
            return call_ctx.return_values(Value::Nil, ctx);
        };

        // resolve mask
        let mask = thread.hook_mask(ctx)?;
        let mut mask_bytes = Vec::with_capacity(3);

        if mask.contains(HookMask::CALL) {
            mask_bytes.push(b'c');
        }

        if mask.contains(HookMask::RETURN) {
            mask_bytes.push(b'r');
        }

        if mask.contains(HookMask::LINE) {
            mask_bytes.push(b'l');
        }

        let mask_string_ref = ctx.intern_string(&mask_bytes);

        // resolve count
        let count = thread.hook_count(ctx)?;

        call_ctx.return_values((fn_ref, mask_string_ref, count), ctx)
    });
    gethook.rehydrate("debug.gethook", ctx)?;

    // traceback
    let traceback = ctx.create_function(|call_ctx, ctx| {
        let (thread, arg_offset) = get_thread(call_ctx, ctx);
        let (message, level): (Value, Option<i64>) = call_ctx.get_args_at(arg_offset, ctx)?;

        let mut message = match message {
            Value::Nil => String::new(),
            Value::String(string_ref) => {
                string_ref.fetch(ctx)?.to_string_lossy().to_string() + "\n"
            }
            // "If message is present but is neither a string nor nil, this function returns message without further processing"
            _ => return call_ctx.return_values(message, ctx),
        };

        // write stack trace
        let level = level.unwrap_or_default().max(0) as usize;
        let trace = thread.traceback(level, ctx)?;

        let _ = write!(&mut message, "{trace}");

        call_ctx.return_values(message, ctx)
    });
    traceback.rehydrate("debug.traceback", ctx)?;

    // sethook
    let sethook = ctx.create_function(|call_ctx, ctx| {
        let (thread, arg_offset) = get_thread(call_ctx, ctx);

        let Some(callback) = call_ctx.get_arg::<Option<FunctionRef>>(arg_offset, ctx)? else {
            // when the first arg is nil, ignore the remaining args and remove the hook
            thread.remove_hook(ctx)?;
            return Ok(());
        };

        let (mask_string, count): (ByteString, Option<usize>) =
            call_ctx.get_args_at(arg_offset + 1, ctx)?;

        // resolve mask
        let mut mask = HookMask::default();

        for byte in mask_string.as_bytes() {
            match byte {
                b'c' => mask.set(HookMask::CALL, true),
                b'r' => mask.set(HookMask::RETURN, true),
                b'l' => mask.set(HookMask::LINE, true),
                _ => {}
            }
        }

        let count = count.unwrap_or(0);

        if count > 0 {
            mask.set(HookMask::INSTRUCTION, true);
        }

        thread.set_hook(mask, count, callback, ctx)?;

        Ok(())
    });
    sethook.rehydrate("debug.sethook", ctx)?;

    if !rehydrating {
        let debug = ctx.create_table();
        debug.raw_set("getregistry", getregistry, ctx)?;
        debug.raw_set("getmetatable", getmetatable, ctx)?;
        debug.raw_set("setmetatable", setmetatable, ctx)?;
        debug.raw_set("gethook", gethook, ctx)?;
        debug.raw_set("sethook", sethook, ctx)?;
        debug.raw_set("traceback", traceback, ctx)?;

        let env = ctx.default_environment();
        env.set("debug", debug, ctx)?;
    }

    Ok(())
}

fn get_thread(call_ctx: &mut NativeCallContext, ctx: &mut VmContext) -> (ThreadRef, usize) {
    call_ctx
        .get_arg::<ThreadRef>(0, ctx)
        .map(|thread| (thread, 1))
        .unwrap_or_else(|_| (ctx.top_thread(), 0))
}
