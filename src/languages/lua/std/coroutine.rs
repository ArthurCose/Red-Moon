use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{CoroutineRef, CoroutineStatus, MultiValue, Value, VmContext};

pub fn impl_coroutine(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // todo: close

    // create
    let create = ctx.create_function(|args, ctx| {
        let function = args.unpack_args(ctx)?;
        let co = ctx.create_coroutine(function)?;
        MultiValue::pack(co, ctx)
    });
    let hydrating = create.hydrate("coroutine.create", ctx)?;

    // isyieldable
    let isyieldable = ctx.create_function(|args, ctx| {
        let (co, mut args): (Option<CoroutineRef>, MultiValue) = args.unpack(ctx)?;

        if !ctx.is_yieldable() {
            args.push_front(Value::Bool(false));
            return Ok(args);
        }

        let top_coroutine = ctx.top_coroutine();
        let yieldable = if co.is_some() {
            top_coroutine == co
        } else {
            top_coroutine.is_some()
        };

        args.push_front(Value::Bool(yieldable));

        Ok(args)
    });
    create.hydrate("coroutine.isyieldable", ctx)?;

    // resume
    let resume = ctx.create_function(|args, ctx| {
        let (co, args): (CoroutineRef, MultiValue) = args.unpack_args(ctx)?;

        match co.resume(args, ctx) {
            Ok(mut values) => {
                values.push_front(Value::Bool(true));
                Ok(values)
            }
            Err(err) => MultiValue::pack((false, err.to_string()), ctx),
        }
    });
    create.hydrate("coroutine.resume", ctx)?;

    // running
    let running = ctx.create_function(|mut args, ctx| {
        args.clear();

        let co = ctx.top_coroutine();

        args.push_front(Value::Bool(co.is_none()));

        let co_value = if let Some(co) = co {
            Value::Coroutine(co)
        } else {
            Value::Nil
        };

        args.push_front(co_value);

        Ok(args)
    });
    create.hydrate("coroutine.running", ctx)?;

    // status
    let suspended_string = ctx.intern_string(b"suspended");
    let running_string = ctx.intern_string(b"running");
    let normal_string = ctx.intern_string(b"normal");
    let dead_string = ctx.intern_string(b"dead");

    let status = ctx.create_function(move |args, ctx| {
        let co: CoroutineRef = args.unpack_args(ctx)?;
        let status = match co.status(ctx)? {
            CoroutineStatus::Suspended => suspended_string.clone(),
            CoroutineStatus::Running => {
                if ctx.top_coroutine() != Some(co) {
                    normal_string.clone()
                } else {
                    running_string.clone()
                }
            }
            CoroutineStatus::Dead => dead_string.clone(),
        };
        MultiValue::pack(status, ctx)
    });
    create.hydrate("coroutine.status", ctx)?;

    // wrap
    let wrap = ctx.create_function(|args, ctx| {
        let function = args.unpack_args(ctx)?;
        let co = ctx.create_coroutine(function)?;

        let f = ctx.create_function(move |args, ctx| co.resume(args, ctx));

        MultiValue::pack(f, ctx)
    });
    create.hydrate("coroutine.wrap", ctx)?;

    // yield
    let r#yield = ctx.create_function(move |args, _| Err(RuntimeErrorData::Yield(args).into()));
    r#yield.set_resume_callback(|(result, _), _| result, ctx)?;
    create.hydrate("coroutine.yield", ctx)?;

    if !hydrating {
        let coroutine = ctx.create_table();
        coroutine.raw_set("create", create, ctx)?;
        coroutine.raw_set("isyieldable", isyieldable, ctx)?;
        coroutine.raw_set("resume", resume, ctx)?;
        coroutine.raw_set("running", running, ctx)?;
        coroutine.raw_set("status", status, ctx)?;
        coroutine.raw_set("wrap", wrap, ctx)?;
        coroutine.raw_set("yield", r#yield, ctx)?;

        let env = ctx.default_environment();
        env.set("coroutine", coroutine, ctx)?;
    }

    Ok(())
}
