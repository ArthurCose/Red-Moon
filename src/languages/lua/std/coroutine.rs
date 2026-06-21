use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::{
    CoroutineRef, CoroutineStatus, FunctionRef, MultiValue, Value, VmContext,
};

pub fn impl_coroutine(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // todo: close

    // create
    let create = ctx.create_function(|call_ctx, ctx| {
        let function = call_ctx.get_args(ctx)?;
        let co = ctx.create_coroutine(function)?;
        call_ctx.return_values(co, ctx)
    });
    let rehydrating = create.rehydrate("coroutine.create", ctx)?;

    // isyieldable
    let isyieldable = ctx.create_function(|call_ctx, ctx| {
        let co: Option<CoroutineRef> = call_ctx.get_args(ctx)?;

        if !ctx.is_yieldable() {
            return call_ctx.return_values(false, ctx);
        }

        let top_coroutine = ctx.top_coroutine();
        let yieldable = if co.is_some() {
            top_coroutine == co
        } else {
            top_coroutine.is_some()
        };

        call_ctx.return_values(yieldable, ctx)
    });
    isyieldable.rehydrate("coroutine.isyieldable", ctx)?;

    // resume
    let resume = ctx.create_function(|call_ctx, ctx| {
        let (co, args): (CoroutineRef, MultiValue) = call_ctx.get_args(ctx)?;

        match co.resume(args, ctx) {
            Ok(values) => call_ctx.return_values((true, values), ctx),
            Err(err) => call_ctx.return_values((false, err.to_string()), ctx),
        }
    });
    resume.rehydrate("coroutine.resume", ctx)?;

    // running
    let running = ctx.create_function(|call_ctx, ctx| {
        let co = ctx.top_coroutine();

        if let Some(co) = co {
            call_ctx.return_values((co, false), ctx)?;
        } else {
            call_ctx.return_values((Value::Nil, true), ctx)?;
        }

        Ok(())
    });
    running.rehydrate("coroutine.running", ctx)?;

    // status
    let status = ctx.create_function(|call_ctx, ctx| {
        let co: CoroutineRef = call_ctx.get_args(ctx)?;
        let status = match co.status(ctx)? {
            CoroutineStatus::Suspended => "suspended",
            CoroutineStatus::Running => {
                if ctx.top_coroutine() != Some(co) {
                    "normal"
                } else {
                    "running"
                }
            }
            CoroutineStatus::Dead => "dead",
        };
        call_ctx.return_values(status, ctx)
    });
    status.rehydrate("coroutine.status", ctx)?;

    // wrap
    let wrapped = ctx.create_function(|call_ctx, ctx| {
        let args: MultiValue = call_ctx.get_args(ctx)?;

        if let Some(co) = call_ctx.read_capture::<CoroutineRef>(ctx) {
            let values = co.clone().resume(args, ctx)?;
            call_ctx.return_values(values, ctx)?;
        }

        Ok(())
    });
    wrapped.rehydrate("coroutine.wrap.wrapped", ctx)?;

    let wrap = ctx.create_function(|call_ctx, ctx| {
        let function = call_ctx.get_args(ctx)?;
        let co = ctx.create_coroutine(function)?;

        if let Some(f) = call_ctx.read_capture::<FunctionRef>(ctx) {
            let f = f.clone().create_closure(co, ctx)?;
            call_ctx.return_values(f, ctx)?;
        }

        Ok(())
    });
    let wrap = wrap.create_closure(wrapped, ctx)?;
    wrap.rehydrate("coroutine.wrap", ctx)?;

    // yield
    let r#yield = ctx.create_resumable_function(|(call_ctx, result, state), ctx| {
        let first_call = state.is_empty();
        ctx.store_multi(state);

        if first_call {
            ctx.resume_call_with_state(true)?;
            result?;

            let args = call_ctx.get_args(ctx)?;
            Err(RuntimeErrorData::Yield(args).into())
        } else {
            call_ctx.return_args(.., ctx);
            result
        }
    });
    r#yield.rehydrate("coroutine.yield", ctx)?;

    if !rehydrating {
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
