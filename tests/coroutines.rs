use red_moon::errors::RuntimeError;
use red_moon::interpreter::Vm;
use red_moon::languages::lua::compile;
use red_moon::languages::lua::std::{load_basic, load_coroutine};
use red_moon::tag_native_type;
use red_moon::values::FunctionRef;

#[test]
fn resumable() -> Result<(), RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    load_basic(ctx)?;
    load_coroutine(ctx)?;

    #[derive(Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    struct ForRangeResumeState {
        start: i64,
        end: i64,
        f: FunctionRef,
        resume_fn: FunctionRef,
    }

    tag_native_type!(ForRangeResumeState);

    let resumed_for_range = ctx.create_function(|call_ctx, ctx| {
        let Some(ForRangeResumeState { start, end, f, .. }) =
            call_ctx.get_capture::<ForRangeResumeState>(ctx)
        else {
            return Err(RuntimeError::new_invalid_internal_state());
        };

        let mut i = *start;
        let end = *end;
        let f = f.clone();

        while i < end {
            // call a function that can yield
            f.yieldable_call::<_, ()>(i, call_ctx, ctx, move |call_ctx, ctx| {
                let Some(ForRangeResumeState { f, resume_fn, .. }) =
                    call_ctx.get_capture::<ForRangeResumeState>(ctx)
                else {
                    return Err(RuntimeError::new_invalid_internal_state());
                };

                let f = f.clone();
                let resume_fn = resume_fn.clone();

                resume_fn.clone().create_closure(
                    ForRangeResumeState {
                        start: i + 1,
                        end,
                        f,
                        resume_fn,
                    },
                    ctx,
                )
            })??;

            i += 1;
        }

        Ok(())
    });

    let for_range = ctx
        .create_function(|call_ctx, ctx| {
            let (mut i, end, f): (i64, i64, FunctionRef) = call_ctx.get_args(ctx)?;

            while i < end {
                // call a function that can yield
                let f_capture = f.clone();
                f.yieldable_call::<_, ()>(i, call_ctx, ctx, move |call_ctx, ctx| {
                    let Some(resume_fn) = call_ctx.get_capture::<FunctionRef>(ctx) else {
                        return Err(RuntimeError::new_invalid_internal_state());
                    };

                    resume_fn.clone().create_closure(
                        ForRangeResumeState {
                            start: i + 1,
                            end,
                            f: f_capture,
                            resume_fn: resume_fn.clone(),
                        },
                        ctx,
                    )
                })??;

                i += 1;
            }

            Ok(())
        })
        .create_closure(resumed_for_range, ctx)?;

    let env = ctx.default_environment();
    env.set("for_range", for_range, ctx)?;

    // we want to yield every other result
    // allows us to test the function immediately resuming without yield
    // as well as resuming with yield
    const SOURCE: &str = r#"
        co = coroutine.create(function()
            for_range(1, 10, function(i)
                if i % 2 == 0 then
                    coroutine.yield(i)
                end
            end)
        end)

        assert(select(2, coroutine.resume(co)) == 2)
        assert(select(2, coroutine.resume(co)) == 4)
    "#;

    let module = compile(SOURCE).unwrap();
    ctx.load_function(file!(), None, module)?
        .call::<_, ()>((), ctx)?;

    Ok(())
}
