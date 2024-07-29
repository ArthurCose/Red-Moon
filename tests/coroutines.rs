use red_moon::errors::RuntimeError;
use red_moon::interpreter::{FunctionRef, MultiValue, Vm};
use red_moon::languages::lua::std::{impl_basic, impl_coroutine};
use red_moon::languages::lua::LuaCompiler;

#[test]
fn resumable() -> Result<(), RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    impl_basic(ctx)?;
    impl_coroutine(ctx)?;

    let for_range = ctx.create_resumable_function(|(result, state), ctx| {
        let (i, end, f): (i64, i64, FunctionRef) = if state.is_empty() {
            // just called, the result passed in are the args
            let args = result?;
            args.unpack_args(ctx)?
        } else {
            // restore from state
            state.unpack(ctx)?
        };

        if i < end {
            // set state to allow yielding and provide information on how to resume
            ctx.resume_call_with_state((i + 1, end, f.clone()))?;

            // call a function that can yield
            f.call(i, ctx)?;
        }

        MultiValue::pack((), ctx)
    });

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

    let compiler = LuaCompiler::default();
    let module = compiler.compile(SOURCE).unwrap();
    ctx.load_function(file!(), None, module)?.call((), ctx)?;

    Ok(())
}
