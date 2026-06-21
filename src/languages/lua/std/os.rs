use crate::errors::RuntimeError;
use crate::interpreter::VmContext;
use cpu_time::ProcessTime;

pub fn load_os(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // clock
    let clock = ctx.create_function(|call_ctx, ctx| {
        let duration = ProcessTime::try_now()
            .map(|t| t.as_duration())
            .unwrap_or_default();

        call_ctx.return_values(duration.as_secs_f64(), ctx)
    });
    let rehydrating = clock.rehydrate("os.clock", ctx)?;

    if !rehydrating {
        let os = ctx.create_table();
        os.raw_set("clock", clock, ctx)?;

        let env = ctx.default_environment();
        env.set("os", os, ctx)?;
    }

    Ok(())
}
