use crate::errors::RuntimeError;
use crate::interpreter::{IntoValue, VmContext};

use cpu_time::ProcessTime;

pub fn impl_os(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // clock
    let clock = ctx.create_function(|mut args, ctx| {
        args.clear();

        let duration = ProcessTime::try_now()
            .map(|t| t.as_duration())
            .unwrap_or_default();

        args.push_front(duration.as_secs_f64().into_value(ctx)?);

        Ok(args)
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
