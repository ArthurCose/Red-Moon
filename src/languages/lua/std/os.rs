use crate::errors::RuntimeError;
use crate::interpreter::{IntoValue, Vm};

use cpu_time::ProcessTime;

pub fn impl_os(vm: &mut Vm) -> Result<(), RuntimeError> {
    let os = vm.create_table();

    // clock
    let clock = vm.create_native_function(|mut args, vm| {
        args.clear();

        let duration = ProcessTime::try_now()
            .map(|t| t.as_duration())
            .unwrap_or_default();

        args.push_front(duration.as_secs_f64().into_value(vm)?);

        Ok(args)
    });
    os.raw_set("clock", clock, vm)?;

    let env = vm.default_environment();
    env.set("os", os, vm)?;

    Ok(())
}
