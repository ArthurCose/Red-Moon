use pretty_assertions::assert_eq;
use red_moon::errors::RuntimeError;
use red_moon::interpreter::Vm;
use red_moon::tag_native_type;
use red_moon::values::Value;

#[test]
fn basic() -> Result<(), RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    // store a variable
    let env = ctx.default_environment();
    env.raw_set("a", 1, ctx)?;

    // take a snapshot
    let mut snapshot = vm.clone();

    // update variable
    let ctx = &mut vm.context();
    env.raw_set("a", 2, ctx)?;
    assert_eq!(Value::Integer(2), env.raw_get("a", ctx)?);

    // check snapshot
    assert_eq!(
        Value::Integer(1),
        env.raw_get("a", &mut snapshot.context())?
    );

    Ok(())
}

#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct IntegerCapture(i32);

tag_native_type!(IntegerCapture);

#[test]
fn singletons() {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    // store a value
    ctx.set_singleton(IntegerCapture(1));

    // take a snapshot
    let mut snapshot = vm.clone();

    // update value
    let ctx = &mut vm.context();
    ctx.set_singleton(IntegerCapture(2));

    // check snapshot
    let snapshot_ctx = &mut snapshot.context();
    assert_eq!(&IntegerCapture(1), snapshot_ctx.singleton().unwrap());
}

#[test]
fn captures() -> Result<(), RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    // store a value
    let closure = ctx
        .create_function(|call_ctx, ctx| {
            let a = call_ctx.get_capture_mut::<IntegerCapture>(ctx).unwrap();
            a.0 += 1;

            call_ctx.return_values(a.0, ctx)
        })
        .create_closure(IntegerCapture(0), ctx)?;

    // test out incrementing
    assert_eq!(closure.call::<_, i32>((), ctx)?, 1);
    assert_eq!(closure.call::<_, i32>((), ctx)?, 2);

    // take a snapshot
    let mut snapshot = vm.clone();

    // update value
    let ctx = &mut vm.context();
    assert_eq!(closure.call::<_, i32>((), ctx)?, 3);
    assert_eq!(closure.call::<_, i32>((), ctx)?, 4);

    // check snapshot
    let snapshot_ctx = &mut snapshot.context();
    assert_eq!(closure.call::<_, i32>((), snapshot_ctx)?, 3);

    Ok(())
}
