use pretty_assertions::assert_eq;
use red_moon::interpreter::Vm;
use red_moon::tag_native_type;
use red_moon::values::Value;

#[test]
fn basic() {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    // store a variable
    let env = ctx.default_environment();
    env.raw_set("a", 1, ctx).unwrap();

    // take a snapshot
    let mut snapshot = vm.clone();

    // update variable
    let ctx = &mut vm.context();
    env.raw_set("a", 2, ctx).unwrap();
    assert_eq!(Value::Integer(2), env.raw_get("a", ctx).unwrap());

    // check snapshot
    assert_eq!(
        Value::Integer(1),
        env.raw_get("a", &mut snapshot.context()).unwrap()
    );
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
