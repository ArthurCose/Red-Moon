use pretty_assertions::assert_eq;
use red_moon::interpreter::{Value, Vm};

#[test]
fn basic() {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    let env = ctx.default_environment();
    env.raw_set("a", 1, ctx).unwrap();

    let mut snapshot = vm.clone();
    let ctx = &mut vm.context();

    env.raw_set("a", 2, ctx).unwrap();

    assert_eq!(Value::Integer(2), env.raw_get("a", ctx).unwrap());

    assert_eq!(
        Value::Integer(1),
        env.raw_get("a", &mut snapshot.context()).unwrap()
    );
}
