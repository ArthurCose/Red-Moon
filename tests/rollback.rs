use pretty_assertions::assert_eq;
use red_moon::interpreter::{Value, Vm};

#[test]
fn basic() {
    let mut vm = Vm::default();

    let env = vm.default_environment();
    env.raw_set("a", 1, &mut vm).unwrap();

    let mut snapshot = vm.clone();

    env.raw_set("a", 2, &mut vm).unwrap();

    assert_eq!(Value::Integer(2), env.raw_get("a", &mut vm).unwrap());

    assert_eq!(Value::Integer(1), env.raw_get("a", &mut snapshot).unwrap());
}
