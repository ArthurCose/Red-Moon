#![cfg(feature = "serde")]

use red_moon::errors::RuntimeError;
use red_moon::interpreter::{FunctionRef, MultiValue, TableRef, Vm};

fn create_vm() -> Result<Vm, RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();
    let env = ctx.default_environment();

    // create garbage for making holes
    ctx.create_table();
    ctx.create_table();

    let table_a = ctx.create_table();
    let table_b = ctx.create_table();
    table_a.set("b", table_b.clone(), ctx)?;
    table_b.set("a", table_a.clone(), ctx)?;
    table_b.set(1, 2, ctx)?;

    env.set("a", table_a, ctx)?;

    // create function
    let f = ctx.create_function(|args, _| Ok(args));

    assert!(!f.hydrate("hydrated_fn", ctx)?);
    env.set("native_fn", f, ctx)?;

    // create holes and make sure the hydration tag doesn't get collected
    ctx.gc_collect();

    Ok(vm)
}

fn test_vm(vm: &mut Vm) -> Result<(), RuntimeError> {
    let ctx = &mut vm.context();
    let env = ctx.default_environment();

    // test strings and tables
    let table_a: TableRef = env.get("a", ctx)?;
    let table_b: TableRef = table_a.get("b", ctx)?;

    // test cycle
    let table_a2: TableRef = table_b.get("a", ctx)?;
    assert_eq!(table_a, table_a2);

    // test number
    assert_eq!(table_b.get::<_, i32>(1, ctx)?, 2);

    // test dehydrated function
    let f: FunctionRef = env.get("native_fn", ctx)?;
    assert_eq!(f.call::<_, MultiValue>(1, ctx)?, ctx.create_multi());

    // hydrate
    let f = ctx.create_function(|args, _| Ok(args));
    assert!(f.hydrate("hydrated_fn", ctx)?);
    assert_eq!(f.call::<_, MultiValue>(1, ctx)?, MultiValue::pack(1, ctx)?);

    Ok(())
}

#[test]
fn bincode() -> Result<(), RuntimeError> {
    let serialized_vm = bincode::serialize(&create_vm()?).unwrap();

    let mut vm: Vm = bincode::deserialize(&serialized_vm).unwrap();
    test_vm(&mut vm)?;

    Ok(())
}

#[test]
fn rmp() -> Result<(), RuntimeError> {
    let serialized_vm = rmp_serde::to_vec(&create_vm()?).unwrap();

    let mut vm: Vm = rmp_serde::from_slice(&serialized_vm).unwrap();
    test_vm(&mut vm)?;

    Ok(())
}

#[test]
fn ron() -> Result<(), RuntimeError> {
    let serialized_vm = ron::to_string(&create_vm()?).unwrap();

    let mut vm: Vm = ron::from_str(&serialized_vm).unwrap();
    test_vm(&mut vm)?;

    Ok(())
}
