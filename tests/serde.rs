#![cfg(feature = "serde")]

use red_moon::errors::{RuntimeError, RuntimeErrorData};
use red_moon::interpreter::{CoroutineRef, FunctionRef, MultiValue, TableRef, Vm, tag_native_type};
use red_moon::languages::lua::compile;
use red_moon::languages::lua::std::impl_coroutine;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct MySingleton(i32);

tag_native_type!(MySingleton);

fn create_vm() -> Result<Vm, RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    impl_coroutine(ctx)?;

    let env = ctx.default_environment();

    // create garbage for making holes
    ctx.create_table();
    ctx.create_table();

    // create resumable native function
    let resumable = ctx.create_resumable_function(|(_, result, _), ctx| {
        result?;

        // store "resumed" in state
        ctx.resume_call_with_state("resumed")?;

        Err(RuntimeErrorData::Yield(ctx.create_multi()).into())
    });

    assert!(!resumable.rehydrate("resumable_fn", ctx)?);
    env.set("resumable_fn", resumable, ctx)?;

    // load lua
    const SOURCE: &str = r#"
        local b = {}
        a = { b = b }
        b.a = a
        b[1] = 2

        function lua_fn()
            return "lua_fn success"
        end

        co = coroutine.create(resumable_fn)
        coroutine.resume(co)
    "#;

    let module = compile(SOURCE).unwrap();
    ctx.load_function(file!(), None, module)?
        .call::<_, ()>((), ctx)?;

    // create native function
    let f = ctx.create_function(|call_ctx, ctx| {
        call_ctx.return_args(.., ctx);
        Ok(())
    });

    assert!(!f.rehydrate("hydrated_fn", ctx)?);
    env.set("native_fn", f, ctx)?;

    // create holes and make sure the hydration tag doesn't get collected
    ctx.gc_collect();

    // add a singleton
    ctx.set_singleton(MySingleton(1));

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

    // test lua function
    let lua_f: FunctionRef = env.get("lua_fn", ctx)?;
    assert_eq!(lua_f.call::<_, String>((), ctx)?, "lua_fn success");

    // test dehydrated function
    let f: FunctionRef = env.get("native_fn", ctx)?;
    assert!(
        f.call::<_, MultiValue>(1, ctx)
            .is_err_and(|err| err.data == RuntimeErrorData::FunctionLostInSerialization)
    );

    // rehydrate
    let f = ctx.create_function(|call_ctx, ctx| {
        call_ctx.return_args(.., ctx);
        Ok(())
    });
    assert!(f.rehydrate("hydrated_fn", ctx)?);
    assert_eq!(f.call::<_, MultiValue>(1, ctx)?, MultiValue::pack(1, ctx)?);

    // test resumable, expecting "resumed" to be stored in state
    let resumable = ctx
        .create_resumable_function(|(call_ctx, _, state), ctx| call_ctx.return_values(state, ctx));
    assert!(resumable.rehydrate("resumable_fn", ctx)?);

    let co: CoroutineRef = env.get("co", ctx)?;
    assert_eq!(co.resume((), ctx)?, MultiValue::pack("resumed", ctx)?);

    // retrieve singleton
    let my_singleton: &MySingleton = ctx.singleton().unwrap();
    assert_eq!(my_singleton, &MySingleton(1));

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
fn ron() -> Result<(), RuntimeError> {
    let serialized_vm = ron::to_string(&create_vm()?).unwrap();

    let mut vm: Vm = ron::from_str(&serialized_vm).unwrap();
    test_vm(&mut vm)?;

    Ok(())
}
