#![cfg(feature = "serde")]

use red_moon::errors::{RuntimeError, RuntimeErrorData};
use red_moon::interpreter::{Vm, VmContext};
use red_moon::languages::lua::compile;
use red_moon::languages::lua::std::load_coroutine;
use red_moon::values::{
    FunctionRef, IntoValue, MultiValue, TableRef, ThreadRef, Value, tag_native_type,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct MySingleton(i32);

tag_native_type!(MySingleton);

// create a closure expecting an integer capture
fn create_closure_base(ctx: &mut VmContext) -> FunctionRef {
    ctx.create_function(|call_ctx, ctx| {
        let Some(Value::Integer(i)) = call_ctx.get_capture_mut::<Value>(ctx) else {
            panic!("Capture lost");
        };

        *i += 1;

        call_ctx.return_values(*i, ctx)
    })
}

fn create_vm() -> Result<Vm, RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    load_coroutine(ctx)?;

    let env = ctx.default_environment();

    // create garbage for making holes
    ctx.create_table();
    ctx.create_table();

    // create resumable native function
    let resumed_fn = ctx.create_function(|call_ctx, ctx| {
        let Some(message) = call_ctx.get_capture::<Value>(ctx) else {
            return Err(RuntimeError::new_invalid_internal_state());
        };

        call_ctx.return_values(message.clone(), ctx)
    });
    assert!(!resumed_fn.rehydrate("resumed_fn", ctx)?);

    let resumable = ctx
        .create_function(|call_ctx, ctx| {
            let message = "resumed".into_value(ctx)?;

            let Some(resume_function) = call_ctx.get_capture::<FunctionRef>(ctx) else {
                return Err(RuntimeError::new_invalid_internal_state());
            };

            let resume_function = resume_function.clone().create_closure(message, ctx)?;
            call_ctx.yield_data((), resume_function, ctx)?;

            Ok(())
        })
        .create_closure(resumed_fn, ctx)?;

    assert!(!resumable.rehydrate("resumable_fn", ctx)?);
    env.set("resumable_fn", resumable, ctx)?;

    // create a native closure
    let closure = create_closure_base(ctx);

    // rehydrate before capturing to test if the implementation propagates
    assert!(!closure.rehydrate("closure", ctx)?);

    // capture a value
    let closure = closure.create_closure(Value::Integer(0), ctx)?;
    env.set("native_closure", closure.clone(), ctx)?;

    assert_eq!(closure.call::<_, i64>((), ctx)?, 1);

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
        call_ctx.return_arg_range(.., ctx);
        Ok(())
    });

    assert!(!f.rehydrate("hydrated_fn", ctx)?);
    env.set("native_fn", f, ctx)?;

    // make sure the main thread (a nullptr) is serializable
    env.set("main_thread", ctx.main_thread(), ctx)?;

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
        call_ctx.return_arg_range(.., ctx);
        Ok(())
    });
    assert!(f.rehydrate("hydrated_fn", ctx)?);
    assert_eq!(f.call::<_, MultiValue>(1, ctx)?, MultiValue::pack(1, ctx)?);

    // test resumable, expecting "resumed" to be stored in state
    let resumed_fn = ctx.create_function(|call_ctx, ctx| {
        let Some(message) = call_ctx.get_capture::<Value>(ctx) else {
            return Err(RuntimeError::new_invalid_internal_state());
        };

        call_ctx.return_values(message.clone(), ctx)
    });
    assert!(resumed_fn.rehydrate("resumed_fn", ctx)?);

    let co: ThreadRef = env.get("co", ctx)?;
    assert_eq!(co.resume((), ctx)?, MultiValue::pack("resumed", ctx)?);

    // rehydrate closure using the same implementation
    let closure = create_closure_base(ctx);
    assert!(closure.rehydrate("closure", ctx)?);

    // test closure
    let stored_closure: FunctionRef = env.get("native_closure", ctx)?;
    assert_eq!(stored_closure.call::<_, i64>((), ctx)?, 2);

    // retrieve singleton
    let my_singleton: &MySingleton = ctx.singleton().unwrap();
    assert_eq!(my_singleton, &MySingleton(1));

    // compare main thread
    assert_eq!(
        env.get::<_, ThreadRef>("main_thread", ctx)?,
        ctx.main_thread()
    );

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
