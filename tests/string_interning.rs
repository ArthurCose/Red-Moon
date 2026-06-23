use red_moon::errors::RuntimeError;
use red_moon::interpreter::Vm;
use red_moon::languages::lua::compile;
use red_moon::languages::lua::std::load_basic;
use red_moon::values::ByteString;

#[test]
fn string_interning() -> Result<(), RuntimeError> {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    load_basic(ctx)?;

    const STR: &[u8] = b"test";

    // make sure these map to the same string
    let string_ref_a = ctx.intern_string(STR);
    let string_ref_b = ctx.intern_string(STR);
    assert_eq!(string_ref_a, string_ref_b);

    // make sure interned strings match with external strings
    let string_a = string_ref_a.fetch(ctx)?;
    let string_c = ByteString::from(STR);
    assert_eq!(string_a, &string_c);

    // strings should match internally
    let env = ctx.default_environment();
    env.set("a", string_ref_a, ctx)?;
    env.set("b", string_ref_b, ctx)?;
    env.set("c", string_c, ctx)?;

    // we don't compare bytes inside of the vm
    let module = compile("assert(a == b) assert(a == c)").unwrap();
    ctx.load_function(file!(), None, module)?
        .call::<_, ()>((), ctx)?;

    Ok(())
}
