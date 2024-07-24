use red_moon::interpreter::{Chunk, Instruction, Module, MultiValue, ReturnMode, Value, Vm};

#[test]
fn instructions_print() {
    let mut vm = Vm::default();
    let ctx = &mut vm.context();

    let print_ref = ctx.create_native_function(|args, ctx| {
        let len = args.len();

        for (i, arg) in args.to_vec().into_iter().enumerate() {
            match arg {
                Value::Nil => print!("nil"),
                Value::Bool(b) => print!("{b}"),
                Value::Integer(n) => print!("{n}"),
                Value::Float(n) => print!("{n}"),
                Value::Table(_) => print!("table"),
                Value::Function(_) => print!("function"),
                Value::Coroutine(_) => print!("thread"),
                Value::String(string_ref) => {
                    print!("{}", string_ref.fetch(ctx).unwrap().to_string_lossy())
                }
            }

            if i < len - 1 {
                print!("\t");
            }
        }

        MultiValue::pack((), ctx)
    });

    let env = ctx.default_environment();
    env.raw_set("print", print_ref, ctx).unwrap();

    let byte_strings: Vec<&[u8]> = vec![b"print", b"hello", b"world", b"!"];

    let function_ref = ctx
        .load_function(
            "",
            None,
            Module {
                chunks: vec![Chunk {
                    env: Some(0),
                    up_values: Vec::new(),
                    dependencies: Default::default(),
                    byte_strings,
                    numbers: vec![3],
                    instructions: vec![
                        // Copy `_ENV` into the first stack register
                        Instruction::CopyUpValue(0, 0),
                        // replace with `_ENV.print`
                        Instruction::CopyTableField(0, 0),
                        Instruction::Constant(0),
                        // "!" ("hello world !" in reverse)
                        Instruction::LoadBytes(4, 3),
                        // "world"
                        Instruction::LoadBytes(3, 2),
                        // "hello"
                        Instruction::LoadBytes(2, 1),
                        // load arg count (3)
                        Instruction::LoadInt(1, 0),
                        // call `_ENV.print`
                        Instruction::Call(0, ReturnMode::Multi),
                    ],
                    source_map: Default::default(),
                }],
                main: 0,
            },
        )
        .unwrap();

    function_ref.call::<_, ()>((), ctx).unwrap();
}
