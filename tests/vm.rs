use red_moon::interpreter::{Instruction, Module, MultiValue, Primitive, ReturnMode, Value, Vm};

#[test]
fn instructions_print() {
    let mut vm = Vm::default();
    let print_ref = vm.create_native_function(|args, vm| {
        let len = args.len();

        for (i, arg) in args.to_vec().into_iter().enumerate() {
            match arg {
                Value::Primitive(Primitive::Nil) => print!("nil"),
                Value::Primitive(Primitive::Bool(b)) => print!("{b}"),
                Value::Primitive(Primitive::Integer(n)) => print!("{n}"),
                Value::Primitive(Primitive::Float(n)) => print!("{n}"),
                Value::Table(_) => print!("table"),
                Value::Function(_) => print!("function"),
                Value::String(string_ref) => {
                    print!("{}", string_ref.fetch(vm).unwrap().to_string_lossy())
                }
            }

            if i < len - 1 {
                print!("\t");
            }
        }

        MultiValue::pack((), vm)
    });

    let env = vm.default_environment();
    env.raw_set("print", print_ref, &mut vm).unwrap();

    let byte_strings: Vec<&[u8]> = vec![b"print", b"hello", b"world", b"!"];

    let function_ref = vm.load_function(
        "",
        None,
        Module {
            byte_strings,
            numbers: vec![3],
            instructions: vec![
                // Copy `_ENV` into the first stack register
                Instruction::CopyUpValue(0, 0),
                // replace with `_ENV.print`
                Instruction::CopyTableField(0, 0, 0),
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
            chunks: Default::default(),
            source_map: Default::default(),
        },
    );

    function_ref.call::<_, ()>((), &mut vm).unwrap();
}
