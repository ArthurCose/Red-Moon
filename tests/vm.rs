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
                Instruction::CopyLocal(0, 0),
                Instruction::LoadBytes(1, 0),
                Instruction::CopyTableValue(0, 0, 1),
                Instruction::LoadBytes(4, 3),
                Instruction::LoadBytes(3, 2),
                Instruction::LoadBytes(2, 1),
                Instruction::LoadInt(1, 0),
                Instruction::Call(0, ReturnMode::Multi),
            ],
            chunks: Default::default(),
            source_map: Default::default(),
        },
    );

    function_ref.call::<_, ()>((), &mut vm).unwrap();
}
