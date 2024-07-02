use pretty_assertions::assert_eq;
use red_moon::errors::{LuaCompilationError, RuntimeErrorData, SyntaxError};
use red_moon::interpreter::{MultiValue, Primitive, Value, Vm};
use red_moon::languages::lua::std::impl_basic;
use red_moon::languages::lua::{LuaCompiler, LuaTokenLabel};
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

#[test]
fn valid() {
    let folder_path = env!("CARGO_MANIFEST_DIR").to_string() + "/tests/lua/valid/";
    let test_files = vec![
        "expressions.lua",
        "functions.lua",
        "loops.lua",
        "metatables.lua",
        "munchausen_numbers.lua",
        "semicolons.lua",
        "tables.lua.txt",
        "variables.lua.txt",
    ];

    let out = Rc::new(RefCell::new(Vec::new()));

    let mut vm = Vm::default();
    impl_basic(&mut vm).unwrap();

    let env = vm.default_environment();

    // override print
    let out_capture = out.clone();

    let print_ref = vm.create_native_function(move |args, vm| {
        let len = args.len();

        let mut out = out_capture.borrow_mut();

        for (i, arg) in args.to_vec().into_iter().enumerate() {
            match arg {
                Value::Primitive(Primitive::Nil) => write!(&mut *out, "nil").unwrap(),
                Value::Primitive(Primitive::Bool(b)) => write!(&mut *out, "{b}").unwrap(),
                Value::Primitive(Primitive::Integer(n)) => write!(&mut *out, "{n}").unwrap(),
                Value::Primitive(Primitive::Float(n)) => write!(&mut *out, "{n:?}").unwrap(),
                Value::Table(_) => write!(&mut *out, "table").unwrap(),
                Value::Function(_) => write!(&mut *out, "function").unwrap(),
                Value::String(string_ref) => write!(
                    &mut *out,
                    "{}",
                    string_ref.fetch(vm).unwrap().to_string_lossy()
                )
                .unwrap(),
            }

            if i < len - 1 {
                write!(&mut *out, "\t").unwrap();
            }
        }

        writeln!(&mut *out).unwrap();

        MultiValue::pack((), vm)
    });

    env.raw_set("print", print_ref, &mut vm).unwrap();

    // the actual tests
    let compiler = LuaCompiler::default();

    for path in test_files {
        let full_path = folder_path.clone() + path;

        let source = std::fs::read_to_string(&full_path).expect(&full_path);
        let module = compiler.compile(&source).expect(path);
        let function_ref = vm.load_function(path, None, module);

        if let Err(err) = function_ref.call::<_, ()>((), &mut vm) {
            panic!(
                "{path}: {err}\n\n{}",
                String::from_utf8_lossy(&out.borrow())
            );
        }

        let mut out = out.borrow_mut();
        let output_path = folder_path.clone() + path + ".expected";
        let failed_path = folder_path.clone() + path + ".failed";

        if let Ok(data) = std::fs::read(&output_path) {
            if *out != data {
                std::fs::write(&failed_path, &*out).unwrap();
                assert_eq!(
                    &*String::from_utf8_lossy(&data),
                    &*String::from_utf8_lossy(&out),
                    "\n{}\n{}\n",
                    output_path,
                    failed_path
                );
            }

            // remove failed file if we passed
            let _ = std::fs::remove_file(failed_path);
        } else {
            // generate expected file
            std::fs::write(&output_path, &*out).unwrap();
        }

        out.clear();
    }
}

#[test]
fn invalid() {
    let folder_path = env!("CARGO_MANIFEST_DIR").to_string() + "/tests/lua/invalid/";
    let test_files: Vec<(&'static str, LuaCompilationError)> = vec![
        // "assign_const.lua.txt",
        // "goto_jump_local_scope.lua.txt",
        (
            "too_many_locals.lua.txt",
            LuaCompilationError::ReachedLocalsLimit {
                offset: 811,
                line: 2,
                col: 7,
            },
        ),
        (
            "unexpected_break.lua.txt",
            LuaCompilationError::UnexpectedBreak {
                offset: 0,
                line: 1,
                col: 1,
            },
        ),
        (
            "unexpected_end.lua.txt",
            SyntaxError::UnexpectedToken {
                label: LuaTokenLabel::End,
                offset: 0,
                line: 1,
                col: 1,
            }
            .into(),
        ),
        (
            "unexpected_name_after_return.lua.txt",
            SyntaxError::UnexpectedToken {
                label: LuaTokenLabel::Name,
                offset: 8,
                line: 2,
                col: 1,
            }
            .into(),
        ),
        (
            "unexpected_semicolon_after_return.lua.txt",
            SyntaxError::UnexpectedToken {
                label: LuaTokenLabel::SemiColon,
                offset: 7,
                line: 1,
                col: 8,
            }
            .into(),
        ),
    ];

    let compiler = LuaCompiler::default();

    for (path, expected) in test_files {
        let full_path = folder_path.clone() + path;

        let source = std::fs::read_to_string(&full_path).expect(&full_path);
        assert_eq!(
            compiler.compile(&source).err(),
            Some(expected),
            "\n{}",
            full_path
        );
    }
}

#[test]
fn runtime_error() {
    let folder_path = env!("CARGO_MANIFEST_DIR").to_string() + "/tests/lua/runtime_error/";
    let test_files: Vec<(&'static str, RuntimeErrorData)> =
        vec![("divide_by_zero.lua.txt", RuntimeErrorData::DivideByZero)];

    let mut vm = Vm::new();
    let compiler = LuaCompiler::default();

    for (path, expected) in test_files {
        let full_path = folder_path.clone() + path;

        let source = std::fs::read_to_string(&full_path).expect(&full_path);
        let module = compiler.compile(&source).unwrap();
        let function_ref = vm.load_function(path, None, module);

        assert_eq!(
            function_ref
                .call::<_, ()>(MultiValue::pack((), &mut vm).unwrap(), &mut vm)
                .err()
                .map(|err| err.data),
            Some(expected),
            "\n{}",
            full_path
        );
    }
}
