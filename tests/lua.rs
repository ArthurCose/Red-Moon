use pretty_assertions::assert_eq;
use red_moon::errors::{LuaCompilationErrorData, RuntimeErrorData, SyntaxErrorData};
use red_moon::interpreter::Vm;
use red_moon::languages::lua::std::*;
use red_moon::languages::lua::{LuaTokenLabel, compile};
use red_moon::tag_native_type;
use red_moon::values::{MultiValue, Value};
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct PrintCapture {
    #[cfg_attr(feature = "serde", serde(skip))]
    out: Rc<RefCell<Vec<u8>>>,
}

tag_native_type!(PrintCapture);

#[test]
fn valid() {
    let folder_path = env!("CARGO_MANIFEST_DIR").to_string() + "/tests/lua/valid/";
    let test_files = vec![
        "coroutines.lua",
        "debug.lua",
        "expressions.lua",
        "functions.lua",
        "garbage_collection.lua",
        "goto.lua",
        "loops.lua",
        "metatables.lua",
        "munchausen_numbers.lua",
        "semicolons.lua",
        "std_basic.lua",
        "std_string.lua",
        "std_table.lua",
        "tables.lua.txt",
        "variables.lua.txt",
    ];

    let mut vm = Vm::default();
    let ctx = &mut vm.context();
    load_basic(ctx).unwrap();
    load_string(ctx).unwrap();
    load_coroutine(ctx).unwrap();
    load_table(ctx).unwrap();
    load_debug(ctx).unwrap();

    let env = ctx.default_environment();

    // override print
    let out_capture = PrintCapture::default();

    let print_ref = ctx
        .create_function(|call_ctx, ctx| {
            let len = call_ctx.arg_count();

            let out_capture: &PrintCapture = call_ctx.get_capture(ctx).unwrap();
            let out_capture = out_capture.out.clone();
            let mut out = out_capture.borrow_mut();

            for i in 0..len {
                match call_ctx.get_arg(i, ctx)? {
                    Value::Nil => write!(&mut *out, "nil").unwrap(),
                    Value::Bool(b) => write!(&mut *out, "{b}").unwrap(),
                    Value::Integer(n) => write!(&mut *out, "{n}").unwrap(),
                    Value::Float(n) => write!(&mut *out, "{n:?}").unwrap(),
                    Value::Table(_) => write!(&mut *out, "table").unwrap(),
                    Value::Function(_) => write!(&mut *out, "function").unwrap(),
                    Value::Coroutine(_) => write!(&mut *out, "thread").unwrap(),
                    Value::String(string_ref) => write!(
                        &mut *out,
                        "{}",
                        string_ref.fetch(ctx).unwrap().to_string_lossy()
                    )
                    .unwrap(),
                }

                if i < len - 1 {
                    write!(&mut *out, "\t").unwrap();
                }
            }

            writeln!(&mut *out).unwrap();

            Ok(())
        })
        .create_closure(out_capture.clone(), ctx)
        .unwrap();

    env.raw_set("print", print_ref, ctx).unwrap();

    // the actual tests
    for path in test_files {
        println!("testing {path}");

        let full_path = folder_path.clone() + path;

        let source = std::fs::read_to_string(&full_path).expect(&full_path);
        let module = compile(&source).expect(path);
        let function_ref = ctx.load_function(path, None, module).unwrap();

        if let Err(err) = function_ref.call::<_, ()>((), ctx) {
            panic!(
                "{path}: {err}\n\n{}",
                String::from_utf8_lossy(&out_capture.out.borrow())
            );
        }

        let mut out = out_capture.out.borrow_mut();
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
    let test_files: Vec<(&'static str, LuaCompilationErrorData)> = vec![
        // "assign_const.lua.txt",
        (
            "break_outside_loop.lua.txt",
            LuaCompilationErrorData::UnexpectedBreak,
        ),
        (
            "goto_jump_inner_scope.lua.txt",
            LuaCompilationErrorData::UnresolvedGoto,
        ),
        (
            "goto_jump_local_scope.lua.txt",
            LuaCompilationErrorData::GotoSkipsLocalDeclaration,
        ),
        (
            "label_redefined_in_new_scope.lua.txt",
            LuaCompilationErrorData::RedefinedLabel,
        ),
        (
            "label_redefined.lua.txt",
            LuaCompilationErrorData::RedefinedLabel,
        ),
        (
            "too_many_locals.lua.txt",
            LuaCompilationErrorData::ReachedLocalsLimit,
        ),
        (
            "unexpected_break.lua.txt",
            LuaCompilationErrorData::UnexpectedBreak,
        ),
        (
            "unexpected_end.lua.txt",
            SyntaxErrorData::UnexpectedToken {
                label: LuaTokenLabel::End,
            }
            .into(),
        ),
        (
            "unexpected_name_after_return.lua.txt",
            SyntaxErrorData::UnexpectedToken {
                label: LuaTokenLabel::Name,
            }
            .into(),
        ),
        (
            "unexpected_semicolon_after_return.lua.txt",
            SyntaxErrorData::UnexpectedToken {
                label: LuaTokenLabel::SemiColon,
            }
            .into(),
        ),
    ];

    for (path, expected) in test_files {
        let full_path = folder_path.clone() + path;

        let source = std::fs::read_to_string(&full_path).expect(&full_path);
        assert_eq!(
            compile(&source).err().map(|err| err.data),
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
    let ctx = &mut vm.context();

    for (path, expected) in test_files {
        let full_path = folder_path.clone() + path;

        let source = std::fs::read_to_string(&full_path).expect(&full_path);
        let module = compile(&source).unwrap();
        let function_ref = ctx.load_function(path, None, module).unwrap();

        assert_eq!(
            function_ref
                .call::<_, ()>(MultiValue::pack((), ctx).unwrap(), ctx)
                .err()
                .map(|err| err.data),
            Some(expected),
            "\n{}",
            full_path
        );
    }
}
