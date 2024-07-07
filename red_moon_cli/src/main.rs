use clap::{command, Parser};
use red_moon::errors::{LuaCompilationError, RuntimeError, RuntimeErrorData, SyntaxError};
use red_moon::interpreter::{FunctionRef, IntoValue, MultiValue, Value, Vm};
use red_moon::languages::lua::{std as lua_std, LuaCompiler};
use rustyline::error::ReadlineError;
use std::process::ExitCode;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Options {
    script: Option<String>,
    args: Vec<String>,

    /// enter interactive mode after executing 'script'
    #[arg(short)]
    interactive: bool,

    /// execute string
    #[arg(short)]
    execute: Vec<String>,
}

fn main() -> ExitCode {
    match main2() {
        Ok(_) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

fn main2() -> Result<(), ()> {
    let options = Options::parse();

    let compiler = LuaCompiler::default();
    let mut vm = Vm::default();

    lua_std::impl_basic(&mut vm).unwrap();
    lua_std::impl_math(&mut vm).unwrap();
    lua_std::impl_table(&mut vm).unwrap();
    lua_std::impl_os(&mut vm).unwrap();
    impl_rewind(&mut vm).unwrap();

    load_args(&mut vm, &options.args);

    // default to true
    let mut interactive = true;

    if !options.execute.is_empty() {
        for source in options.execute {
            execute_source(&mut vm, &compiler, "(command line)", &source, Vec::new())?;
        }

        // only interactive if it's explicitly stated when a script is set
        interactive = options.interactive;
    }

    if let Some(path) = options.script {
        execute_file(&mut vm, &compiler, &path, options.args)?;

        // only interactive if it's explicitly stated when a script is set
        interactive = options.interactive;
    }

    if interactive {
        repl(&mut vm, &compiler)?
    }

    Ok(())
}

fn load_args(vm: &mut Vm, args: &[String]) {
    let table = vm.create_table();

    for (i, arg) in args.iter().enumerate() {
        table.set(i + 1, arg.as_str(), vm).unwrap();
    }

    vm.default_environment().set("arg", table, vm).unwrap();
}

fn execute_file(
    vm: &mut Vm,
    compiler: &LuaCompiler,
    path: &str,
    args: Vec<String>,
) -> Result<(), ()> {
    let source = match std::fs::read_to_string(path) {
        Ok(source) => source,
        Err(err) => {
            println!("cannot open {path}: {err}");
            return Err(());
        }
    };

    execute_source(vm, compiler, path, &source, args)
}

fn execute_source(
    vm: &mut Vm,
    compiler: &LuaCompiler,
    label: &str,
    source: &str,
    args: Vec<String>,
) -> Result<(), ()> {
    // compile
    let module = match compiler.compile(source) {
        Ok(module) => module,
        Err(err) => {
            println!("{label}:{err}");
            return Err(());
        }
    };

    let function_ref = vm.load_function(label, None, module).unwrap();

    // translate args
    let mut args_multi = vm.create_multi();

    for arg in args.into_iter().rev() {
        args_multi.push_front(arg.into_value(vm).unwrap());
    }

    // execute
    if let Err(err) = function_ref.call::<_, Value>(args_multi, vm) {
        println!("{err}");
        return Err(());
    }

    #[cfg(feature = "instruction_exec_counts")]
    print_instruction_exec_counts(vm);

    Ok(())
}

fn repl(vm: &mut Vm, compiler: &LuaCompiler) -> Result<(), ()> {
    let mut rl = rustyline::DefaultEditor::new().unwrap();
    let mut input_buffer = String::new();
    let mut request_more = false;

    let print_function: FunctionRef = vm.default_environment().get("print", vm).unwrap();

    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    loop {
        let prompt = if request_more { ">> " } else { "> " };
        request_more = false;

        match rl.readline(prompt) {
            Ok(s) => {
                input_buffer += &s;
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                break Ok(());
            }
            Err(err) => {
                println!("error: {:?}", err);
                break Err(());
            }
        }

        // try compiling the input as an expression first
        let as_expression = format!("return {input_buffer}");

        let module = match compiler.compile(&as_expression) {
            Ok(module) => module,
            Err(err) => {
                if matches!(
                    err,
                    LuaCompilationError::SyntaxError(SyntaxError::UnexpectedEOF)
                ) {
                    // just need more input, request more
                    request_more = true;
                    input_buffer.push('\n');
                    continue;
                }

                // compile the input directly
                match compiler.compile(&input_buffer) {
                    Ok(module) => module,
                    Err(err) => {
                        // give up and report error
                        println!("stdin:{err}");
                        input_buffer.clear();
                        continue;
                    }
                }
            }
        };

        // store the original input in the history
        let _ = rl.add_history_entry(&input_buffer);

        let function = vm.load_function("stdin", None, module).unwrap();

        match function.call::<_, MultiValue>((), vm) {
            Err(err) => println!("{err}"),
            Ok(multi) => {
                if !multi.is_empty() {
                    if let Err(err) = print_function.call::<_, Value>(multi, vm) {
                        println!("{err}")
                    }
                }
            }
        }

        #[cfg(feature = "instruction_exec_counts")]
        print_instruction_exec_counts(vm);

        input_buffer.clear();
    }
}

fn impl_rewind(vm: &mut Vm) -> Result<(), RuntimeError> {
    use std::cell::RefCell;
    use std::rc::Rc;

    let snapshots: Rc<RefCell<Vec<Vm>>> = Default::default();

    let env = vm.default_environment();

    let snapshots_capture = snapshots.clone();
    let snap = vm.create_native_function(move |_, vm| {
        let mut snapshots = snapshots_capture.borrow_mut();
        snapshots.push(vm.clone());
        MultiValue::pack((), vm)
    });
    env.set("snap", snap, vm)?;

    let rewind = vm.create_native_function(move |args, vm| {
        let x: Option<i64> = args.unpack(vm)?;
        let x = x.unwrap_or(1);

        let snapshots = snapshots.borrow();

        if x <= 0 {
            return Err(RuntimeError::from(RuntimeErrorData::OutOfBounds));
        }

        let x = x as usize;

        if snapshots.len() < x {
            return Err(RuntimeError::new_static_string(
                "not enough snapshots taken",
            ));
        }

        *vm = snapshots[snapshots.len() - x].clone();

        MultiValue::pack((), vm)
    });
    env.set("rewind", rewind, vm)?;

    Ok(())
}

#[cfg(feature = "instruction_exec_counts")]
pub(crate) fn print_instruction_exec_counts(vm: &mut Vm) {
    let results = vm.instruction_exec_counts();
    vm.clear_instruction_exec_counts();

    // collect data for formatting
    let mut label_max_len = 0;
    let mut count_max_len = 0;
    let mut total_instructions = 0;

    for (label, count) in &results {
        label_max_len = label.len().max(label_max_len);
        count_max_len = count_max_len.max(count.ilog10() + 1);
        total_instructions += count;
    }

    for (label, count) in results {
        let percent = count as f32 / total_instructions as f32 * 100.0;

        println!(
            "{percent:>6.2}% | {label:<label_max_len$} | {count:>count_max_len$}",
            label_max_len = label_max_len,
            count_max_len = count_max_len as usize
        );
    }
}
