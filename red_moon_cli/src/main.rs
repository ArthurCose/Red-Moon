use clap::{command, Parser};
use red_moon::errors::{LuaCompilationError, RuntimeError, RuntimeErrorData, SyntaxError};
use red_moon::interpreter::{FunctionRef, IntoValue, MultiValue, Value, Vm, VmContext};
use red_moon::languages::lua::{std as lua_std, LuaCompiler};
use rustyline::error::ReadlineError;
use std::cell::{Cell, RefCell};
use std::process::ExitCode;
use std::rc::Rc;

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
    let ctx = &mut vm.context();

    lua_std::impl_basic(ctx).unwrap();
    lua_std::impl_math(ctx).unwrap();
    lua_std::impl_os(ctx).unwrap();
    lua_std::impl_string(ctx).unwrap();
    lua_std::impl_table(ctx).unwrap();

    load_args(ctx, &options.args);

    // default to true
    let mut interactive = true;

    if !options.execute.is_empty() {
        for source in options.execute {
            execute_source(ctx, &compiler, "(command line)", &source, Vec::new())?;
        }

        // only interactive if it's explicitly stated when a script is set
        interactive = options.interactive;
    }

    if let Some(path) = options.script {
        execute_file(ctx, &compiler, &path, options.args)?;

        // only interactive if it's explicitly stated when a script is set
        interactive = options.interactive;
    }

    if interactive {
        repl(&mut vm, &compiler)?
    }

    Ok(())
}

fn load_args(ctx: &mut VmContext, args: &[String]) {
    let table = ctx.create_table();

    for (i, arg) in args.iter().enumerate() {
        table.set(i + 1, arg.as_str(), ctx).unwrap();
    }

    ctx.default_environment().set("arg", table, ctx).unwrap();
}

fn execute_file(
    ctx: &mut VmContext,
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

    execute_source(ctx, compiler, path, &source, args)
}

fn execute_source(
    ctx: &mut VmContext,
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

    let function_ref = ctx.load_function(label, None, module).unwrap();

    // translate args
    let mut args_multi = ctx.create_multi();

    for arg in args.into_iter().rev() {
        args_multi.push_front(arg.into_value(ctx).unwrap());
    }

    // execute
    if let Err(err) = function_ref.call::<_, Value>(args_multi, ctx) {
        println!("{err}");
        return Err(());
    }

    #[cfg(feature = "instruction_exec_counts")]
    print_instruction_exec_counts(ctx);

    Ok(())
}

fn repl(vm: &mut Vm, compiler: &LuaCompiler) -> Result<(), ()> {
    let mut rl = rustyline::DefaultEditor::new().unwrap();
    let mut input_buffer = String::new();
    let mut request_more = false;

    let ctx = &mut vm.context();
    let print_function: FunctionRef = ctx.default_environment().get("print", ctx).unwrap();

    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    let queued_rewind = impl_rewind(ctx).unwrap();

    loop {
        if let Some(snapshot) = queued_rewind.take() {
            *vm = snapshot;
        }

        let ctx = &mut vm.context();

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

        let function = ctx.load_function("stdin", None, module).unwrap();

        match function.call::<_, MultiValue>((), ctx) {
            Err(err) => println!("{err}"),
            Ok(multi) => {
                if !multi.is_empty() {
                    if let Err(err) = print_function.call::<_, Value>(multi, ctx) {
                        println!("{err}")
                    }
                }
            }
        }

        #[cfg(feature = "instruction_exec_counts")]
        print_instruction_exec_counts(ctx);

        input_buffer.clear();
    }
}

type QueuedRewind = Rc<Cell<Option<Vm>>>;

fn impl_rewind(ctx: &mut VmContext) -> Result<QueuedRewind, RuntimeError> {
    let snapshots: Rc<RefCell<Vec<Vm>>> = Default::default();
    let queued_rewind = QueuedRewind::default();

    let env = ctx.default_environment();

    let snapshots_capture = snapshots.clone();
    let snap = ctx.create_native_function(move |_, ctx| {
        let mut snapshots = snapshots_capture.borrow_mut();
        snapshots.push(ctx.clone_vm());
        MultiValue::pack((), ctx)
    });
    env.set("snap", snap, ctx)?;

    let queued_rewind_capture = queued_rewind.clone();
    let rewind = ctx.create_native_function(move |args, ctx| {
        let x: Option<i64> = args.unpack(ctx)?;
        let x = x.unwrap_or(-1);

        let mut snapshots = snapshots.borrow_mut();

        let x = match x.cmp(&0) {
            std::cmp::Ordering::Less => return Err(RuntimeErrorData::OutOfBounds.into()),
            std::cmp::Ordering::Equal => return MultiValue::pack((), ctx),
            std::cmp::Ordering::Greater => {
                let x = x as usize;

                if x > snapshots.len() {
                    return Err(RuntimeError::new_static_string(
                        "not enough snapshots taken",
                    ));
                }

                snapshots.len() - x
            }
        };

        snapshots.truncate(x + 1);
        queued_rewind_capture.set(snapshots.pop());

        MultiValue::pack((), ctx)
    });
    env.set("queue_rewind", rewind, ctx)?;

    Ok(queued_rewind)
}

#[cfg(feature = "instruction_exec_counts")]
pub(crate) fn print_instruction_exec_counts(ctx: &mut VmContext) {
    let results = ctx.instruction_exec_counts();
    ctx.clear_instruction_exec_counts();

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
