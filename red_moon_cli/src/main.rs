use clap::Parser;
use red_moon::errors::{LuaCompilationErrorData, RuntimeError, RuntimeErrorData, SyntaxErrorData};
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

    /// Enter interactive mode after executing 'script'
    #[arg(short)]
    interactive: bool,

    /// Execute string
    #[arg(short)]
    execute: Vec<String>,

    /// Print instruction enums
    #[arg(long)]
    enum_code: bool,
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
    lua_std::impl_coroutine(ctx).unwrap();
    lua_std::impl_math(ctx).unwrap();
    lua_std::impl_os(ctx).unwrap();
    lua_std::impl_debug(ctx).unwrap();
    lua_std::impl_string(ctx).unwrap();
    lua_std::impl_table(ctx).unwrap();

    load_args(ctx, &options.args);

    // default to true
    let mut interactive = true;

    if !options.execute.is_empty() {
        for source in options.execute {
            execute_source(
                ctx,
                &compiler,
                "(command line)",
                &source,
                options.enum_code,
                Vec::new(),
            )?;
        }

        // only interactive if it's explicitly stated when a script is set
        interactive = options.interactive;
    }

    if let Some(path) = options.script {
        execute_file(ctx, &compiler, &path, options.enum_code, options.args)?;

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
    print_enum_code: bool,
    args: Vec<String>,
) -> Result<(), ()> {
    let source = match std::fs::read_to_string(path) {
        Ok(source) => source,
        Err(err) => {
            println!("cannot open {path}: {err}");
            return Err(());
        }
    };

    execute_source(ctx, compiler, path, &source, print_enum_code, args)
}

fn execute_source(
    ctx: &mut VmContext,
    compiler: &LuaCompiler,
    label: &str,
    source: &str,
    print_enum_code: bool,
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

    if print_enum_code {
        for (i, chunk) in module.chunks.iter().enumerate() {
            if module.main == i {
                println!("Chunk {i} (main):");
            } else {
                println!("Chunk {i}:");
            }
            println!("{}\n", chunk.to_readable_instructions(label));
        }
    }

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

    #[cfg(feature = "instruction_metrics")]
    print_instruction_metrics(ctx);

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
                    err.data,
                    LuaCompilationErrorData::SyntaxError(SyntaxErrorData::UnexpectedEOF)
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

        #[cfg(feature = "instruction_metrics")]
        print_instruction_metrics(ctx);

        input_buffer.clear();
    }
}

type QueuedRewind = Rc<Cell<Option<Vm>>>;

fn impl_rewind(ctx: &mut VmContext) -> Result<QueuedRewind, RuntimeError> {
    let snapshots: Rc<RefCell<Vec<Vm>>> = Default::default();
    let queued_rewind = QueuedRewind::default();

    let env = ctx.default_environment();

    let snapshots_capture = snapshots.clone();
    let snap = ctx.create_function(move |_, ctx| {
        let mut snapshots = snapshots_capture.borrow_mut();
        snapshots.push(ctx.clone_vm());
        Ok(())
    });
    env.set("snap", snap, ctx)?;

    let queued_rewind_capture = queued_rewind.clone();
    let rewind = ctx.create_function(move |call_ctx, ctx| {
        let x: Option<i64> = call_ctx.get_args(ctx)?;
        let x = x.unwrap_or(-1);

        let mut snapshots = snapshots.borrow_mut();

        let x = match x.cmp(&0) {
            std::cmp::Ordering::Less => return Err(RuntimeErrorData::OutOfBounds.into()),
            std::cmp::Ordering::Equal => return Ok(()),
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

        Ok(())
    });
    env.set("queue_rewind", rewind, ctx)?;

    Ok(queued_rewind)
}

#[cfg(feature = "instruction_metrics")]
pub(crate) fn print_instruction_metrics(ctx: &mut VmContext) {
    use std::time::Duration;

    use red_moon::interpreter::InstructionMetrics;

    // flush metrics
    let results = ctx.instruction_metrics();
    ctx.clear_instruction_metrics();

    let total_instructions: usize = results.iter().map(|metrics| metrics.count).sum();

    // build table
    let mut table_builder = tabled::builder::Builder::new();

    table_builder.push_record([
        "instruction",
        "min (ns)",
        "max (ns)",
        "avg (ns)",
        "total (ns)",
        "time%",
        "hits",
        "hit%",
    ]);

    let combined_metrics = results.iter().fold(
        InstructionMetrics {
            name: "(*)",
            count: total_instructions,
            min_time: if results.is_empty() {
                Duration::ZERO
            } else {
                Duration::MAX
            },
            max_time: Duration::ZERO,
            total_time: Duration::ZERO,
        },
        |mut acc, metrics| {
            acc.min_time = acc.min_time.min(metrics.min_time);
            acc.max_time = acc.max_time.max(metrics.max_time);
            acc.total_time += metrics.total_time;
            acc
        },
    );

    let combined_time = combined_metrics.total_time;

    for InstructionMetrics {
        name,
        count: hit_count,
        min_time,
        max_time,
        total_time,
    } in std::iter::once(combined_metrics).chain(results.into_iter())
    {
        let hit_percent = hit_count as f32 / total_instructions as f32 * 100.0;
        let time_percent = ((total_time.as_nanos() * 100) / combined_time.as_nanos()) as f32;

        table_builder.push_record([
            name.to_string(),
            format!("{}", min_time.as_nanos()),
            format!("{}", max_time.as_nanos()),
            format!("{}", total_time.as_nanos() / hit_count as u128),
            format!("{}", total_time.as_nanos()),
            format!("{time_percent:>.2}%"),
            hit_count.to_string(),
            format!("{hit_percent:>.2}%"),
        ]);
    }

    use tabled::settings::{
        object::{Columns, Rows},
        Alignment, Style,
    };

    let mut table = table_builder.build();
    table.modify(Columns::single(5), Alignment::right());
    table.modify(Columns::single(7), Alignment::right());
    table.modify(Rows::first(), Alignment::left());
    table.with(Style::markdown());
    println!("{table}");
}
