use crate::errors::RuntimeError;
use crate::interpreter::VmContext;
use crate::values::{ByteString, FromValue, FunctionRef, TableRef, Value};

pub fn load_table(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // concat
    let concat = ctx.create_function(|call_ctx, ctx| {
        let (table, separator, start, end): (
            TableRef,
            Option<ByteString>,
            Option<i64>,
            Option<i64>,
        ) = call_ctx.get_args(ctx)?;

        let mut bytes = Vec::<u8>::new();

        let separator = separator.as_ref().map(|b| b.as_bytes()).unwrap_or(&[]);

        let start = start.unwrap_or(1);
        let end = if let Some(end) = end {
            end
        } else {
            table.raw_len(ctx)? as _
        };

        if start > end {
            let string = ctx.intern_string(&bytes);
            return call_ctx.return_values(string, ctx);
        }

        for index in start..=end {
            let value = table.raw_get(index, ctx)?;

            match value {
                Value::String(s) => {
                    bytes.extend(s.fetch(ctx)?.as_bytes());
                }
                _ if let Some(n) = value.as_number() => {
                    bytes.extend(n.to_string().as_bytes());
                }
                _ => {
                    return Err(RuntimeError::new_string(format!(
                        "invalid value ({:?}) at index 2 in table for `concat`",
                        value
                    )));
                }
            }

            if index < end {
                bytes.extend(separator);
            }
        }

        let string = ctx.intern_string(&bytes);
        call_ctx.return_values(string, ctx)
    });
    let rehydrating = concat.rehydrate("table.concat", ctx)?;

    // insert
    let insert = ctx.create_function(|call_ctx, ctx| {
        let (table, middle, last): (TableRef, Value, Value) = call_ctx.get_args(ctx)?;

        if call_ctx.arg_count() <= 2 {
            table.raw_push(middle, ctx)?;
        } else {
            let map_err = |err: RuntimeError| {
                // assume it's related to the middle arg
                RuntimeError::new_bad_argument(2, err)
            };

            let index = i64::from_value(middle, ctx).map_err(map_err)?;
            table.raw_insert(index, last, ctx).map_err(map_err)?;
        }

        Ok(())
    });
    insert.rehydrate("table.insert", ctx)?;

    // remove
    let remove = ctx.create_function(|call_ctx, ctx| {
        let (table, index): (TableRef, Option<i64>) = call_ctx.get_args(ctx)?;

        let len = table.raw_len(ctx)?;
        let index = index.unwrap_or(len as _);

        // lua allows for `#table + 1`
        if index == len as i64 + 1 {
            return call_ctx.return_values(Value::Nil, ctx);
        }

        // lua allows index to be 0 when the table len is 0
        if len == 0 && index == 0 {
            return call_ctx.return_values(Value::Nil, ctx);
        }

        let value = table.raw_remove::<Value>(index, ctx)?;

        call_ctx.return_values(value, ctx)
    });
    remove.rehydrate("table.remove", ctx)?;

    // pack
    let pack = ctx.create_function(|call_ctx, ctx| {
        let table = ctx.create_table();

        for i in 0..call_ctx.arg_count() {
            let value: Value = call_ctx.get_arg(i, ctx)?;
            table.raw_insert((i + 1) as _, value, ctx)?;
        }

        call_ctx.return_values(table, ctx)
    });
    pack.rehydrate("table.pack", ctx)?;

    // unpack
    let unpack = ctx.create_function(|call_ctx, ctx| {
        let table: TableRef = call_ctx.get_args(ctx)?;

        for index in 1..=table.raw_len(ctx)? {
            let value: Value = table.raw_get(index, ctx)?;
            call_ctx.return_values(value, ctx)?;
        }

        Ok(())
    });
    unpack.rehydrate("table.unpack", ctx)?;

    // move
    let table_move = ctx.create_function(|call_ctx, ctx| {
        let (src_table, src_start, src_end, dest_start, dest_table): (
            TableRef,
            i64,
            i64,
            i64,
            Option<TableRef>,
        ) = call_ctx.get_args(ctx)?;

        let src_start = usize::try_from(src_start).unwrap_or(0);
        let src_end = usize::try_from(src_end).unwrap_or(0);
        let dest_table = dest_table.unwrap_or_else(|| src_table.clone());

        if src_start > src_end {
            return Ok(());
        }

        let len = src_end.saturating_sub(1) - src_start.saturating_sub(1) + 1;
        let dest_start = usize::try_from(dest_start).unwrap_or(0);

        dest_table.copy_from(src_start, dest_start, len, &src_table, ctx)?;

        call_ctx.return_values(dest_table, ctx)
    });
    table_move.rehydrate("table.move", ctx)?;

    // sort
    let sort = ctx.create_function(|call_ctx, ctx| {
        let (table, less_than_fn): (TableRef, Option<FunctionRef>) = call_ctx.get_args(ctx)?;

        if let Some(less_than_fn) = less_than_fn {
            table.sort_unstable_by(ctx, |a, b, ctx| {
                less_than_fn.call((a.clone(), b.clone()), ctx)
            })?;
        } else {
            table.sort_unstable_by(ctx, |a, b, ctx| a.is_less_than(b, ctx))?;
        }

        Ok(())
    });
    sort.rehydrate("table.sort", ctx)?;

    if !rehydrating {
        let table = ctx.create_table();
        table.raw_set("concat", concat, ctx)?;
        table.raw_set("insert", insert, ctx)?;
        table.raw_set("remove", remove, ctx)?;
        table.raw_set("pack", pack, ctx)?;
        table.raw_set("unpack", unpack, ctx)?;
        table.raw_set("move", table_move, ctx)?;
        table.raw_set("sort", sort, ctx)?;

        let env = ctx.default_environment();
        env.set("table", table, ctx)?;
    }

    Ok(())
}
