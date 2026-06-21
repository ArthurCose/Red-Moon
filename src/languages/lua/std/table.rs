use crate::errors::RuntimeError;
use crate::interpreter::VmContext;
use crate::values::{ByteString, FromValue, TableRef, Value};

pub fn load_table(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // concat
    let concat = ctx.create_function(|call_ctx, ctx| {
        let (table, separator, start, end): (TableRef, Option<ByteString>, i64, i64) =
            call_ctx.get_args(ctx)?;

        let mut bytes = Vec::<u8>::new();

        let separator = separator.as_ref().map(|b| b.as_bytes()).unwrap_or(&[]);

        if start <= end {
            for index in start..=end {
                let value = table.raw_get(index, ctx)?;

                match value {
                    Value::String(s) => {
                        bytes.extend(s.fetch(ctx)?.as_bytes());
                    }
                    Value::Integer(i) => {
                        bytes.extend(i.to_string().as_bytes());
                    }
                    Value::Float(f) => {
                        // todo: use lua's formatting
                        bytes.extend(f.to_string().as_bytes());
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
        }

        Ok(())
    });
    let rehydrating = concat.rehydrate("table.concat", ctx)?;

    // insert
    let insert = ctx.create_function(|call_ctx, ctx| {
        let (table, middle, last): (TableRef, Value, Value) = call_ctx.get_args(ctx)?;

        if last.is_nil() {
            let index = table.raw_len(ctx)?;
            table.raw_insert(index as i64, middle, ctx)?;
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
        let (table, index): (TableRef, i64) = call_ctx.get_args(ctx)?;

        let len = table.raw_len(ctx)?;

        // lua allows for `#table + 1`
        if index == len as i64 + 1 {
            return Ok(());
        }

        // lua allows index to be 0 when the table len is 0
        if len == 0 && index == 0 {
            return Ok(());
        }

        table.raw_remove::<Value>(index, ctx)?;

        Ok(())
    });
    remove.rehydrate("table.remove", ctx)?;

    // pack
    let pack = ctx.create_function(|call_ctx, ctx| {
        let table = ctx.create_table();

        for i in 0..call_ctx.arg_count() {
            let value: Value = call_ctx.get_arg(i, ctx)?;
            table.raw_insert((i + 1) as _, value, ctx)?;
        }

        Ok(())
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

    // todo: table.move() https://www.lua.org/manual/5.4/manual.html#pdf-table.move
    // todo: table.sort() https://www.lua.org/manual/5.4/manual.html#pdf-table.sort

    if !rehydrating {
        let table = ctx.create_table();
        table.raw_set("concat", concat, ctx)?;
        table.raw_set("insert", insert, ctx)?;
        table.raw_set("remove", remove, ctx)?;
        table.raw_set("pack", pack, ctx)?;
        table.raw_set("unpack", unpack, ctx)?;

        let env = ctx.default_environment();
        env.set("table", table, ctx)?;
    }

    Ok(())
}
