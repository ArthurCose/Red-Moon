use crate::errors::RuntimeError;
use crate::interpreter::{ByteString, FromValue, MultiValue, TableRef, Value, VmContext};

pub fn impl_table(ctx: &mut VmContext) -> Result<(), RuntimeError> {
    // concat
    let concat = ctx.create_function(|args, ctx| {
        let (table, separator, start, end): (TableRef, Option<ByteString>, i64, i64) =
            args.unpack_args(ctx)?;

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
                        )))
                    }
                }

                if index < end {
                    bytes.extend(separator);
                }
            }
        }

        MultiValue::pack((), ctx)
    });
    let hydrating = concat.hydrate("table.concat", ctx)?;

    // insert
    let insert = ctx.create_function(|args, ctx| {
        let (table, middle, last): (TableRef, Value, Value) = args.unpack_args(ctx)?;

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

        MultiValue::pack((), ctx)
    });
    insert.hydrate("table.insert", ctx)?;

    // remove
    let remove = ctx.create_function(|args, ctx| {
        let (table, index): (TableRef, i64) = args.unpack_args(ctx)?;

        let len = table.raw_len(ctx)?;

        // lua allows for `#table + 1`
        if index == len as i64 + 1 {
            return MultiValue::pack((), ctx);
        }

        // lua allows index to be 0 when the table len is 0
        if len == 0 && index == 0 {
            return MultiValue::pack((), ctx);
        }

        table.raw_remove::<Value>(index, ctx)?;

        MultiValue::pack((), ctx)
    });
    remove.hydrate("table.remove", ctx)?;

    // pack
    let pack = ctx.create_function(|mut args, ctx| {
        let table = ctx.create_table();

        let mut index = 1;

        while let Some(value) = args.pop_front() {
            table.raw_insert(index, value, ctx)?;
            index += 1;
        }

        MultiValue::pack(table, ctx)
    });
    pack.hydrate("table.pack", ctx)?;

    // unpack
    let unpack = ctx.create_function(|args, ctx| {
        let table: TableRef = args.unpack_args(ctx)?;

        let mut multi = ctx.create_multi();

        for index in (1..=table.raw_len(ctx)?).rev() {
            let value = table.raw_get(index, ctx)?;
            multi.push_front(value);
        }

        MultiValue::pack(multi, ctx)
    });
    unpack.hydrate("table.unpack", ctx)?;

    // todo: table.move() https://www.lua.org/manual/5.4/manual.html#pdf-table.move
    // todo: table.sort() https://www.lua.org/manual/5.4/manual.html#pdf-table.sort

    if !hydrating {
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
