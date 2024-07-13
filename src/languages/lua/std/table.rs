use crate::errors::RuntimeError;
use crate::interpreter::{ByteString, FromValue, MultiValue, Primitive, Value};
use crate::interpreter::{TableRef, Vm};

pub fn impl_table(vm: &mut Vm) -> Result<(), RuntimeError> {
    let table = vm.create_table();

    // concat
    let concat = vm.create_native_function(|args, vm| {
        let (table, separator, start, end): (TableRef, Option<ByteString>, i64, i64) =
            args.unpack_args(vm)?;

        let mut bytes = Vec::<u8>::new();

        let separator = separator.as_ref().map(|b| b.as_bytes()).unwrap_or(&[]);

        if start <= end {
            for index in start..=end {
                let value = table.raw_get(index, vm)?;

                match value {
                    Value::String(s) => {
                        bytes.extend(s.fetch(vm)?.as_bytes());
                    }
                    Value::Primitive(Primitive::Integer(i)) => {
                        bytes.extend(i.to_string().as_bytes());
                    }
                    Value::Primitive(Primitive::Float(f)) => {
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

        MultiValue::pack((), vm)
    });
    table.raw_set("concat", concat, vm)?;

    // insert
    let insert = vm.create_native_function(|args, vm| {
        let (table, middle, last): (TableRef, Value, Value) = args.unpack_args(vm)?;

        if last.is_nil() {
            let index = table.raw_len(vm)?;
            table.raw_insert(index as i64, middle, vm)?;
        } else {
            let map_err = |err: RuntimeError| {
                // assume it's related to the middle arg
                RuntimeError::new_bad_argument(2, err)
            };

            let index = i64::from_value(middle, vm).map_err(map_err)?;
            table.raw_insert(index, last, vm).map_err(map_err)?;
        }

        MultiValue::pack((), vm)
    });
    table.raw_set("insert", insert, vm)?;

    // remove
    let remove = vm.create_native_function(|args, vm| {
        let (table, index): (TableRef, i64) = args.unpack_args(vm)?;

        let len = table.raw_len(vm)?;

        // lua allows for `#table + 1`
        if index == len as i64 + 1 {
            return MultiValue::pack((), vm);
        }

        // lua allows index to be 0 when the table len is 0
        if len == 0 && index == 0 {
            return MultiValue::pack((), vm);
        }

        table.raw_remove::<Value>(index, vm)?;

        MultiValue::pack((), vm)
    });
    table.raw_set("remove", remove, vm)?;

    // pack
    let pack = vm.create_native_function(|mut args, vm| {
        let table = vm.create_table();

        let mut index = 1;

        while let Some(value) = args.pop_front() {
            table.raw_insert(index, value, vm)?;
            index += 1;
        }

        MultiValue::pack(table, vm)
    });
    table.raw_set("pack", pack, vm)?;

    // unpack
    let unpack = vm.create_native_function(|args, vm| {
        let table: TableRef = args.unpack_args(vm)?;

        let mut multi = vm.create_multi();

        for index in (1..=table.raw_len(vm)?).rev() {
            let value = table.raw_get(index, vm)?;
            multi.push_front(value);
        }

        MultiValue::pack(multi, vm)
    });
    table.raw_set("unpack", unpack, vm)?;

    // todo: table.move() https://www.lua.org/manual/5.4/manual.html#pdf-table.move
    // todo: table.sort() https://www.lua.org/manual/5.4/manual.html#pdf-table.sort

    let env = vm.default_environment();
    env.set("table", table, vm)?;

    Ok(())
}
