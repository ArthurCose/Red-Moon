use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::time::Duration;

use red_moon_mlua::prelude::*;

fn collect_gc_twice(lua: &Lua) {
    lua.gc_collect().unwrap();
    lua.gc_collect().unwrap();
}

// fn snapshots(c: &mut Criterion) {
//     let mut lua = Lua::new_rollback(5);

//     c.bench_function("snapshot", |b| {
//         b.iter(|| {
//             lua.globals();
//             lua.snap()
//         });
//     });
// }

// fn rollback(c: &mut Criterion) {
//     let mut lua = Lua::new_rollback(5);
//     lua.snap();

//     c.bench_function("rollback", |b| {
//         b.iter(|| {
//             lua.globals();
//             lua.rollback(1)
//         });
//     });
// }

fn create_table(c: &mut Criterion) {
    let lua = Lua::new();

    c.bench_function("create [table empty]", |b| {
        b.iter_batched(
            || collect_gc_twice(&lua),
            |_| {
                lua.create_table().unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn create_array(c: &mut Criterion) {
    let lua = Lua::new();

    c.bench_function("create [array] 10", |b| {
        b.iter_batched(
            || collect_gc_twice(&lua),
            |_| {
                let table = lua.create_table().unwrap();
                for i in 1..=10 {
                    table.set(i, i).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn create_string_table(c: &mut Criterion) {
    let lua = Lua::new();

    c.bench_function("create [table string] 10", |b| {
        b.iter_batched(
            || collect_gc_twice(&lua),
            |_| {
                let table = lua.create_table().unwrap();
                for &s in &["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"] {
                    let s = lua.create_string(s).unwrap();
                    table.set(s.clone(), s).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn table_get_set(c: &mut Criterion) {
    let lua = Lua::new();

    let table = lua.create_table().unwrap();

    c.bench_function("table raw_get and raw_set [10]", |b| {
        b.iter_batched(
            || {
                collect_gc_twice(&lua);
                table.clear().unwrap();
            },
            |_| {
                for (i, &s) in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
                    .iter()
                    .enumerate()
                {
                    table.raw_set(s, i).unwrap();
                    assert_eq!(table.raw_get::<_, usize>(s).unwrap(), i);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn create_function(c: &mut Criterion) {
    let lua = Lua::new();

    c.bench_function("create [function] 10", |b| {
        b.iter_batched(
            || collect_gc_twice(&lua),
            |_| {
                for i in 0..10 {
                    lua.create_function(move |_, ()| Ok(i)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn call_lua_function(c: &mut Criterion) {
    let lua = Lua::new();

    c.bench_function("call Lua function [sum] 3 10", |b| {
        b.iter_batched_ref(
            || {
                collect_gc_twice(&lua);
                lua.load("function(a, b, c) return a + b + c end")
                    .eval::<LuaFunction>()
                    .unwrap()
            },
            |function| {
                for i in 0..10 {
                    let _result: i64 = function.call((i, i + 1, i + 2)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn call_sum_callback(c: &mut Criterion) {
    let lua = Lua::new();
    let callback = lua
        .create_function(|_, (a, b, c): (i64, i64, i64)| Ok(a + b + c))
        .unwrap();
    lua.globals().set("callback", callback).unwrap();

    c.bench_function("call Rust callback [sum] 3 10", |b| {
        b.iter_batched_ref(
            || {
                collect_gc_twice(&lua);
                lua.load("function() for i = 1,10 do callback(i, i+1, i+2) end end")
                    .eval::<LuaFunction>()
                    .unwrap()
            },
            |function| {
                function.call::<_, ()>(()).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn call_concat_callback(c: &mut Criterion) {
    let lua = Lua::new();
    let callback = lua
        .create_function(|_, (a, b): (LuaString, LuaString)| {
            Ok(format!("{}{}", a.to_str()?, b.to_str()?))
        })
        .unwrap();
    lua.globals().set("callback", callback).unwrap();

    c.bench_function("call Rust callback [concat string] 10", |b| {
        b.iter_batched_ref(
            || {
                collect_gc_twice(&lua);
                lua.load("function() for i = 1,10 do callback('a', tostring(i)) end end")
                    .eval::<LuaFunction>()
                    .unwrap()
            },
            |function| {
                function.call::<_, ()>(()).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn create_registry_values(c: &mut Criterion) {
    let lua = Lua::new();

    c.bench_function("create [registry value] 10", |b| {
        b.iter_batched(
            || collect_gc_twice(&lua),
            |_| {
                for _ in 0..10 {
                    lua.create_registry_value(lua.pack(true).unwrap()).unwrap();
                }
                lua.expire_registry_values();
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(300)
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.02);
    targets =
        create_table,
        create_array,
        create_string_table,
        table_get_set,
        create_function,
        call_lua_function,
        call_sum_callback,
        call_concat_callback,
        create_registry_values,
        // snapshots,
        // rollback
}

criterion_main!(benches);
