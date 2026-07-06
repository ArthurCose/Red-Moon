# Red Moon

A Lua VM designed for multiplayer games with player-made scripts.

## Design Goals

- Safe and Sandboxed
  - For sharing untrusted custom content between players
- Determinism and Rollback
  - A primary motivation, ensuring Lua is fully capable of rollback, by allowing the entire VM to be cloned and preventing clones from accidentally modifying each other
- Serialization
  - For allowing players to spectate or join in the middle of an active game

## Sandboxing

Parts of the standard library that are unsafe for untrusted code are kept obvious in Red Moon, such as `io` and `os`.

For unsafe functions in non-obvious parts of Lua's standard library:

`require()` is currently unimplemented. When it is implemented: there will be no support for loading C libraries (to prevent execution of untrusted files that could be on the system from other programs, as well as inherent compatibility issues).

## Determinism

`pairs` and `next` iterate based on insertion order for map keys, but may reorder when new keys are inserted.

`math.randomseed` always uses 0, 0 as a default instead of the system time.

## Rollback

Every value in the Red Moon VM must implement Clone, as cloning the VM is used to take a snapshot / save state of the VM.

```rust
use red_moon::interpreter::Vm;

let mut vm = Vm::default();
let ctx = &mut vm.context();

let env = ctx.default_environment();
env.set("a", 1, ctx).unwrap();

// copy a version where a = 1
let snapshot = vm.clone();

let ctx = &mut vm.context();
env.set("a", 2, ctx).unwrap();

// 2 should be stored in a
let a: i32 = env.get("a", ctx).unwrap();
assert_eq!(a, 2);

// rewinding
vm.clone_from(&snapshot);

// 1 should be stored in `a`
let ctx = &mut vm.context();
let a: i32 = env.get("a", ctx).unwrap();
assert_eq!(a, 1);
```

### Closures

Rust closures are prevented by default as implicit captures can not be seen by the VM for serialization.

Function references created from `ctx.create_function()` can use the `create_closure` method to create an explicit closure,
which allows the VM to serialize and enforce serialization on captures.

```rust
use red_moon::interpreter::Vm;
use red_moon::values::tag_native_type;

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Counter(i64);

// if serde isn't enabled it's not necessary to include this,
// but it doesn't hurt to leave in
tag_native_type!(Counter);

let mut vm = Vm::default();
let ctx = &mut vm.context();

let f = ctx.create_function(|call_ctx, ctx| {
  // data is stored in the Vm, we're free to modify it without serialization issues
  let capture = call_ctx.get_capture_mut::<Counter>(ctx).unwrap();
  capture.0 += 1;

  call_ctx.return_values(capture.0, ctx)
}).create_closure(Counter(1), ctx); // explicit capture
```

Values entering the VM must support Clone (for rollback), and should avoid shared interior mutability, as modifying shared data will affect the state of snapshots.

Note: this is only an issue for interior mutability with shared ownership, such as Rc. Interior mutability with direct ownership is acceptable as long as serialization is not a requirement.

If serialization is not necessary, the `implicit_closures` feature can be enabled to loosen `fn` parameters to `impl Fn`.

```rust
use red_moon::interpreter::Vm;
use std::cell::Cell;
use std::rc::Rc;

// interior mutability is fine as long as serialization isn't necessary and Rc is not involved:
let counter = Cell::new(1);
// Rc is fine as long as the data is immutable and serialization isn't needed:
let data = Rc::new(1);
// Rc with interior mutability will cause issues during rollback:
let rc_counter = Rc::new(Cell::new(1));

let mut vm = Vm::default();
let ctx = &mut vm.context();

// these can be captured, but not all are bug free:
#[cfg(feature = "implicit_closures")]
let f = ctx.create_function(move |call_ctx, ctx| {
  let count = counter.get();
  counter.set(count + 1);

  call_ctx.return_values(rc_counter.get() + *data + count, ctx)
});
```

## Serialization

Enabling the `serde` feature adds serialization support through [serde](https://crates.io/crates/serde).

Data stored directly within the VM is serialized without issue, but special attention is necessary for external data and Rust functions. Dynamically creating Rust functions should be avoided to allow for "rehydration", also be mindful of implicit captures as serialization of captures outside of explicit API methods is not possible.

As for external data: Refs, such as `FunctionRef`, should not be serialized from outside the VM. Any ref deserialized outside of the VM can be garbage collected while still live.

To properly serialize a Ref, it can be stored with `ctx.set_singleton()`, `function_ref.create_closure()`, or similar.

```rust
#![cfg(feature = "serde")]

use red_moon::interpreter::{Vm, VmContext};
use red_moon::values::{FunctionRef, Value};
use red_moon::errors::RuntimeError;

fn load_foo(ctx: &mut VmContext) -> Result<bool, RuntimeError> {
  // Value implements Clone + Serialize + Deserialize + NativeValue
  let count = Value::Integer(0);

  let f = ctx.create_function(|call_ctx, ctx| {
    let Some(Value::Integer(count)) = call_ctx.get_capture_mut::<Value>(ctx) else {
      panic!("Failed to deserialize");
    };

    *count += 1;

    call_ctx.return_values(*count, ctx)
  })
    .create_closure(count, ctx)?; // explicit capture, serializes with the VM

  // rehydrate our function using a tag
  // on the first run this will just tag our function
  // on the second run it will overwrite existing functions
  // with this function's implementation and return true
  let rehydrating = f.rehydrate("my_function", ctx)?;

  if !rehydrating {
    // if we're rehydrating we don't want to set values,
    // since the previous execution may have written over our values
    // and we want to preserve the existing state

    let env = ctx.default_environment();
    env.set("foo", f, ctx)?;
  }

  Ok(rehydrating)
}

let mut vm = Vm::default();
let ctx = &mut vm.context();

// implement some API
load_foo(ctx);

// test out foo
let env = ctx.default_environment();
let foo: Value = env.get("foo", ctx).unwrap();
let result: i64 = foo.call((), ctx).unwrap();
assert_eq!(result, 1);

// copy a reference of foo to bar
env.set("bar", foo, ctx).unwrap();

// serialize for the network
let serialized_vm = bincode::serialize(&vm).unwrap();

// ... a network between us ...

// a new VM deserialized from the previous one
let mut vm: Vm = bincode::deserialize(&serialized_vm).unwrap();
let ctx = &mut vm.context();

// rehydrate
assert!(load_foo(ctx).unwrap());

// testing if bar was updated
let env = ctx.default_environment();
let bar: FunctionRef = env.get("bar", ctx).unwrap();
let result: i64 = bar.call((), ctx).unwrap();
assert_eq!(result, 2);
```

## Lua Support

Aiming for Lua 5.4, currently missing support for `<const>` and `<close>`.

The garbage collector is incremental only.

### Standard Library

| Library     | Progress | Notes                                                                                                                                                                                           |
| ----------- | -------: | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `basic`     |  23 / 26 | Missing `dofile`, `require`, `warn` and level parameter for `error`. `load` and `loadfile` can't read binary chunks.                                                                            |
| `coroutine` |    7 / 8 | Missing `coroutine.close`.                                                                                                                                                                      |
| `debug`     |   6 / 16 | Only `debug.getregistry`, `debug.getmetatable`, `debug.setmetatable`, `debug.traceback`, `debug.gethook` and count support for `debug.sethook`. Threads are not accepted from this library yet. |
| `math`      |  27 / 27 | A time based seed is not supplied to `math.randomseed` for determinism by default.                                                                                                              |
| `os`        |   1 / 11 | Only `os.clock`                                                                                                                                                                                 |
| `package`   |    0 / 8 |                                                                                                                                                                                                 |
| `string`    |  12 / 17 | Missing `string.dump`, `string.format`, `string.pack`, `string.unpack` and `string.packsize`                                                                                                    |
| `table`     |    7 / 7 |                                                                                                                                                                                                 |
| `utf8`      |    0 / 6 |                                                                                                                                                                                                 |
| `io`        |   0 / 21 |                                                                                                                                                                                                 |

### Metamethods

| Method        | Supported  |
| ------------- | ---------- |
| `__unm`       | ✅ Yes     |
| `__bnot`      | ✅ Yes     |
| `__add`       | ✅ Yes     |
| `__sub`       | ✅ Yes     |
| `__mul`       | ✅ Yes     |
| `__div`       | ✅ Yes     |
| `__idiv`      | ✅ Yes     |
| `__mod`       | ✅ Yes     |
| `__pow`       | ✅ Yes     |
| `__band`      | ✅ Yes     |
| `__bor`       | ✅ Yes     |
| `__bxor`      | ✅ Yes     |
| `__shl`       | ✅ Yes     |
| `__shr`       | ✅ Yes     |
| `__eq`        | ✅ Yes     |
| `__lt`        | ✅ Yes     |
| `__le`        | ✅ Yes     |
| `__concat`    | ✅ Yes     |
| `__len`       | ✅ Yes     |
| `__index`     | ✅ Yes     |
| `__newindex`  | ✅ Yes     |
| `__call`      | ✅ Yes     |
| `__mode`      | ✅ Yes     |
| `__close`     | ⛔ Not yet |
| `__gc`        | ⛔ Not yet |
| `__metatable` | ✅ Yes     |
| `__name`      | ✅ Yes     |
| `__tostring`  | ✅ Yes     |
| `__pairs`     | ✅ Yes     |
| `__ipairs`    | ✅ Yes     |
