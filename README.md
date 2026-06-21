# Red Moon Lua VM

A Lua VM designed for multiplayer games with player-made scripts.

## Design Goals

- Safe and Sandboxed
  - For sharing untrusted custom content between players
- Rollback
  - A primary motivation, ensuring Lua fully works with rollback, by allowing the entire VM to be cloned and preventing clones from accidentally modifying each other
- Serialization
  - For allowing players to spectate or join in the middle of an active game

## Safety

Parts of the standard library that are unsafe for untrusted code are kept obvious in Red Moon, such as `io` and `os`.

For unsafe functions in non-obvious parts of Lua's standard library:

`require()` is currently unimplemented. When it is implemented: there will be no support for loading C libraries (to prevent execution of untrusted files that could be on the system from other programs, as well as inherent compatibility issues).

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

There are parts of Red Moon that won't work with rollback such as the `os` module.

### Functions

Values entering the VM must support Clone, and should avoid interior mutability, as modifying shared data will affect the state of snapshots, and modified captures can not be serialized.

Note: this is only an issue for Rc, interior mutability with direct ownership is acceptable.

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

// the "implicit_closures" feature isn't enabled by default as data captured by a Rust closure can't be serialized
use red_moon::values::tag_native_type;

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Counter(i64);

// if serde isn't enabled it's not necessary to include this,
// but it doesn't hurt to leave in
tag_native_type!(Counter);

let f = ctx.create_function(|call_ctx, ctx| {
  // data is stored in the Vm, we're free to modify it without serialization issues
  let capture = call_ctx.read_capture_mut::<Counter>(ctx).unwrap();
  capture.0 += 1;

  call_ctx.return_values(capture.0, ctx)
}).create_closure(Counter(1), ctx);
```

## Serialization

Enabling the `serde` feature adds serialization support through [serde](https://crates.io/crates/serde).

Lua values and functions are serialized without issue, but special attention is necessary for app data and Rust functions. Dynamically creating Rust functions should be avoided to allow for "rehydration", also be mindful of Rust captures as serialization of implicit captures is not possible.

Rust functions can be tagged and reimplemented through the "rehydrate" function:

```rust
#![cfg(feature = "serde")]

use red_moon::interpreter::{Vm, VmContext};
use red_moon::values::{FunctionRef, Value};
use red_moon::errors::RuntimeError;

fn implement_foo(ctx: &mut VmContext) -> Result<bool, RuntimeError> {
  let f = ctx.create_function(|call_ctx, ctx| call_ctx.return_values("hello", ctx));

  // rehydrate our function using a tag
  // on the first run this will just tag our function
  // on the second run it will overwrite existing functions
  // with this function's implementation and return true
  let rehydrating = f.rehydrate("my_function", ctx)?;

  if !rehydrating {
    // if we're rehydrating we don't want to set values,
    // since a script may have written over our values
    // and we want to keep in sync

    let env = ctx.default_environment();
    env.set("foo", f, ctx)?;
  }

  Ok(rehydrating)
}

let mut vm = Vm::default();
let ctx = &mut vm.context();

// implement some API
implement_foo(ctx);

// copy a reference of foo to bar
let env = ctx.default_environment();
let foo: Value = env.get("foo", ctx).unwrap();
env.set("bar", foo, ctx).unwrap();

// serialize for the network
let serialized_vm = bincode::serialize(&vm).unwrap();

// ... a network between us ...

// a new VM deserialized from the previous one
let mut vm: Vm = bincode::deserialize(&serialized_vm).unwrap();
let ctx = &mut vm.context();

// rehydrate
assert!(implement_foo(ctx).unwrap());

// testing if bar was updated
let env = ctx.default_environment();
let bar: FunctionRef = env.get("bar", ctx).unwrap();
let result: String = bar.call((), ctx).unwrap();
assert_eq!(result, "hello");
```

## Lua Support

Aiming for Lua 5.4, currently missing support for `<const>` and `<close>`.

The garbage collector is incremental only.

### Standard Library

| Library     | Supported                                                                                                                                         |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `basic`     | ⚠️ Missing `warn` and level parameter for `error`. `load` and `loadfile` can't read binary chunks.                                                |
| `coroutine` | ⚠️ Missing `coroutine.close`                                                                                                                      |
| `debug`     | ⚠️ Only `debug.getregistry`, `debug.getmetatable`, `debug.setmetatable`, `debug.traceback`, `debug.gethook` and count support for `debug.sethook` |
| `math`      | ✅ Note: A time based seed is not supplied to `math.randomseed` for determinism.                                                                  |
| `os`        | ⚠️ Only `os.clock`                                                                                                                                |
| `package`   | ⛔ Not yet                                                                                                                                        |
| `string`    | ⚠️ Missing `string.gmatch`, `string.gsub`, `string.dump`, `string.format`, `string.pack`, `string.unpack` and `string.packsize`                   |
| `table`     | ⚠️ Missing `table.move` and `table.sort`                                                                                                          |
| `utf8`      | ⛔ Not yet                                                                                                                                        |
| `io`        | ⛔ Not yet                                                                                                                                        |

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
