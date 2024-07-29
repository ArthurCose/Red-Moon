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

`load()` + `loadfile()`, currently unimplemented. Loading arbitrary bytecode should be memory safe, but currently can cause panics from `unreachable!()` / `unwrap()` checks, this may change in the future to prevent deserialized VMs from causing crashes.

`require()` is currently unimplemented. When it is implemented: there will be no support for loading C libraries (to prevent execution of untrusted files that could be on the system from other programs, and from inherent compatibility issues).

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
use red_moon::interpreter::{Vm, MultiValue};
use std::cell::Cell;
use std::rc::Rc;

// interior mutability is fine as long as serialization isn't necessary and Rc is not involved:
let counter = Cell::new(1);
// Rc is fine as long as the data is immutable:
let data = Rc::new(1);
// Rc with interior mutability will cause issues during rollback:
let rc_counter = Rc::new(Cell::new(1));

let mut vm = Vm::default();
let ctx = &mut vm.context();

// these can be captured, but not all are bug free:
let f = ctx.create_function(move |_, ctx| {
  let count = counter.get();
  counter.set(count + 1);
  MultiValue::pack(rc_counter.get() + *data + count, ctx)
});
```

## Serialization

Enabling the `serde` feature enables serialization support through [serde](https://crates.io/crates/serde).

Lua values and functions are serialized without issue, but special attention is necessary for app data and Rust functions. App data is not serialized and must be handled separately from the VM. Dynamically creating Rust functions should be avoided to allow for "rehydration", also be mindful of Rust captures as serialization of captures is not possible.

Rust functions can be tagged and reimplemented through the "rehydrate" function:

```rust
#![cfg(feature = "serde")]

use red_moon::interpreter::{Vm, VmContext, FunctionRef, MultiValue, Value};
use red_moon::errors::RuntimeError;

fn implement_foo(ctx: &mut VmContext) -> Result<bool, RuntimeError> {
  let f = ctx.create_function(|_, ctx| MultiValue::pack("hello", ctx));

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

Aiming for Lua 5.4, currently missing support for `<const>`, `<close>`, and `goto`.

The garbage collector is incremental only.

### Standard Library

| Library     | Supported                                                             |
| ----------- | --------------------------------------------------------------------- |
| `basic`     | ⚠️ Missing `load`, `loadfile`, `warn` and level parameter for `error` |
| `coroutine` | ⚠️ Missing `coroutine.close`                                          |
| `debug`     | ⛔ Not planned                                                        |
| `math`      | ⚠️ Missing `math.random` and `math.randomseed`                        |
| `os`        | ⚠️ Only `os.clock`                                                    |
| `package`   | ⛔ Not yet                                                            |
| `string`    | ⚠️ Only `string.len` and metamethods                                  |
| `table`     | ⚠️ Missing `table.move` and `table.sort`                              |
| `utf8`      | ⛔ Not yet                                                            |
| `io`        | ⛔ Not yet                                                            |

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
