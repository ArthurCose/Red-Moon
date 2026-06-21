mod debug_hooks;
mod instruction;
mod interpreted_function;
mod metatable_keys;
mod module;
mod native_function;
mod type_set;
mod up_values;
mod vm;

pub(crate) mod cache_pools;
pub(crate) mod coroutine;
pub(crate) mod execution;
pub(crate) mod heap;
pub(crate) mod table;
pub(crate) mod value_stack;

#[cfg(feature = "instruction_metrics")]
mod instruction_metrics;

pub use debug_hooks::HookMask;
pub use heap::GarbageCollectorConfig;
pub use instruction::{ConstantIndex, Instruction, Register, ReturnMode};
pub use module::{Chunk, Module, SourceMapping, UpValueSource};
pub use native_function::NativeCallContext;
pub use vm::{Vm, VmContext, VmLimits};

pub(crate) use coroutine::Continuation;

#[cfg(feature = "serde")]
pub(crate) use {heap::StackObjectKey, interpreted_function::FunctionDefinition};

#[cfg(feature = "instruction_metrics")]
pub use instruction_metrics::InstructionMetrics;
