mod byte_string;
mod cache_pools;
mod coroutine;
mod coroutine_ref;
mod debug_hooks;
mod execution;
mod function_ref;
mod heap;
mod instruction;
mod interpreted_function;
mod metatable_keys;
mod module;
mod multivalue;
mod native_function;
mod number;
mod string_ref;
mod struct_set;
mod table;
mod table_ref;
mod type_erased_snapshot;
mod up_values;
mod value;
mod value_stack;
mod value_traits;
mod vm;

#[cfg(feature = "instruction_metrics")]
mod instruction_metrics;

pub use byte_string::ByteString;
pub use coroutine::CoroutineStatus;
pub use coroutine_ref::CoroutineRef;
pub use debug_hooks::HookMask;
pub use function_ref::FunctionRef;
pub use heap::GarbageCollectorConfig;
pub use instruction::{ConstantIndex, Instruction, Register, ReturnMode};
pub use module::{Chunk, Module, SourceMapping, UpValueSource};
pub use multivalue::MultiValue;
pub use native_function::NativeCallContext;
pub use number::Number;
pub use string_ref::StringRef;
pub use table_ref::TableRef;
pub use type_erased_snapshot::TypeErasedSnapshot;
pub use value::{FromValue, IntoValue, TypeName, Value};
pub use value_traits::{ForEachValue, FromValues};
pub use vm::{Vm, VmContext, VmLimits};

pub(crate) use coroutine::Continuation;

#[cfg(feature = "serde")]
pub(crate) use {heap::StackObjectKey, interpreted_function::FunctionDefinition};

#[cfg(feature = "instruction_metrics")]
pub use instruction_metrics::InstructionMetrics;
