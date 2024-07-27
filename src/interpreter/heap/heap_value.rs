use super::Heap;
use crate::interpreter::coroutine::Coroutine;
use crate::interpreter::interpreted_function::Function;
use crate::interpreter::native_function::NativeFunction;
use crate::interpreter::table::Table;
use crate::interpreter::value_stack::StackValue;
use crate::interpreter::{ByteString, MultiValue, TypeName};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum HeapValue {
    StackValue(StackValue),
    Bytes(ByteString),
    Table(Box<Table>),
    #[cfg_attr(feature = "serde", serde(serialize_with = "serialize_native_fn"))]
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_native_fn"))]
    NativeFunction(NativeFunction<MultiValue>),
    Function(Function),
    Coroutine(Box<Coroutine>),
}

#[cfg(feature = "serde")]
use crate::serde_util::{impl_serde_deserialize_stub_fn, impl_serde_serialize_stub_fn};

#[cfg(feature = "serde")]
impl_serde_serialize_stub_fn!(serialize_native_fn, NativeFunction<MultiValue>);
#[cfg(feature = "serde")]
impl_serde_deserialize_stub_fn!(
    deserialize_native_fn,
    NativeFunction<MultiValue>,
    NativeFunction::from(
        |mut args: MultiValue, _: &mut crate::interpreter::VmContext| {
            args.clear();
            Ok(args)
        }
    )
);

impl HeapValue {
    pub(crate) fn gc_size(&self) -> usize {
        let heap_size = match self {
            HeapValue::StackValue(_) => 0,
            HeapValue::Bytes(bytes) => bytes.heap_size(),
            HeapValue::Table(table) => table.heap_size() + std::mem::size_of::<Table>(),
            HeapValue::NativeFunction(_) => 0,
            HeapValue::Function(function) => function.heap_size(),
            HeapValue::Coroutine(coroutine) => {
                coroutine.heap_size() + std::mem::size_of::<Coroutine>()
            }
        };

        std::mem::size_of::<Self>() + heap_size
    }

    pub(crate) fn type_name(&self, heap: &Heap) -> TypeName {
        match self {
            HeapValue::StackValue(v) => v.type_name(heap),
            HeapValue::Bytes(_) => TypeName::String,
            HeapValue::Table(_) => TypeName::Table,
            HeapValue::NativeFunction(_) | HeapValue::Function(_) => TypeName::Function,
            HeapValue::Coroutine(_) => TypeName::Thread,
        }
    }
}
