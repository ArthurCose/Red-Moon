use crate::interpreter::coroutine::Coroutine;
use crate::interpreter::interpreted_function::Function;
use crate::interpreter::native_function::NativeFunction;
use crate::interpreter::table::Table;
use crate::interpreter::value_stack::StackValue;
use crate::interpreter::ByteString;

#[derive(Clone)]
pub(crate) enum HeapValue {
    StackValue(StackValue),
    Bytes(ByteString),
    Table(Box<Table>),
    NativeFunction(NativeFunction),
    Function(Function),
    Coroutine(Box<Coroutine>),
}

impl HeapValue {
    pub(crate) fn gc_size(&self) -> usize {
        match self {
            HeapValue::StackValue(_) => std::mem::size_of::<Self>(),
            HeapValue::Bytes(bytes) => bytes.heap_size() + std::mem::size_of::<Self>(),
            HeapValue::Table(table) => {
                table.heap_size() + std::mem::size_of::<Table>() + std::mem::size_of::<Self>()
            }
            HeapValue::NativeFunction(_) => std::mem::size_of::<Self>(),
            HeapValue::Function(function) => function.heap_size() + std::mem::size_of::<Self>(),
            HeapValue::Coroutine(coroutine) => {
                coroutine.heap_size()
                    + std::mem::size_of::<Coroutine>()
                    + std::mem::size_of::<Self>()
            }
        }
    }
}
