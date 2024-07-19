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
}

impl HeapValue {
    pub(crate) fn gc_size(&self) -> usize {
        match self {
            HeapValue::StackValue(_) => std::mem::size_of::<Self>(),
            HeapValue::Bytes(bytes) => {
                bytes.gc_size() + std::mem::size_of::<Self>() - std::mem::size_of_val(bytes)
            }
            HeapValue::Table(table) => table.gc_size() + std::mem::size_of::<Self>(),
            HeapValue::NativeFunction(_) => std::mem::size_of::<Self>(),
            HeapValue::Function(function) => {
                function.gc_size() + std::mem::size_of::<Self>() - std::mem::size_of_val(function)
            }
        }
    }
}
