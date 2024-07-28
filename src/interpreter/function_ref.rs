use super::heap::HeapRef;
use super::{FromMulti, IntoMulti, VmContext};
use crate::errors::RuntimeError;
use slotmap::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionRef(pub(crate) HeapRef);

impl FunctionRef {
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.key().data().as_ffi()
    }

    #[cfg(feature = "serde")]
    pub fn hydrate<T: super::IntoValue>(
        &self,
        tag: T,
        ctx: &mut VmContext,
    ) -> Result<bool, RuntimeError> {
        use super::heap::HeapValue;
        use super::Value;
        use crate::errors::RuntimeErrorData;

        let tag = tag.into_value(ctx)?;

        let heap = &mut ctx.vm.execution_data.heap;
        tag.test_validity(heap)?;

        if !matches!(
            tag,
            Value::Nil | Value::Bool(_) | Value::Integer(_) | Value::Float(_) | Value::String(_)
        ) {
            return Err(RuntimeErrorData::InvalidTag.into());
        }

        let tag = tag.to_stack_value();
        let new_key = self.0.key();

        let old_key = match heap.tags.entry(tag) {
            indexmap::map::Entry::Occupied(entry) => *entry.get(),
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(new_key);
                return Ok(false);
            }
        };

        let Some(heap_value) = heap.get(new_key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        if !matches!(
            heap_value,
            HeapValue::NativeFunction(_) | HeapValue::Function(_),
        ) {
            return Err(RuntimeErrorData::InvalidRef.into());
        }

        let heap_value = heap_value.clone();

        let gc = &mut ctx.vm.execution_data.gc;
        heap.set(gc, old_key, heap_value);

        Ok(true)
    }

    pub fn call<A: IntoMulti, R: FromMulti>(
        &self,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<R, RuntimeError> {
        ctx.call_function_key(self.0.key(), args)
    }
}
