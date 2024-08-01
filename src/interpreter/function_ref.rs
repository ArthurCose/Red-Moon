use super::heap::{Heap, HeapRef, Storage, StorageKey};
use super::{FromMulti, IntoMulti, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use slotmap::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionRef(pub(crate) HeapRef<StorageKey>);

impl FunctionRef {
    #[inline]
    pub fn id(&self) -> u64 {
        match self.0.key() {
            StorageKey::NativeFunction(key) => {
                Storage::key_to_id(key.data(), Storage::NATIVE_FUNCTIONS_TAG)
            }
            StorageKey::Function(key) => Storage::key_to_id(key.data(), Storage::FUNCTIONS_TAG),
            _ => unreachable!(),
        }
    }

    pub(crate) fn test_validity(&self, heap: &Heap) -> Result<(), RuntimeErrorData> {
        let valid = match self.0.key() {
            StorageKey::Function(key) => heap.get_interpreted_fn(key).is_some(),
            StorageKey::NativeFunction(key) => heap.get_native_fn(key).is_some(),
            _ => false,
        };

        if valid {
            Ok(())
        } else {
            Err(RuntimeErrorData::InvalidRef)
        }
    }

    /// Returns false if there's no function with a matching tag, this function will receive the tag to maintain identity after serialization.
    ///
    /// Returns true if there's a function with a matching tag, that function will be replaced with a new copy of this function.
    #[cfg_attr(not(feature = "serde"), allow(unused))]
    pub fn rehydrate<T: super::IntoValue>(
        &self,
        tag: T,
        ctx: &mut VmContext,
    ) -> Result<bool, RuntimeError> {
        #[cfg(feature = "serde")]
        {
            use super::Value;

            let tag = tag.into_value(ctx)?;

            let heap = &mut ctx.vm.execution_data.heap;
            tag.test_validity(heap)?;

            if !matches!(
                tag,
                Value::Nil
                    | Value::Bool(_)
                    | Value::Integer(_)
                    | Value::Float(_)
                    | Value::String(_)
            ) {
                return Err(RuntimeErrorData::InvalidTag.into());
            }

            let tag = tag.to_stack_value();

            let StorageKey::NativeFunction(new_key) = self.0.key() else {
                return Err(RuntimeErrorData::RequiresNativeFunction.into());
            };

            let old_key = match heap.tags.entry(tag) {
                indexmap::map::Entry::Occupied(entry) => *entry.get(),
                indexmap::map::Entry::Vacant(entry) => {
                    entry.insert(new_key);
                    return Ok(false);
                }
            };

            let Some(function) = heap.get_native_fn(new_key) else {
                return Err(RuntimeErrorData::InvalidRef.into());
            };

            let function = function.clone();

            let gc = &mut ctx.vm.execution_data.gc;
            heap.rehydrate(gc, old_key, function);

            if let Some(callback) = heap.resume_callbacks.get(&new_key) {
                heap.resume_callbacks.insert(old_key, callback.clone());
            }

            Ok(true)
        }

        #[cfg(not(feature = "serde"))]
        Ok(false)
    }

    pub fn call<A: IntoMulti, R: FromMulti>(
        &self,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<R, RuntimeError> {
        ctx.call_function_key(self.0.key().into(), args)
    }
}
