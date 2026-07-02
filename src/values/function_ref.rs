use super::{ForEachValue, FromValues, NativeValue};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::heap::{Heap, HeapRef, Storage, StorageKey};
use crate::interpreter::{NativeCallContext, VmContext};
use crate::tag_native_type;
use slotmap::Key;

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FunctionRef(pub(crate) HeapRef<StorageKey>);

tag_native_type!(FunctionRef);

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
                    let closure_to_base = &mut heap.storage.closure_to_base;
                    let tagged_key = closure_to_base.get(&new_key).unwrap_or(&new_key);

                    entry.insert(*tagged_key);
                    return Ok(false);
                }
            };

            let Some(function) = heap.get_native_fn(new_key) else {
                return Err(RuntimeErrorData::InvalidRef.into());
            };

            let function = function.clone();

            heap.rehydrate(old_key, function);

            Ok(true)
        }

        #[cfg(not(feature = "serde"))]
        Ok(false)
    }

    pub fn call<A: ForEachValue, R: FromValues>(
        &self,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<R, RuntimeError> {
        ctx.call_function_key(self.0.key().into(), args)
    }

    /// The function called may yield if the caller is also yieldable.
    pub fn yieldable_call<A: ForEachValue, R: FromValues>(
        &self,
        args: A,
        call_ctx: &mut NativeCallContext,
        ctx: &mut VmContext,
        yield_response: impl FnOnce(
            &mut NativeCallContext,
            &mut VmContext,
        ) -> Result<FunctionRef, RuntimeError>,
    ) -> Result<Result<R, RuntimeError>, RuntimeError> {
        ctx.yieldable_call_function_key(self.0.key().into(), args, call_ctx, yield_response)
    }

    /// Creates a rollback safe closure.
    /// The capture can be read by a native function, it is immediately dropped by interpreted functions.
    pub fn create_closure(
        self,
        capture: impl NativeValue,
        ctx: &mut VmContext,
    ) -> Result<Self, RuntimeError> {
        let StorageKey::NativeFunction(key) = self.0.key() else {
            return Ok(self);
        };

        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;
        let Some(native_fn) = heap.get_native_fn(key) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        let cloned_fn = native_fn.clone();
        let closure_key = heap.store_native_fn(gc, cloned_fn);
        heap.store_capture(closure_key, capture);

        // track base implementation for rehydration
        #[cfg(feature = "serde")]
        {
            let base_key = *heap.storage.closure_to_base.get(&key).unwrap_or(&key);
            heap.storage.closure_to_base.insert(closure_key, base_key);

            let closure_keys = heap.storage.base_to_closures.entry(base_key).or_default();
            closure_keys.insert(closure_key);
        }

        let heap_ref = heap.create_ref(closure_key.into());

        // test after creating ref to avoid immediately collecting the generated value
        ctx.try_gc_step();

        Ok(Self(heap_ref))
    }
}
