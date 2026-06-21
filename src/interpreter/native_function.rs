use super::coroutine::YieldPermissions;
use super::heap::NativeFnObjectKey;
use super::value_stack::StackValue;
use super::{Continuation, Vm, VmContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::values::{ForEachValue, FromValue, FromValues, NativeValue, Value};
use std::ops::RangeBounds;
use std::rc::Rc;

pub struct NativeCallContext {
    pub(crate) key: NativeFnObjectKey,
    pub(crate) stack_start: usize,
    pub(crate) arg_count: usize,
    pub(crate) return_count: usize,
}

impl NativeCallContext {
    pub fn arg_count(&self) -> usize {
        self.arg_count
    }

    /// Gets an argument value using a zero based index.
    pub fn get_arg<V: FromValue>(
        &self,
        index: usize,
        ctx: &mut VmContext,
    ) -> Result<V, RuntimeError> {
        let value = if index < self.arg_count {
            // add two to skip the function ref and arg count
            let stack_index = self.stack_start + index + 2;
            let execution = ctx.vm.execution_stack.last_mut().unwrap();
            let value_stack = &execution.value_stack;
            value_stack.get_deref(&ctx.vm.execution_data.heap, stack_index)
        } else {
            StackValue::Nil
        };

        let value = Value::from_stack_value(&mut ctx.vm.execution_data.heap, value);

        V::from_value(value, ctx).map_err(|err| RuntimeError::new_bad_argument(index + 1, err))
    }

    /// Gets argument values.
    pub fn get_args<V: FromValues>(&self, ctx: &mut VmContext) -> Result<V, RuntimeError> {
        self.get_args_at(0, ctx)
    }

    /// Gets argument values starting from a zero based index.
    pub fn get_args_at<V: FromValues>(
        &self,
        mut index: usize,
        ctx: &mut VmContext,
    ) -> Result<V, RuntimeError> {
        V::from_values(ctx, |ctx| {
            let value = if index < self.arg_count {
                let stack_index = self.stack_start + index + 2;
                let execution = ctx.vm.execution_stack.last_mut().unwrap();
                let heap = &mut ctx.vm.execution_data.heap;
                let value_stack = &execution.value_stack;
                let stack_value = value_stack.get(stack_index);
                Some(Value::from_stack_value(heap, stack_value))
            } else {
                None
            };

            index += 1;

            value
        })
        .map_err(|err| RuntimeError::new_bad_argument(index, err))
    }

    pub fn set_capture<V: NativeValue>(
        &self,
        value: impl NativeValue,
        ctx: &mut VmContext,
    ) -> Option<V> {
        let heap = &mut ctx.vm.execution_data.heap;
        let prev_capture = heap.storage.captures.insert(self.key, Box::new(value))?;
        Some(*prev_capture.downcast().ok()?)
    }

    pub fn read_capture<'vm, V: NativeValue>(&self, ctx: &'vm VmContext) -> Option<&'vm V> {
        let heap = &ctx.vm.execution_data.heap;
        let capture = heap.storage.captures.get(&self.key)?;
        capture.downcast_ref().ok()
    }

    pub fn read_capture_mut<'vm, V: NativeValue>(
        &self,
        ctx: &'vm mut VmContext,
    ) -> Option<&'vm mut V> {
        let heap = &mut ctx.vm.execution_data.heap;
        let capture = heap.storage.captures.get_mut(&self.key)?;
        capture.downcast_mut().ok()
    }

    pub fn remove_capture<V: NativeValue>(&self, ctx: &mut VmContext) -> Option<V> {
        let heap = &mut ctx.vm.execution_data.heap;
        let capture = heap.storage.captures.remove(&self.key)?;
        Some(*capture.downcast().ok()?)
    }

    /// Appends values to the return multivalue
    pub fn return_values(
        &mut self,
        values: impl ForEachValue,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        values.for_each_value(ctx, |result, ctx| {
            let value = result?;
            value.test_validity(&ctx.vm.execution_data.heap)?;

            let execution = ctx.vm.execution_stack.last_mut().unwrap();
            execution.value_stack.push(value.to_stack_value());
            self.return_count += 1;

            Ok(())
        })?;

        Ok(())
    }

    /// Appends a value from the call arguments to the return multivalue
    pub fn return_arg(&mut self, index: usize, ctx: &mut VmContext) {
        let execution = ctx.vm.execution_stack.last_mut().unwrap();

        let value = if index < self.arg_count {
            // add two to skip the function ref and arg count
            let stack_index = self.stack_start + index + 2;
            let value_stack = &mut execution.value_stack;
            value_stack.get_deref(&ctx.vm.execution_data.heap, stack_index)
        } else {
            StackValue::Nil
        };

        execution.value_stack.push(value);
        self.return_count += 1;
    }

    /// Appends values from the call arguments to the return multivalue
    pub fn return_args<R: RangeBounds<usize>>(&mut self, range: R, ctx: &mut VmContext) {
        let execution = ctx.vm.execution_stack.last_mut().unwrap();

        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => *i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => self.arg_count,
        };

        for index in start..end {
            let value = if index < self.arg_count {
                // add two to skip the function ref and arg count
                let stack_index = self.stack_start + index + 2;
                let value_stack = &mut execution.value_stack;
                value_stack.get_deref(&ctx.vm.execution_data.heap, stack_index)
            } else {
                StackValue::Nil
            };

            execution.value_stack.push(value);
        }

        self.return_count += end - start;
    }

    #[inline]
    pub(crate) fn flush_return_values_to_args(&mut self, vm: &mut Vm) {
        let execution = vm.execution_stack.last_mut().unwrap();
        let value_stack = &mut execution.value_stack;
        value_stack.chip(self.stack_start + 1, self.return_count + 1);
        self.return_count = 0;
    }

    #[inline]
    pub(crate) fn return_count_index(&self) -> usize {
        self.stack_start + self.arg_count + 2
    }

    #[inline]
    pub(crate) fn finalize(&self, vm: &mut Vm) {
        let execution = vm.execution_stack.last_mut().unwrap();
        let stack_index = self.return_count_index();

        let value_stack = &mut execution.value_stack;
        value_stack.set(stack_index, StackValue::Integer(self.return_count as _));
    }
}

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub(crate) trait NativeFunctionTrait<A>:
    Fn(NativeFnObjectKey, A, &mut VmContext) -> Result<NativeCallContext, RuntimeError>
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait<A>>;
}

impl<
    A,
    T: Fn(NativeFnObjectKey, A, &mut VmContext) -> Result<NativeCallContext, RuntimeError>
        + Clone
        + 'static,
> NativeFunctionTrait<A> for T
{
    fn deep_clone(&self) -> Rc<dyn NativeFunctionTrait<A>> {
        Rc::new(self.clone())
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct NativeFunction<A> {
    #[cfg_attr(feature = "serde", serde(with = "serde_callback"))]
    pub(crate) callback: Rc<dyn NativeFunctionTrait<A>>,
}

#[cfg(feature = "serde")]
mod serde_callback {
    use super::*;

    pub(super) fn serialize<A, S>(
        _: &Rc<dyn NativeFunctionTrait<A>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Serialize::serialize(&(), serializer)
    }

    pub(super) fn deserialize<'de, A, D>(
        deserializer: D,
    ) -> Result<Rc<dyn NativeFunctionTrait<A>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _: () = serde::Deserialize::deserialize(deserializer)?;

        Ok(Rc::new(|_, _: A, _: &mut VmContext| {
            Err(RuntimeErrorData::FunctionLostInSerialization.into())
        }))
    }
}

impl<A> NativeFunction<A> {
    pub(crate) fn call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<NativeCallContext, RuntimeError> {
        let coroutine_data = &ctx.vm.execution_data.coroutine_data;

        if coroutine_data.yield_permissions.allows_yield {
            self.yieldable_call(key, args, ctx)
        } else {
            self.non_yielding_call(key, args, ctx)
        }
    }

    fn non_yielding_call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<NativeCallContext, RuntimeError> {
        (self.callback)(key, args, ctx).map_err(|mut err| {
            if matches!(err.data, RuntimeErrorData::Yield(_)) {
                err.data = RuntimeErrorData::InvalidYield
            }
            err
        })
    }

    fn yieldable_call(
        &self,
        key: NativeFnObjectKey,
        args: A,
        ctx: &mut VmContext,
    ) -> Result<NativeCallContext, RuntimeError> {
        let execution_data = &mut ctx.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;

        if !coroutine_data.in_progress_yield.is_empty() {
            return Err(RuntimeErrorData::UnhandledYield.into());
        }

        let previous_yield_permissions = coroutine_data.yield_permissions;
        let continuation_previously_set = coroutine_data.continuation_state_set;

        coroutine_data.yield_permissions = YieldPermissions {
            parent_allows_yield: previous_yield_permissions.allows_yield,
            allows_yield: execution_data.heap.resume_callbacks.contains_key(&key),
        };
        coroutine_data.continuation_state_set = false;

        let result = match (self.callback)(key, args, ctx) {
            Ok(values) => {
                let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                if !coroutine_data.in_progress_yield.is_empty() {
                    Err(RuntimeErrorData::UnhandledYield.into())
                } else {
                    Ok(values)
                }
            }
            Err(mut err) => {
                let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                if matches!(err.data, RuntimeErrorData::Yield(_)) {
                    if coroutine_data.continuation_state_set {
                        let Some(state) = coroutine_data.continuation_states.pop() else {
                            return Err(RuntimeErrorData::InvalidInternalState.into());
                        };

                        // pass the continuation
                        let continuation = Continuation::Callback(key, state);

                        coroutine_data.in_progress_yield.push((
                            continuation,
                            coroutine_data.yield_permissions.parent_allows_yield,
                        ));
                    } else {
                        err.data = RuntimeErrorData::UnhandledYield;
                    }
                }

                Err(err)
            }
        };

        let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;
        coroutine_data.yield_permissions = previous_yield_permissions;
        coroutine_data.continuation_state_set = continuation_previously_set;

        result
    }

    pub(crate) fn shallow_clone(&self) -> Self {
        Self {
            callback: self.callback.clone(),
        }
    }
}

impl<A> Clone for NativeFunction<A> {
    fn clone(&self) -> Self {
        Self {
            callback: self.callback.deep_clone(),
        }
    }
}

impl<A, F> From<F> for NativeFunction<A>
where
    F: Fn(NativeFnObjectKey, A, &mut VmContext) -> Result<NativeCallContext, RuntimeError>
        + Clone
        + 'static,
{
    fn from(value: F) -> Self {
        Self {
            callback: Rc::new(value),
        }
    }
}
