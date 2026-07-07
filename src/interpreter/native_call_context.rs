use super::value_stack::StackValue;
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::heap::{NativeFnObjectKey, StorageKey};
use crate::interpreter::{Continuation, Vm, VmContext};
use crate::values::{
    ForEachValue, FromValue, FromValues, FunctionRef, MultiValue, NativeValue, SharedNativeValue,
    Value,
};
use std::ops::RangeBounds;

pub struct NativeCallContext {
    pub(crate) key: NativeFnObjectKey,
    pub(crate) stack_start: usize,
    pub(crate) arg_count: usize,
    pub(crate) return_count: usize,
}

impl NativeCallContext {
    /// A [FunctionRef] to the called function. Useful for recursion without ref cycles.
    pub fn function_ref(&self, ctx: &mut VmContext) -> FunctionRef {
        let heap = &mut ctx.vm.execution_data.heap;
        let heap_ref = heap.create_ref(StorageKey::NativeFunction(self.key));
        FunctionRef(heap_ref)
    }

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

    pub fn set_capture<V: NativeValue + Clone>(
        &self,
        value: impl NativeValue,
        ctx: &mut VmContext,
    ) -> Option<V> {
        let heap = &mut ctx.vm.execution_data.heap;
        let value = SharedNativeValue::new(value);
        let prev_capture = heap.storage.captures.insert(self.key, value)?;
        prev_capture.take()
    }

    pub fn get_capture<'vm, V: NativeValue>(&self, ctx: &'vm VmContext) -> Option<&'vm V> {
        let heap = &ctx.vm.execution_data.heap;
        let capture = heap.storage.captures.get(&self.key)?;
        capture.get()
    }

    pub fn get_capture_mut<'vm, V: NativeValue + Clone>(
        &self,
        ctx: &'vm mut VmContext,
    ) -> Option<&'vm mut V> {
        let heap = &mut ctx.vm.execution_data.heap;
        let capture = heap.storage.captures.get_mut(&self.key)?;
        capture.get_mut()
    }

    pub fn remove_capture<V: NativeValue + Clone>(&self, ctx: &mut VmContext) -> Option<V> {
        let heap = &mut ctx.vm.execution_data.heap;
        let capture = heap.storage.captures.remove(&self.key)?;
        capture.take()
    }

    /// Takes the result of the last yieldable call
    pub fn take_resumed_result(
        &self,
        ctx: &mut VmContext,
    ) -> Option<Result<MultiValue, RuntimeError>> {
        ctx.vm.execution_data.coroutine_data.resumed_result.take()
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
    pub fn return_args(&mut self, start: usize, len: usize, ctx: &mut VmContext) {
        let execution = ctx.vm.execution_stack.last_mut().unwrap();

        for index in start..start + len {
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

        self.return_count += len;
    }

    /// Appends values from the call arguments to the return multivalue
    pub fn return_arg_range<R: RangeBounds<usize>>(&mut self, range: R, ctx: &mut VmContext) {
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

        self.return_args(start, end - start, ctx);
    }

    /// Suspends the currently running coroutine, data passed in are passed as extra results to `coroutine.resume(co)`
    ///
    /// This function always returns an error to bubble [RuntimeErrorData::Yield].
    pub fn yield_data<A: ForEachValue>(
        &mut self,
        data: A,
        resume_function: FunctionRef,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        let multi = MultiValue::pack(data, ctx)?;

        let execution_data = &mut ctx.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;

        if !coroutine_data.yield_permitted {
            return Err(RuntimeErrorData::InvalidYield.into());
        }

        if coroutine_data.yield_pending {
            coroutine_data.yield_pending = false;
            coroutine_data.in_progress_yield.clear();

            return Err(RuntimeErrorData::UnhandledYield.into());
        }

        coroutine_data
            .in_progress_yield
            .push((Continuation::Callback(resume_function), true));

        coroutine_data.yield_pending = true;

        Err(RuntimeErrorData::Yield(multi).into())
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
