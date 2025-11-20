use super::coroutine::Continuation;
use super::heap::{BytesObjectKey, FnObjectKey, Heap, NativeFnObjectKey};
use super::instruction::{Instruction, Register, ReturnMode};
use super::interpreted_function::Function;
use super::multivalue::MultiValue;
use super::native_function::NativeCallContext;
use super::table::Table;
use super::value_stack::{StackValue, ValueStack};
use super::vm::{ExecutionAccessibleData, Vm};
use super::{FromValues, TypeName, UpValueSource, Value, VmContext};
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData};
use crate::languages::lua::coerce_integer;
use std::borrow::Cow;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub(crate) enum CallResult {
    Call(usize, ReturnMode),
    Return(usize),
    StepGc,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct ReturnContext {
    pub(crate) interpreted: bool,
    pub(crate) tail_called: bool,
    pub(crate) stack_start: usize,
    pub(crate) register_base: usize,
    pub(crate) return_mode: ReturnMode,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct ExecutionContext {
    pub(crate) interpreter_stack: Vec<Interpreter>,
    pub(crate) return_contexts: Vec<ReturnContext>,
    pub(crate) value_stack: ValueStack,
}

impl ExecutionContext {
    pub(crate) fn call_interpreted(
        function_key: FnObjectKey,
        args: MultiValue,
        vm: &mut Vm,
    ) -> Result<ExecutionReturnValues, RuntimeError> {
        let exec_data = &mut vm.execution_data;
        let mut value_stack = exec_data.cache_pools.create_value_stack();

        let Some(function) = exec_data.heap.get_interpreted_fn(function_key) else {
            return Err(RuntimeErrorData::InvalidInternalState.into());
        };

        value_stack.push(function_key.into());
        args.push_stack_multi(&mut value_stack);
        exec_data.cache_pools.store_multi(args);

        let interpreter_stack = vec![Interpreter {
            function: function.clone(),
            next_instruction_index: 0,
            stack_start: 0,
            register_base: value_stack.len(),
        }];

        let execution = Self {
            interpreter_stack,
            return_contexts: vec![ReturnContext {
                interpreted: true,
                tail_called: false,
                stack_start: 0,
                register_base: value_stack.len(),
                return_mode: ReturnMode::Multi,
            }],
            value_stack,
        };

        vm.execution_stack.push(execution);
        ExecutionContext::resume(vm)
    }

    pub(crate) fn call_native_fn(
        function_key: NativeFnObjectKey,
        args: MultiValue,
        vm: &mut Vm,
    ) -> Result<ExecutionReturnValues, RuntimeError> {
        let exec_data = &mut vm.execution_data;

        let Some(function) = exec_data.heap.get_native_fn(function_key) else {
            return Err(RuntimeErrorData::InvalidInternalState.into());
        };

        let function = function.shallow_clone();

        Self::call_closure(args, vm, |call_ctx, ctx| {
            function.call(function_key, call_ctx, ctx)
        })
    }

    pub(crate) fn call_closure(
        args: MultiValue,
        vm: &mut Vm,
        callback: impl FnOnce(
            NativeCallContext,
            &mut VmContext,
        ) -> Result<NativeCallContext, RuntimeError>,
    ) -> Result<ExecutionReturnValues, RuntimeError> {
        let exec_data = &mut vm.execution_data;
        let mut value_stack = exec_data.cache_pools.create_value_stack();

        // push a placeholder for the function reference
        value_stack.push(Default::default());

        // push args
        args.push_stack_multi(&mut value_stack);
        let arg_count = args.len() as _;
        exec_data.cache_pools.store_multi(args);

        // push a placeholder for the return count
        value_stack.push(Default::default());

        let native_call_ctx = NativeCallContext {
            stack_start: 0,
            arg_count,
            return_count: 0,
        };

        vm.execution_stack.push(Self {
            interpreter_stack: Vec::new(),
            return_contexts: Vec::new(),
            value_stack,
        });

        let return_count_index = native_call_ctx.return_count_index();
        let result = callback(native_call_ctx, &mut vm.context());

        match result {
            Ok(call_ctx) => call_ctx.finalize(vm),
            Err(err) => {
                let execution = vm.execution_stack.pop().unwrap();
                let value_stack = execution.value_stack;
                vm.execution_data.cache_pools.store_value_stack(value_stack);
                return Err(err);
            }
        }

        let execution = vm.execution_stack.pop().unwrap();

        Ok(ExecutionReturnValues {
            value_stack: execution.value_stack,
            count_register: return_count_index,
        })
    }

    pub(crate) fn call_value(
        value: StackValue,
        mut args: MultiValue,
        vm: &mut Vm,
    ) -> Result<ExecutionReturnValues, RuntimeError> {
        let exec_data = &mut vm.execution_data;

        let function_value = resolve_call(exec_data, value, |heap, value| {
            args.push_front(Value::from_stack_value(heap, value));
        })?;

        match function_value {
            StackValue::NativeFunction(key) => ExecutionContext::call_native_fn(key, args, vm),
            StackValue::Function(key) => ExecutionContext::call_interpreted(key, args, vm),
            _ => Err(RuntimeErrorData::InvalidRef.into()),
        }
    }

    pub(crate) fn resume(vm: &mut Vm) -> Result<ExecutionReturnValues, RuntimeError> {
        let coroutine_data = &vm.execution_data.coroutine_data;
        if !coroutine_data.in_progress_yield.is_empty() {
            return Err(RuntimeErrorData::UnhandledYield.into());
        }

        let mut execution = vm.execution_stack.last_mut().unwrap();

        while let Some(interpreter) = execution.interpreter_stack.last_mut() {
            let exec_data = &mut vm.execution_data;

            let result = match interpreter.resume(&mut execution.value_stack, exec_data) {
                Ok(result) => result,
                Err(err) => return Err(Self::unwind_error(vm, err)),
            };

            match result {
                CallResult::Call(registry_index, return_mode) => {
                    // expects the stack to contain (starting at stack_start)
                    // the function to call
                    // arg len
                    // each arg

                    // expects the return values to be stored in the same format as args

                    let stack_start = interpreter.register_base + registry_index;

                    let StackValue::Integer(mut arg_count) =
                        execution.value_stack.get(stack_start + 1)
                    else {
                        return Err(Self::unwind_error(
                            vm,
                            IllegalInstruction::MissingArgCount.into(),
                        ));
                    };

                    let function_value = execution.value_stack.get(stack_start);
                    let function_key = resolve_call(exec_data, function_value, |_, value| {
                        execution.value_stack.insert(stack_start + 2, value);

                        arg_count += 1;
                        execution
                            .value_stack
                            .set(stack_start + 1, StackValue::Integer(arg_count));
                    });

                    let function_value = match function_key {
                        Ok(key) => key,
                        Err(err) => return Err(Self::unwind_error(vm, err)),
                    };

                    match function_value {
                        StackValue::NativeFunction(key) => {
                            let Some(callback) = exec_data.heap.get_native_fn(key) else {
                                return Err(Self::unwind_error(
                                    vm,
                                    RuntimeErrorData::InvalidInternalState,
                                ));
                            };

                            let callback = callback.shallow_clone();

                            // handle tail call
                            let mut return_context = ReturnContext {
                                interpreted: false,
                                tail_called: false,
                                stack_start,
                                register_base: 0, // resolved later
                                return_mode,
                            };

                            if return_mode == ReturnMode::TailCall {
                                // if the callstack == 0 we retain the TailCall return mode and handle it later
                                // remove caller and recycle value stacks
                                if let Some(parent_context) = execution.return_contexts.pop() {
                                    if parent_context.interpreted {
                                        execution.interpreter_stack.pop();
                                    }

                                    // adopt caller's return mode and stack placement
                                    execution.value_stack.chip(
                                        parent_context.stack_start,
                                        execution.value_stack.len() - stack_start,
                                    );
                                    return_context.return_mode = parent_context.return_mode;
                                    return_context.stack_start = parent_context.stack_start;
                                }
                                return_context.tail_called = true;
                            }

                            // resolve call context and return register
                            let native_call_ctx = NativeCallContext {
                                stack_start: return_context.stack_start,
                                arg_count: arg_count as _,
                                return_count: 0,
                            };
                            return_context.register_base = native_call_ctx.return_count_index();

                            // push a placeholder for the return count
                            let value_stack = &mut execution.value_stack;
                            value_stack.set(return_context.register_base, Default::default());

                            // update tracked stack size in case the native function calls an interpreted function
                            let old_stack_size = exec_data.tracked_stack_size;
                            exec_data.tracked_stack_size = old_stack_size + value_stack.len();

                            // store return context for tracebacks and coroutine resuming
                            execution.return_contexts.push(return_context);

                            // call the function
                            let result = callback.call(key, native_call_ctx, &mut vm.context());

                            // revert tracked stack size before handling the result
                            vm.execution_data.tracked_stack_size = old_stack_size;

                            match result {
                                Ok(call_ctx) => call_ctx.finalize(vm),
                                Err(mut err) => {
                                    // handle yielding
                                    if let RuntimeErrorData::Yield(_) = &mut err.data {
                                        let coroutine_data = &mut vm.execution_data.coroutine_data;

                                        if !coroutine_data.yield_permissions.allows_yield {
                                            // we can't yield here
                                            err.data = RuntimeErrorData::InvalidYield;
                                            return Err(Self::continue_unwind(vm, err));
                                        }

                                        let execution = vm.execution_stack.pop().unwrap();

                                        coroutine_data
                                            .in_progress_yield
                                            .push((Continuation::Execution(execution), true));

                                        return Err(err);
                                    }

                                    return Err(Self::continue_unwind(vm, err));
                                }
                            }

                            // juggling lifetimes
                            execution = vm.execution_stack.last_mut().unwrap();
                            let return_context = execution.return_contexts.pop().unwrap();

                            let result = execution.handle_return(
                                &mut vm.execution_data,
                                return_context.return_mode,
                                return_context.stack_start,
                                return_context.register_base,
                            );

                            if let Err(err) = result {
                                return Err(Self::unwind_error(vm, err));
                            }
                        }
                        StackValue::Function(key) => {
                            let Some(func) = exec_data.heap.get_interpreted_fn(key) else {
                                return Err(Self::unwind_error(
                                    vm,
                                    RuntimeErrorData::InvalidInternalState,
                                ));
                            };

                            if return_mode == ReturnMode::TailCall {
                                // transform the caller
                                interpreter.function = func.clone();
                                interpreter.next_instruction_index = 0;
                                interpreter.register_base =
                                    interpreter.stack_start + 2 + arg_count as usize;

                                execution.value_stack.chip(
                                    interpreter.stack_start,
                                    execution.value_stack.len() - stack_start,
                                );

                                // update return context
                                let return_context = execution.return_contexts.last_mut().unwrap();
                                return_context.register_base = interpreter.register_base;
                                return_context.tail_called = true;
                            } else {
                                let register_base = stack_start + 2 + arg_count as usize;

                                let interpreter = Interpreter {
                                    function: func.clone(),
                                    next_instruction_index: 0,
                                    stack_start,
                                    register_base,
                                };
                                execution.interpreter_stack.push(interpreter);

                                execution.return_contexts.push(ReturnContext {
                                    interpreted: true,
                                    tail_called: false,
                                    stack_start,
                                    register_base,
                                    return_mode,
                                });
                            }
                        }
                        _ => {
                            let type_name = function_value.type_name(&exec_data.heap);

                            return Err(Self::unwind_error(
                                vm,
                                RuntimeErrorData::InvalidCall(type_name),
                            ));
                        }
                    }
                }
                CallResult::Return(registry_index) => {
                    let interpreter = execution.interpreter_stack.pop().unwrap();
                    let return_context = execution.return_contexts.pop().unwrap();

                    let return_count_index = return_context.register_base + registry_index;

                    let result = execution.handle_return(
                        exec_data,
                        return_context.return_mode,
                        return_context.stack_start,
                        return_count_index,
                    );

                    if let Err(err) = result {
                        // put the call back if the error context needs it
                        if matches!(
                            err,
                            RuntimeErrorData::IllegalInstruction(
                                IllegalInstruction::MissingReturnCount
                            )
                        ) {
                            execution.return_contexts.push(return_context);
                            execution.interpreter_stack.push(interpreter);
                        }

                        return Err(Self::unwind_error(vm, err));
                    }
                }
                CallResult::StepGc => {
                    exec_data.gc.step(
                        &exec_data.metatable_keys,
                        &exec_data.cache_pools,
                        &mut exec_data.heap,
                        &vm.execution_stack,
                        &exec_data.coroutine_data,
                        &exec_data.debug_hook,
                    );

                    execution = vm.execution_stack.last_mut().unwrap();
                }
            }
        }

        let execution = vm.execution_stack.pop().unwrap();

        Ok(ExecutionReturnValues {
            value_stack: execution.value_stack,
            count_register: 0,
        })
    }

    fn handle_return(
        &mut self,
        exec_data: &mut ExecutionAccessibleData,
        return_mode: ReturnMode,
        stack_start: usize,
        return_count_index: usize,
    ) -> Result<(), RuntimeErrorData> {
        let parent_base = self
            .return_contexts
            .last()
            .map(|r| r.register_base)
            .unwrap_or_default();

        let value_stack = &mut self.value_stack;

        // get return count
        let StackValue::Integer(return_count) = value_stack.get(return_count_index) else {
            return Err(IllegalInstruction::MissingReturnCount.into());
        };

        let mut return_count = return_count as usize;

        match return_mode {
            ReturnMode::Multi => {
                // remove extra values past the return values
                value_stack.chip(return_count_index + return_count + 1, 0);

                // chip under the return values and count
                value_stack.chip(stack_start, return_count + 1);
            }
            ReturnMode::Static(expected_count) => {
                return_count = return_count.min(expected_count as _);

                // remove extra values past the return values
                value_stack.chip(return_count_index + return_count + 1, 0);

                // chip under the return values
                value_stack.chip(stack_start, return_count);
            }
            ReturnMode::Destination(dest) => {
                // copy value
                let value = value_stack.get(return_count_index + 1);

                // remove all values
                value_stack.chip(stack_start, 0);

                // store value
                value_stack.set(parent_base + dest as usize, value);
            }
            ReturnMode::Extend(len_index) => {
                // remove extra values past the return values
                value_stack.chip(return_count_index + return_count + 1, 0);

                // get the return count
                let len_register = parent_base + len_index as usize;
                let StackValue::Integer(stored_return_count) = value_stack.get(len_register) else {
                    return Err(IllegalInstruction::MissingReturnCount.into());
                };

                // chip under the return values
                value_stack.chip(stack_start, return_count);
                // add the return count
                let count = stored_return_count + return_count as i64 - 1;
                let count_value = StackValue::Integer(count);
                value_stack.set(len_register, count_value);
            }
            ReturnMode::UnsizedDestinationPreserve(dest) => {
                let start = return_count_index + 1;
                let dest_index = parent_base + dest as usize;

                value_stack.copy_within(start..start + return_count, dest_index);
                value_stack.chip(dest_index + return_count, 0);
            }
            ReturnMode::TailCall => {
                // we're going to assume this is the final call
                // the return mode should've been resolved to something else earlier
                self.value_stack.chip(0, return_count + 1);
            }
            ReturnMode::Hook => {
                // remove all values
                value_stack.chip(stack_start, 0);
                exec_data.debug_hook.executing = false;
            }
        }

        Ok(())
    }

    pub(crate) fn handle_external_return(
        &mut self,
        exec_data: &mut ExecutionAccessibleData,
        return_values: &mut MultiValue,
    ) -> Result<(), RuntimeErrorData> {
        let return_context = self.return_contexts.pop().unwrap();

        self.value_stack.chip(return_context.register_base, 0);
        return_values.push_stack_multi(&mut self.value_stack);

        self.handle_return(
            exec_data,
            return_context.return_mode,
            return_context.stack_start,
            return_context.register_base,
        )
    }

    pub(crate) fn unwind_error(vm: &mut Vm, data: RuntimeErrorData) -> RuntimeError {
        Self::continue_unwind(vm, data.into())
    }

    pub(crate) fn continue_unwind(vm: &mut Vm, mut err: RuntimeError) -> RuntimeError {
        let Some(execution) = vm.execution_stack.pop() else {
            crate::debug_unreachable!();
            #[cfg(not(debug_assertions))]
            return RuntimeErrorData::InvalidInternalState.into();
        };

        let exec_data = &mut vm.execution_data;

        // recycle value stacks
        for interpreter in execution.interpreter_stack.into_iter().rev() {
            let instruction_index = interpreter.next_instruction_index.saturating_sub(1);
            let definition = interpreter.function.definition;
            let frame = definition.create_stack_trace_frame(instruction_index);
            err.trace.push_frame(frame);
        }

        let cache_pools = &mut exec_data.cache_pools;
        cache_pools.store_value_stack(execution.value_stack);

        err
    }
}

pub(crate) struct ExecutionReturnValues {
    value_stack: ValueStack,
    count_register: usize,
}

impl ExecutionReturnValues {
    pub(crate) fn unpack<R: FromValues>(self, ctx: &mut VmContext) -> Result<R, RuntimeError> {
        let StackValue::Integer(return_count) = self.value_stack.get(self.count_register) else {
            return Err(IllegalInstruction::MissingReturnCount.into());
        };

        let mut index = self.count_register + 1;
        let end = index + return_count as usize;

        let r = R::from_values(ctx, |ctx| {
            let value = if index < end {
                let heap = &mut ctx.vm.execution_data.heap;
                let stack_value = self.value_stack.get(index);
                Some(Value::from_stack_value(heap, stack_value))
            } else {
                None
            };

            index += 1;

            value
        });

        ctx.vm
            .execution_data
            .cache_pools
            .store_value_stack(self.value_stack);

        r
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Interpreter {
    pub(crate) function: Function,
    pub(crate) next_instruction_index: usize,
    pub(crate) stack_start: usize,
    pub(crate) register_base: usize,
}

impl Interpreter {
    fn resume(
        &mut self,
        value_stack: &mut ValueStack,
        exec_data: &mut ExecutionAccessibleData,
    ) -> Result<CallResult, RuntimeErrorData> {
        let mut for_loop_jump = false;

        if exec_data.tracked_stack_size + Register::MAX as usize > exec_data.limits.stack_size {
            return Err(RuntimeErrorData::StackOverflow);
        }

        while let Some(&instruction) = self
            .function
            .definition
            .instructions
            .get(self.next_instruction_index)
        {
            let gc = &mut exec_data.gc;
            let heap = &mut exec_data.heap;

            // check debug hook before incrementing instruction index
            let debug_hook = &mut exec_data.debug_hook;
            if debug_hook.count_instruction() {
                return Ok(debug_hook.call_count_hook(gc, heap, value_stack, self.register_base));
            }

            self.next_instruction_index += 1;

            #[cfg(feature = "instruction_metrics")]
            let _instruction_tracker = exec_data.instruction_tracking.track(instruction);

            match instruction {
                Instruction::Constant(_) => {
                    return Err(IllegalInstruction::UnexpectedConstant.into())
                }
                Instruction::SetNil(dest) => {
                    value_stack.set(self.register_base + dest as usize, StackValue::Nil);
                }
                Instruction::SetBool(dest, b) => {
                    let value = StackValue::Bool(b);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::SetInt(dest, value) => {
                    let value = StackValue::Integer(value as _);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadInt(dest, index) => {
                    let definition = &self.function.definition;
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = StackValue::Integer(*number);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadFloat(dest, index) => {
                    let definition = &self.function.definition;
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = StackValue::Float(f64::from_bits(*number as u64));
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadBytes(dest, index) => {
                    let definition = &self.function.definition;
                    let Some(&bytes_key) = definition.byte_strings.get(index as usize) else {
                        return Err(IllegalInstruction::MissingByteStringConstant(index).into());
                    };

                    value_stack.set(self.register_base + dest as usize, bytes_key.into());
                }
                Instruction::ClearFrom(dest) => {
                    let dest_index = self.register_base + dest as usize;
                    value_stack.resize(dest_index);
                }
                Instruction::PrepMulti(dest, value) => {
                    let dest_index = self.register_base + dest as usize;
                    value_stack.set(dest_index, StackValue::Integer(value as _));
                    value_stack.resize(dest_index + 1);
                }
                Instruction::PrepSelf(dest, bytes_index) => {
                    let definition = &self.function.definition;
                    let Some(&bytes_key) = definition.byte_strings.get(bytes_index as usize) else {
                        return Err(
                            IllegalInstruction::MissingByteStringConstant(bytes_index).into()
                        );
                    };

                    let dest_index = self.register_base + dest as usize;
                    let base = value_stack.get_deref(heap, dest_index);
                    value_stack.set(dest_index + 2, base);

                    if let Some(call_result) = self.copy_from_table(
                        exec_data,
                        value_stack,
                        dest,
                        base,
                        bytes_key.into(),
                        |table, key| table.get_from_map(key),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::CreateTable(dest, len_index) => {
                    let definition = &self.function.definition;
                    let Some(&len) = definition.numbers.get(len_index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(len_index).into());
                    };

                    let table_key = heap.create_table(gc, len as _, 0);

                    value_stack.set(self.register_base + dest as usize, table_key.into());

                    if gc.should_step() {
                        return Ok(CallResult::StepGc);
                    }
                }
                Instruction::FlushToTable(dest, total, index_offset) => {
                    if let Some(call_result) =
                        self.flush_to_table(exec_data, value_stack, dest, total, index_offset)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::VariadicToTable(dest, src_start, index_offset) => {
                    if let Some(call_result) = self.variadic_to_table(
                        exec_data,
                        value_stack,
                        dest,
                        src_start,
                        index_offset,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyTableField(dest, table_index) => {
                    let definition = &self.function.definition;

                    // resolve field key
                    let Some(Instruction::Constant(bytes_index)) =
                        definition.instructions.get(self.next_instruction_index)
                    else {
                        return Err(IllegalInstruction::ExpectingConstant.into());
                    };
                    self.next_instruction_index += 1;

                    let Some(&bytes_key) = definition.byte_strings.get(*bytes_index as usize)
                    else {
                        return Err(
                            IllegalInstruction::MissingByteStringConstant(*bytes_index).into()
                        );
                    };

                    // table
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);

                    if let Some(call_result) = self.copy_from_table(
                        exec_data,
                        value_stack,
                        dest,
                        base,
                        bytes_key.into(),
                        |table, key| table.get_from_map(key),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyToTableField(table_index, src) => {
                    let definition = &self.function.definition;

                    // resolve field key
                    let Some(Instruction::Constant(bytes_index)) =
                        definition.instructions.get(self.next_instruction_index)
                    else {
                        return Err(IllegalInstruction::ExpectingConstant.into());
                    };
                    self.next_instruction_index += 1;

                    let Some(&bytes_key) = definition.byte_strings.get(*bytes_index as usize)
                    else {
                        return Err(
                            IllegalInstruction::MissingByteStringConstant(*bytes_index).into()
                        );
                    };

                    // table
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);

                    if let Some(call_result) = self.copy_to_table(
                        exec_data,
                        value_stack,
                        base,
                        bytes_key.into(),
                        src,
                        |table, key, value| table.set_in_map(key, value),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyTableValue(dest, table_index, key_index) => {
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);
                    let key = value_stack.get_deref(heap, self.register_base + key_index as usize);

                    if let Some(call_result) = self.copy_from_table(
                        exec_data,
                        value_stack,
                        dest,
                        base,
                        key,
                        |table, key| table.get(key),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyToTableValue(table_index, key_index, src) => {
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);
                    let key = value_stack.get_deref(heap, self.register_base + key_index as usize);

                    if let Some(call_result) = self.copy_to_table(
                        exec_data,
                        value_stack,
                        base,
                        key,
                        src,
                        |table, key, value| table.set(key, value),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyArg(dest, index) => self.copy_arg(value_stack, dest, index),
                Instruction::CopyArgs(dest, count) => {
                    self.copy_args(value_stack, dest, count);
                }
                Instruction::CopyVariadic(dest, count_register, skip) => {
                    self.copy_variadic(value_stack, dest, count_register, skip)?;
                }
                Instruction::CopyUnsizedVariadic(dest, skip) => {
                    self.copy_unsized_variadic(value_stack, dest, skip);
                }
                Instruction::Closure(dest, function_index) => {
                    if let Some(call_result) =
                        self.create_closure(exec_data, value_stack, dest, function_index as _)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyUpValue(dest, src) => {
                    let Some(key) = self.function.up_values.get(src as usize) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        return Err(RuntimeErrorData::InvalidInternalState);
                    };

                    let Some(value) = heap.get_stack_value(*key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        return Err(RuntimeErrorData::InvalidInternalState);
                    };

                    value_stack.set(self.register_base + dest as usize, *value);
                }
                Instruction::CopyToUpValueDeref(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);

                    let Some(key) = self.function.up_values.get(dest as usize) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        return Err(RuntimeErrorData::InvalidInternalState);
                    };

                    let Some(stored) = heap.get_stack_value_mut(gc, *key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        return Err(RuntimeErrorData::InvalidInternalState);
                    };

                    *stored = value
                }
                Instruction::Copy(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::CopyToDeref(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);
                    let dest_index = self.register_base + dest as usize;

                    if let StackValue::Pointer(key) = value_stack.get(dest_index) {
                        let Some(stored) = heap.get_stack_value_mut(gc, key) else {
                            crate::debug_unreachable!();
                            #[cfg(not(debug_assertions))]
                            return Err(RuntimeErrorData::InvalidInternalState);
                        };

                        *stored = value
                    } else {
                        value_stack.set(dest_index, value);
                    }
                }
                Instruction::CopyRangeToDeref(dest, src, count) => {
                    self.copy_range_to_deref(exec_data, value_stack, dest, src, count)?;
                }
                Instruction::Not(dest, src) => {
                    let value = !value_stack.is_truthy(self.register_base + src as usize);
                    value_stack.set(self.register_base + dest as usize, StackValue::Bool(value));
                }
                Instruction::Len(dest, src) => {
                    if let Some(call_result) =
                        self.resolve_len(exec_data, value_stack, dest, src)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::UnaryMinus(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.unm.0.key();

                    if let Some(call_result) = self.unary_number_operation(
                        (heap, value_stack),
                        (dest, src),
                        metamethod_key,
                        &|type_name| RuntimeErrorData::InvalidArithmetic(type_name),
                        &|heap, value| match value {
                            StackValue::Integer(n) => Ok(StackValue::Integer(-n)),
                            StackValue::Float(n) => Ok(StackValue::Float(-n)),
                            _ => Err(RuntimeErrorData::InvalidArithmetic(value.type_name(heap))),
                        },
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseNot(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.bnot.0.key();

                    if let Some(call_result) = self.unary_number_operation(
                        (heap, value_stack),
                        (dest, src),
                        metamethod_key,
                        &|type_name| RuntimeErrorData::InvalidArithmetic(type_name),
                        &|heap, value| {
                            Ok(StackValue::Integer(-arithmetic_cast_integer(heap, value)?))
                        },
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Add(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.add.0.key();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a.wrapping_add(b),
                        &|a, b| a + b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Subtract(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.sub.0.key();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a.wrapping_sub(b),
                        &|a, b| a - b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Multiply(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.mul.0.key();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a.wrapping_mul(b),
                        &|a, b| a * b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Division(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.div.0.key();

                    if let Some(call_result) = self.binary_float_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a / b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::IntegerDivision(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.idiv.0.key();

                    if let Some(call_result) = self.division_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a / b,
                        &|a, b| a / b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Modulus(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.modulus.0.key();

                    if let Some(call_result) = self.division_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a % b,
                        &|a, b| a % b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Power(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.pow.0.key();

                    if let Some(call_result) = self.binary_float_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a.powf(b),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseAnd(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.band.0.key();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a & b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseOr(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.bor.0.key();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a | b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseXor(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.bxor.0.key();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a ^ b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitShiftLeft(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.shl.0.key();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a << b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitShiftRight(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.shr.0.key();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a >> b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Equal(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.eq.0.key();

                    let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
                    let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

                    let equal = match (value_a, value_b) {
                        (StackValue::Float(float), StackValue::Integer(int))
                        | (StackValue::Integer(int), StackValue::Float(float)) => {
                            int as f64 == float
                        }
                        _ => {
                            if let Some(call_result) = self.try_binary_metamethods(
                                (heap, value_stack),
                                metamethod_key,
                                dest,
                                value_a,
                                value_b,
                            ) {
                                return Ok(call_result);
                            }

                            value_a == value_b
                        }
                    };

                    value_stack.set(self.register_base + dest as usize, StackValue::Bool(equal));
                }
                Instruction::LessThan(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.lt.0.key();

                    if let Some(call_result) = self.comparison_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a < b,
                        &|a, b| a < b,
                        &|heap, a, b| {
                            if let (StackValue::Bytes(key_a), StackValue::Bytes(key_b)) = (a, b) {
                                let Some(bytes_a) = heap.get_bytes(key_a) else {
                                    crate::debug_unreachable!();
                                    #[cfg(not(debug_assertions))]
                                    return None;
                                };

                                let Some(bytes_b) = heap.get_bytes(key_b) else {
                                    crate::debug_unreachable!();
                                    #[cfg(not(debug_assertions))]
                                    return None;
                                };

                                Some(bytes_a < bytes_b)
                            } else {
                                None
                            }
                        },
                    )? {
                        return Ok(call_result);
                    };
                }
                Instruction::LessThanEqual(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.le.0.key();

                    if let Some(call_result) = self.comparison_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        &|a, b| a <= b,
                        &|a, b| a <= b,
                        &|heap, a, b| {
                            if let (StackValue::Bytes(key_a), StackValue::Bytes(key_b)) = (a, b) {
                                let Some(bytes_a) = heap.get_bytes(key_a) else {
                                    crate::debug_unreachable!();
                                    #[cfg(not(debug_assertions))]
                                    return None;
                                };

                                let Some(bytes_b) = heap.get_bytes(key_b) else {
                                    crate::debug_unreachable!();
                                    #[cfg(not(debug_assertions))]
                                    return None;
                                };

                                Some(bytes_a <= bytes_b)
                            } else {
                                None
                            }
                        },
                    )? {
                        return Ok(call_result);
                    };
                }
                Instruction::Concat(dest, a, b) => {
                    if let Some(call_result) =
                        self.concat_values(exec_data, value_stack, dest, a, b)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::TestTruthy(expected, src) => {
                    if value_stack.is_truthy(self.register_base + src as usize) != expected {
                        self.next_instruction_index += 1;
                    }
                }
                Instruction::TestNil(src) => {
                    if value_stack.get_deref(heap, self.register_base + src as usize)
                        != StackValue::Nil
                    {
                        self.next_instruction_index += 1;
                    }
                }
                Instruction::NumericFor(src, forward_jump) => {
                    for_loop_jump =
                        self.numeric_for(heap, value_stack, for_loop_jump, src, forward_jump)?;
                }
                Instruction::JumpToForLoop(i) => {
                    self.next_instruction_index = i.into();
                    for_loop_jump = true;
                }
                Instruction::Jump(i) => {
                    self.next_instruction_index = i.into();
                }
                Instruction::Call(stack_start, return_mode) => {
                    return Ok(CallResult::Call(stack_start as usize, return_mode))
                }
                Instruction::Return(register) => return Ok(CallResult::Return(register as usize)),
            }
        }

        value_stack.set(self.register_base, StackValue::Integer(0));

        // exhausted instructions
        Ok(CallResult::Return(0))
    }

    fn flush_to_table(
        &mut self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        total: Register,
        index_offset: Register,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let definition = &self.function.definition;

        // get the table
        let dest_index = self.register_base + dest as usize;

        let StackValue::Table(table_key) = value_stack.get(dest_index) else {
            return Err(RuntimeErrorData::InvalidInternalState);
        };

        let gc = &mut exec_data.gc;
        let heap = &mut exec_data.heap;

        let Some(table) = heap.get_table_mut(gc, table_key) else {
            return Err(RuntimeErrorData::InvalidInternalState);
        };

        // get the index offset
        let mut index_offset = index_offset as usize;

        if let Some(&Instruction::Constant(index)) =
            definition.instructions.get(self.next_instruction_index)
        {
            self.next_instruction_index += 1;

            let Some(&len) = definition.numbers.get(index as usize) else {
                return Err(IllegalInstruction::MissingNumberConstant(index).into());
            };

            index_offset += len as usize;
        }

        let start = dest_index + 2;
        let end = start + total as usize;

        let original_size = table.heap_size();

        table.flush(index_offset, value_stack.get_slice(start..end));

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        if gc.should_step() {
            return Ok(Some(CallResult::StepGc));
        }

        Ok(None)
    }

    fn variadic_to_table(
        &mut self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        src_start: Register,
        index_offset: Register,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        // grab the table
        let dest_index = self.register_base + dest as usize;

        let StackValue::Table(table_key) = value_stack.get(dest_index) else {
            return Err(RuntimeErrorData::InvalidInternalState);
        };

        let gc = &mut exec_data.gc;
        let heap = &mut exec_data.heap;

        let Some(table) = heap.get_table_mut(gc, table_key) else {
            return Err(RuntimeErrorData::InvalidInternalState);
        };

        // get the index offset
        let mut index_offset = index_offset as usize;

        let definition = &self.function.definition;

        if let Some(&Instruction::Constant(index)) =
            definition.instructions.get(self.next_instruction_index)
        {
            self.next_instruction_index += 1;

            let Some(&len) = definition.numbers.get(index as usize) else {
                return Err(IllegalInstruction::MissingNumberConstant(index).into());
            };

            index_offset += len as usize;
        }

        // grab the count
        let count_index = dest_index + 1;
        let StackValue::Integer(count) = value_stack.get(count_index) else {
            return Err(IllegalInstruction::MissingVariadicCount.into());
        };

        let start = self.register_base + src_start as usize;
        let end = start + count as usize;

        let original_size = table.heap_size();

        table.reserve_list(count as usize);
        table.flush(index_offset, value_stack.get_slice(start..end));

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        if gc.should_step() {
            return Ok(Some(CallResult::StepGc));
        }

        Ok(None)
    }

    fn create_closure(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        function_index: usize,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let Some(&function_key) = self.function.definition.functions.get(function_index) else {
            return Err(IllegalInstruction::MissingFunctionConstant(function_index as _).into());
        };

        let heap = &mut exec_data.heap;

        let Some(func) = heap.get_interpreted_fn(function_key) else {
            return Err(RuntimeErrorData::InvalidInternalState);
        };

        if func.definition.up_values.is_empty() {
            value_stack.set(self.register_base + dest as usize, function_key.into());
        } else {
            // copy the function to pass captures
            let mut func = func.clone();

            // resolve captures
            let mut up_values = Vec::new();
            up_values.reserve_exact(func.definition.up_values.len());

            for capture_source in &func.definition.up_values {
                let key = match capture_source {
                    UpValueSource::Stack(src) => {
                        let src_index = self.register_base + *src as usize;
                        let value = value_stack.get(src_index);

                        if let StackValue::Pointer(key) = value {
                            key
                        } else {
                            // move the stack value to the heap
                            let key = heap.store_stack_value(&mut exec_data.gc, value);
                            value_stack.set(src_index, StackValue::Pointer(key));
                            key
                        }
                    }
                    UpValueSource::UpValue(src) => {
                        let src_index = *src as usize;

                        let Some(key) = self.function.up_values.get(src_index) else {
                            crate::debug_unreachable!();
                            #[cfg(not(debug_assertions))]
                            return Err(RuntimeErrorData::InvalidInternalState);
                        };

                        *key
                    }
                };

                up_values.push(key);
            }

            func.up_values = up_values.into();

            // store the new function
            let function_key = heap.store_interpreted_fn(&mut exec_data.gc, func);
            value_stack.set(self.register_base + dest as usize, function_key.into());

            if exec_data.gc.should_step() {
                return Ok(Some(CallResult::StepGc));
            }
        }

        Ok(None)
    }

    fn resolve_len(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        src: Register,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let metamethod_key = exec_data.metatable_keys.len.0.key();
        let heap = &mut exec_data.heap;

        let value_a = value_stack.get_deref(heap, self.register_base + src as usize);

        // default behavior
        let len = match value_a {
            StackValue::Table(key) => {
                let Some(table) = heap.get_table(key) else {
                    return Err(RuntimeErrorData::InvalidInternalState);
                };

                table.list_len()
            }
            StackValue::Bytes(key) => {
                let Some(bytes) = heap.get_bytes(key) else {
                    return Err(RuntimeErrorData::InvalidInternalState);
                };

                bytes.len()
            }
            _ => return Err(RuntimeErrorData::NoLength(value_a.type_name(heap))),
        };

        // try metamethod before using the default value
        if matches!(value_a, StackValue::Table(_)) {
            if let Some(call_result) =
                self.unary_metamethod(heap, value_stack, metamethod_key, dest, value_a)
            {
                return Ok(Some(call_result));
            }
        }

        let value = StackValue::Integer(len as i64);
        value_stack.set(self.register_base + dest as usize, value);

        Ok(None)
    }

    fn concat_values(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        a: Register,
        b: Register,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let metamethod_key = exec_data.metatable_keys.concat.0.key();

        let gc = &mut exec_data.gc;
        let heap = &mut exec_data.heap;

        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        // default behavior
        let string_a = stringify(heap, value_a);
        let string_b = stringify(heap, value_b);

        if let (Some(string_a), Some(string_b)) = (string_a, string_b) {
            let mut bytes = Vec::<u8>::with_capacity(string_a.len() + string_b.len());

            bytes.extend(string_a.iter());
            bytes.extend(string_b.iter());
            let bytes_key = heap.intern_bytes(gc, &bytes);
            value_stack.set(self.register_base + dest as usize, bytes_key.into());

            if gc.should_step() {
                return Ok(Some(CallResult::StepGc));
            }
        } else {
            // try metamethod as a fallback using the default value
            let call_result = self
                .try_binary_metamethods((heap, value_stack), metamethod_key, dest, value_a, value_b)
                .ok_or_else(|| {
                    let type_name_a = value_a.type_name(heap);

                    match type_name_a {
                        TypeName::String => {
                            RuntimeErrorData::AttemptToConcat(value_b.type_name(heap))
                        }
                        _ => RuntimeErrorData::AttemptToConcat(type_name_a),
                    }
                })?;

            return Ok(Some(call_result));
        }

        Ok(None)
    }

    /// Resolves stack value pointers, calls metamethods for heap values, calls coerce functions if necessary
    #[allow(clippy::too_many_arguments)]
    fn resolve_binary_operand<T>(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        metamethod_key: BytesObjectKey,
        dest: Register,
        value_a: StackValue,
        value_b: StackValue,
        value: StackValue,
        coerce_value: impl Fn(&Heap, StackValue) -> Result<T, RuntimeErrorData>,
    ) -> Result<ValueOrCallResult<T>, RuntimeErrorData> {
        if value.lives_in_heap() {
            match self.binary_metamethod(
                heap,
                value_stack,
                (value, metamethod_key),
                dest,
                value_a,
                value_b,
            ) {
                Some(call_result) => return Ok(ValueOrCallResult::CallResult(call_result)),
                None => return Err(RuntimeErrorData::InvalidArithmetic(value.type_name(heap))),
            }
        }

        match coerce_value(heap, value) {
            Ok(coerced_value) => Ok(ValueOrCallResult::Value(coerced_value)),
            Err(err) => Err(err),
        }
    }

    /// Converts integers to floats if any operand is a float, preserves type otherwise
    fn binary_number_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: BytesObjectKey,
        integer_operation: &dyn Fn(i64, i64) -> i64,
        float_operation: &dyn Fn(f64, f64) -> f64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        let value = match (value_a, value_b) {
            (StackValue::Integer(a), StackValue::Integer(b)) => {
                StackValue::Integer(integer_operation(a, b))
            }
            (StackValue::Float(a), StackValue::Float(b)) => {
                StackValue::Float(float_operation(a, b))
            }
            (StackValue::Float(a), StackValue::Integer(b)) => {
                StackValue::Float(float_operation(a, b as f64))
            }
            (StackValue::Integer(a), StackValue::Float(b)) => {
                StackValue::Float(float_operation(a as f64, b))
            }
            _ => {
                // avoiding passing value_a and value_b
                // significantly improves speed when this branch isn't reached
                // this path is uncommon, so we'll accept the speed loss from refetching values
                return self.binary_number_metamethod_fallback(
                    (heap, value_stack),
                    (dest, a, b),
                    metamethod_key,
                );
            }
        };

        value_stack.set(self.register_base + dest as usize, value);

        Ok(None)
    }

    fn binary_number_metamethod_fallback(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: BytesObjectKey,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        let metamethod_value = if matches!(value_a, StackValue::Integer(_) | StackValue::Float(_)) {
            value_b
        } else {
            value_a
        };

        if metamethod_value.lives_in_heap() {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (metamethod_value, metamethod_key),
                dest,
                value_a,
                value_b,
            ) {
                return Ok(Some(call_result));
            }
        }

        Err(RuntimeErrorData::InvalidArithmetic(
            metamethod_value.type_name(heap),
        ))
    }

    /// Converts integers to floats
    fn binary_float_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: BytesObjectKey,
        operation: &dyn Fn(f64, f64) -> f64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        let float_a = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            dest,
            value_a,
            value_b,
            value_a,
            arithmetic_cast_float,
        )? {
            ValueOrCallResult::Value(value) => value,
            ValueOrCallResult::CallResult(call_result) => return Ok(Some(call_result)),
        };

        let float_b = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            dest,
            value_a,
            value_b,
            value_b,
            arithmetic_cast_float,
        )? {
            ValueOrCallResult::Value(value) => value,
            ValueOrCallResult::CallResult(call_result) => return Ok(Some(call_result)),
        };

        value_stack.set(
            self.register_base + dest as usize,
            StackValue::Float(operation(float_a, float_b)),
        );

        Ok(None)
    }

    /// Converts floats to integers. Errors if any float has a fractional part
    fn binary_integer_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: BytesObjectKey,
        operation: &dyn Fn(i64, i64) -> i64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        let int_a = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            dest,
            value_a,
            value_b,
            value_a,
            arithmetic_cast_integer,
        )? {
            ValueOrCallResult::Value(value) => value,
            ValueOrCallResult::CallResult(call_result) => return Ok(Some(call_result)),
        };

        let int_b = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            dest,
            value_a,
            value_b,
            value_b,
            arithmetic_cast_integer,
        )? {
            ValueOrCallResult::Value(value) => value,
            ValueOrCallResult::CallResult(call_result) => return Ok(Some(call_result)),
        };

        value_stack.set(
            self.register_base + dest as usize,
            StackValue::Integer(operation(int_a, int_b)),
        );

        Ok(None)
    }

    fn division_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: BytesObjectKey,
        integer_operation: &dyn Fn(i64, i64) -> i64,
        float_operation: &dyn Fn(f64, f64) -> f64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        let value = match (value_a, value_b) {
            (StackValue::Integer(a), StackValue::Integer(b)) => {
                if b == 0 {
                    return Err(RuntimeErrorData::DivideByZero);
                }
                StackValue::Integer(integer_operation(a, b))
            }
            (StackValue::Float(a), StackValue::Float(b)) => {
                StackValue::Float(float_operation(a, b))
            }
            (StackValue::Float(a), StackValue::Integer(b)) => {
                StackValue::Float(float_operation(a, b as f64))
            }
            (StackValue::Integer(a), StackValue::Float(b)) => {
                StackValue::Float(float_operation(a as f64, b))
            }
            _ => {
                return Ok(Some(
                    self.try_binary_metamethods(
                        (heap, value_stack),
                        metamethod_key,
                        dest,
                        value_a,
                        value_b,
                    )
                    .ok_or_else(|| match (value_a, value_b) {
                        (StackValue::Integer(_) | StackValue::Float(_), _) => {
                            RuntimeErrorData::InvalidArithmetic(value_b.type_name(heap))
                        }
                        _ => RuntimeErrorData::InvalidArithmetic(value_a.type_name(heap)),
                    })?,
                ));
            }
        };

        value_stack.set(self.register_base + dest as usize, value);

        Ok(None)
    }

    fn comparison_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: BytesObjectKey,
        integer_comparison: &dyn Fn(i64, i64) -> bool,
        float_comparison: &dyn Fn(f64, f64) -> bool,
        heap_comparison: &dyn Fn(&Heap, StackValue, StackValue) -> Option<bool>,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        let result = match (value_a, value_b) {
            (StackValue::Integer(int_a), StackValue::Integer(int_b)) => {
                integer_comparison(int_a, int_b)
            }
            (StackValue::Float(float), StackValue::Integer(int))
            | (StackValue::Integer(int), StackValue::Float(float)) => {
                float_comparison(int as f64, float)
            }
            (StackValue::Float(float_a), StackValue::Float(float_b)) => {
                float_comparison(float_a, float_b)
            }
            _ => {
                if let Some(call_result) = self.try_binary_metamethods(
                    (heap, value_stack),
                    metamethod_key,
                    dest,
                    value_a,
                    value_b,
                ) {
                    return Ok(Some(call_result));
                }

                let Some(result) = heap_comparison(heap, value_a, value_b) else {
                    return Err(RuntimeErrorData::InvalidCompare(
                        value_a.type_name(heap),
                        value_b.type_name(heap),
                    ));
                };

                result
            }
        };

        value_stack.set(self.register_base + dest as usize, StackValue::Bool(result));

        Ok(None)
    }

    fn try_binary_metamethods(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        metamethod_key: BytesObjectKey,
        dest: Register,
        value_a: StackValue,
        value_b: StackValue,
    ) -> Option<CallResult> {
        if value_a.lives_in_heap() {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (value_a, metamethod_key),
                dest,
                value_a,
                value_b,
            ) {
                return Some(call_result);
            }
        }

        if value_b.lives_in_heap() {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (value_b, metamethod_key),
                dest,
                value_a,
                value_b,
            ) {
                return Some(call_result);
            }
        }

        None
    }

    fn binary_metamethod(
        &self,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        (heap_key, metamethod_key): (StackValue, BytesObjectKey),
        dest: Register,
        value_a: StackValue,
        value_b: StackValue,
    ) -> Option<CallResult> {
        let function_key = heap.get_metamethod(heap_key, metamethod_key)?;

        let function_index = value_stack.len() - self.register_base;

        value_stack.extend([function_key, StackValue::Integer(2), value_a, value_b]);

        Some(CallResult::Call(
            function_index,
            ReturnMode::Destination(dest),
        ))
    }

    fn unary_number_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a): (Register, Register),
        metamethod_key: BytesObjectKey,
        generate_error: &dyn Fn(TypeName) -> RuntimeErrorData,
        primitive_operation: &dyn Fn(&Heap, StackValue) -> Result<StackValue, RuntimeErrorData>,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);

        if value_a.lives_in_heap() {
            return Ok(Some(
                self.unary_metamethod(heap, value_stack, metamethod_key, dest, value_a)
                    .ok_or_else(|| generate_error(value_a.type_name(heap)))?,
            ));
        }

        let result = primitive_operation(heap, value_a)?;

        value_stack.set(self.register_base + dest as usize, result);

        Ok(None)
    }

    fn unary_metamethod(
        &self,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        metamethod_key: BytesObjectKey,
        dest: Register,
        value: StackValue,
    ) -> Option<CallResult> {
        let function_key = heap.get_metamethod(value, metamethod_key)?;
        let function_index = value_stack.len() - self.register_base;

        value_stack.extend([function_key, StackValue::Integer(1), value]);

        Some(CallResult::Call(
            function_index,
            ReturnMode::Destination(dest),
        ))
    }

    fn copy_from_table(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        base: StackValue,
        key: StackValue,
        getter: impl Fn(&Table, StackValue) -> StackValue,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let mut value = match base {
            StackValue::Table(table_key) => {
                let Some(table) = exec_data.heap.get_table(table_key) else {
                    return Err(RuntimeErrorData::InvalidInternalState);
                };

                getter(table, key)
            }
            StackValue::Bytes(_) => StackValue::Nil,
            _ => {
                return Err(RuntimeErrorData::AttemptToIndex(
                    base.type_name(&exec_data.heap),
                ))
            }
        };

        if value == StackValue::Nil {
            // resolve using __index
            let metamethod_key = exec_data.metatable_keys.index.0.key();
            let max_chain_depth = exec_data.limits.metatable_chain_depth;
            let mut chain_depth = 0;

            let mut index_base = base;
            let mut next_index_base = index_base;

            while next_index_base != StackValue::Nil {
                index_base = next_index_base;

                match index_base {
                    StackValue::Table(table_key) => {
                        let Some(table) = exec_data.heap.get_table(table_key) else {
                            return Err(RuntimeErrorData::InvalidInternalState);
                        };

                        value = getter(table, key);

                        if value != StackValue::Nil {
                            break;
                        }
                    }
                    StackValue::NativeFunction(_) | StackValue::Function(_) => {
                        let function_index = value_stack.len() - self.register_base;
                        value_stack.extend([index_base, StackValue::Integer(2), base, key]);

                        return Ok(Some(CallResult::Call(
                            function_index,
                            ReturnMode::Destination(dest),
                        )));
                    }
                    StackValue::Bytes(_) => {}
                    StackValue::Nil
                    | StackValue::Bool(_)
                    | StackValue::Integer(_)
                    | StackValue::Float(_)
                    | StackValue::Pointer(_)
                    | StackValue::Coroutine(_) => {
                        return Err(RuntimeErrorData::AttemptToIndex(
                            index_base.type_name(&exec_data.heap),
                        ));
                    }
                };

                next_index_base = exec_data.heap.get_metavalue(index_base, metamethod_key);
                chain_depth += 1;

                if chain_depth > max_chain_depth {
                    return Err(RuntimeErrorData::MetatableIndexChainTooLong);
                }
            }
        }

        value_stack.set(self.register_base + dest as usize, value);

        Ok(None)
    }

    fn copy_to_table(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        table_stack_value: StackValue,
        key: StackValue,
        src: u8,
        setter: impl Fn(&mut Table, StackValue, StackValue),
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let StackValue::Table(table_key) = table_stack_value else {
            return Err(RuntimeErrorData::AttemptToIndex(
                table_stack_value.type_name(&exec_data.heap),
            ));
        };

        let heap = &mut exec_data.heap;
        let metamethod_key = exec_data.metatable_keys.newindex.0.key();
        let src_value = value_stack.get_deref(heap, self.register_base + src as usize);

        if let Some(function_key) = heap.get_table_metamethod(table_key, metamethod_key) {
            let function_index = value_stack.len() - self.register_base;

            value_stack.extend([
                function_key,
                StackValue::Integer(3),
                table_key.into(),
                key,
                src_value,
            ]);

            Ok(Some(CallResult::Call(
                function_index,
                ReturnMode::Static(0),
            )))
        } else {
            let gc = &mut exec_data.gc;
            let Some(table) = heap.get_table_mut(gc, table_key) else {
                return Err(RuntimeErrorData::InvalidInternalState);
            };

            let original_size = table.heap_size();

            setter(table, key, src_value);

            let new_size = table.heap_size();
            gc.modify_used_memory(new_size as isize - original_size as isize);

            if gc.should_step() {
                Ok(Some(CallResult::StepGc))
            } else {
                Ok(None)
            }
        }
    }

    fn copy_arg(&self, value_stack: &mut ValueStack, dest: Register, index: Register) {
        let arg_index = self.stack_start + 2 + index as usize;
        let dest_index = self.register_base + dest as usize;

        if arg_index >= self.register_base {
            // out of args, set nil
            value_stack.set(dest_index, Default::default());
        } else {
            let value = value_stack.get(arg_index);
            value_stack.set(dest_index, value);
        }
    }

    fn copy_args(&self, value_stack: &mut ValueStack, dest: Register, count: Register) {
        let dest_start = self.register_base + dest as usize;
        let count = count as usize;

        let arg_start_index = self.stack_start + 2;
        let arg_end_index = self.register_base.min(arg_start_index + count);

        // dest will always be greater than the arg source
        // since args are stored before the register base, and dest is stored after
        let end = dest_start + count;
        let slice = value_stack.get_slice_mut(0..end);

        slice.copy_within(arg_start_index..arg_end_index, dest_start);

        // set nil if there isn't enough args
        let copied = arg_end_index - arg_start_index;
        for value in &mut slice[dest_start + copied..dest_start + count] {
            *value = StackValue::Nil;
        }
    }

    fn copy_variadic(
        &self,
        value_stack: &mut ValueStack,
        dest: Register,
        count_register: Register,
        skip: Register,
    ) -> Result<(), RuntimeErrorData> {
        let dest_index = self.register_base + dest as usize;
        let arg_index = self.stack_start + 2 + skip as usize;
        let count_index = self.register_base + count_register as usize;

        let StackValue::Integer(count) = value_stack.get(count_index) else {
            return Err(IllegalInstruction::MissingVariadicCount.into());
        };

        let total = self.register_base.saturating_sub(arg_index);

        if total > 0 {
            // todo: recycle?
            let mut values = Vec::new();

            for i in 0..total {
                values.push(value_stack.get(arg_index + i));
            }

            for (i, value) in values.into_iter().enumerate() {
                value_stack.set(dest_index + i, value);
            }

            let count_value = StackValue::Integer(count + total as i64);
            value_stack.set(count_index, count_value);
        }

        Ok(())
    }

    fn copy_unsized_variadic(&self, value_stack: &mut ValueStack, dest: Register, skip: Register) {
        let dest_index = self.register_base + dest as usize;
        let arg_index = self.stack_start + 2 + skip as usize;

        let total = self.register_base.saturating_sub(arg_index);

        if total > 0 {
            // todo: recycle?
            let mut values = Vec::new();

            for i in 0..total {
                values.push(value_stack.get(arg_index + i));
            }

            for (i, value) in values.into_iter().enumerate() {
                value_stack.set(dest_index + i, value);
            }
        }
    }

    fn copy_range_to_deref(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        src: Register,
        count: Register,
    ) -> Result<(), RuntimeErrorData> {
        let gc = &mut exec_data.gc;
        let heap = &mut exec_data.heap;

        let src_start = self.register_base + src as usize;
        let dest_start = self.register_base + dest as usize;
        let count = count as usize;

        let end = src_start.max(dest_start) + count;
        let slice = value_stack.get_slice_mut(0..end);

        for i in 0..count {
            let dest_index = dest_start + i;
            let src_index = src_start + i;

            let value = slice[src_index].get_deref(heap);

            if let StackValue::Pointer(key) = slice[dest_index] {
                let Some(stored) = heap.get_stack_value_mut(gc, key) else {
                    crate::debug_unreachable!();
                    #[cfg(not(debug_assertions))]
                    return Err(RuntimeErrorData::InvalidInternalState);
                };

                *stored = value
            } else {
                slice[dest_index] = value;
            }
        }

        Ok(())
    }

    fn numeric_for(
        &mut self,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        mut for_loop_jump: bool,
        src: Register,
        forward_jump: u16,
    ) -> Result<bool, RuntimeErrorData> {
        let slice_start = self.register_base + src as usize;
        let values = value_stack.get_slice_mut(slice_start..slice_start + 4);

        let (value, limit, step) = (
            values[0].get_deref(heap),
            values[1].get_deref(heap),
            values[2].get_deref(heap),
        );

        if let (
            StackValue::Integer(mut value),
            StackValue::Integer(limit),
            StackValue::Integer(step),
        ) = (value, limit, step)
        {
            if for_loop_jump {
                value += step;
                for_loop_jump = false;
            }

            let stop = match step.is_positive() {
                true => value > limit,
                false => value < limit,
            };

            if stop {
                self.next_instruction_index += forward_jump as usize;
            } else {
                values[0] = StackValue::Integer(value);
                values[3] = StackValue::Integer(value);
            }
        } else {
            let mut value = cast_float(heap, value, |type_name| {
                RuntimeErrorData::InvalidForInitialValue(type_name)
            })?;
            let limit = cast_float(heap, limit, |type_name| {
                RuntimeErrorData::InvalidForLimit(type_name)
            })?;
            let step = cast_float(heap, step, |type_name| {
                RuntimeErrorData::InvalidForStep(type_name)
            })?;

            if for_loop_jump {
                value += step;
                for_loop_jump = false;
            }

            let stop = match step.is_sign_positive() {
                true => value > limit,
                false => value < limit,
            };

            if stop {
                self.next_instruction_index += forward_jump as usize;
            } else {
                values[0] = StackValue::Float(value);
                values[3] = StackValue::Float(value);
            }
        }

        Ok(for_loop_jump)
    }
}

fn stringify(heap: &Heap, value: StackValue) -> Option<Cow<'_, [u8]>> {
    match value {
        StackValue::Bytes(key) => {
            if let Some(bytes) = heap.get_bytes(key) {
                Some(Cow::Borrowed(bytes.as_bytes()))
            } else {
                crate::debug_unreachable!();
                #[cfg(not(debug_assertions))]
                None
            }
        }
        StackValue::Integer(i) => Some(i.to_string().into_bytes().into()),
        StackValue::Float(f) => Some(format!("{f:?}").into_bytes().into()),
        StackValue::Pointer(key) => {
            if let Some(value) = heap.get_stack_value(key) {
                if matches!(value, StackValue::Pointer(_)) {
                    crate::debug_unreachable!();
                    #[cfg(not(debug_assertions))]
                    return None;
                }

                stringify(heap, *value)
            } else {
                crate::debug_unreachable!();
                #[cfg(not(debug_assertions))]
                None
            }
        }
        _ => None,
    }
}

fn cast_integer(
    heap: &Heap,
    value: StackValue,
    generate_err: impl FnOnce(TypeName) -> RuntimeErrorData,
) -> Result<i64, RuntimeErrorData> {
    match value {
        StackValue::Float(float) => {
            coerce_integer(float).ok_or(RuntimeErrorData::NoIntegerRepresentation)
        }
        StackValue::Integer(int) => Ok(int),
        _ => Err(generate_err(value.type_name(heap))),
    }
}

fn cast_float(
    heap: &Heap,
    value: StackValue,
    generate_err: impl FnOnce(TypeName) -> RuntimeErrorData,
) -> Result<f64, RuntimeErrorData> {
    match value {
        StackValue::Integer(int) => Ok(int as f64),
        StackValue::Float(float) => Ok(float),
        _ => Err(generate_err(value.type_name(heap))),
    }
}

fn arithmetic_cast_float(heap: &Heap, value: StackValue) -> Result<f64, RuntimeErrorData> {
    cast_float(heap, value, |type_name| {
        RuntimeErrorData::InvalidArithmetic(type_name)
    })
}

fn arithmetic_cast_integer(heap: &Heap, value: StackValue) -> Result<i64, RuntimeErrorData> {
    cast_integer(heap, value, |type_name| {
        RuntimeErrorData::InvalidArithmetic(type_name)
    })
}

// resolves __call metamethod chains and stack values promoted to heap values
fn resolve_call(
    exec_data: &mut ExecutionAccessibleData,
    mut value: StackValue,
    mut prepend_arg: impl FnMut(&mut Heap, StackValue),
) -> Result<StackValue, RuntimeErrorData> {
    let call_key = exec_data.metatable_keys.call.0.key();
    let max_chain_depth = exec_data.limits.metatable_chain_depth;
    let mut chain_depth = 0;

    loop {
        match value {
            StackValue::Function(_) | StackValue::NativeFunction(_) => return Ok(value),
            StackValue::Bytes(_) | StackValue::Table(_) => {}
            _ => {
                return Err(RuntimeErrorData::InvalidCall(
                    value.type_name(&exec_data.heap),
                ))
            }
        };

        let next_value = exec_data.heap.get_metavalue(value, call_key);
        prepend_arg(&mut exec_data.heap, value);
        value = next_value;

        chain_depth += 1;

        if chain_depth > max_chain_depth {
            return Err(RuntimeErrorData::MetatableCallChainTooLong);
        }
    }
}

enum ValueOrCallResult<T> {
    Value(T),
    CallResult(CallResult),
}
