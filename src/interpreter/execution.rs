use super::coroutine::Continuation;
use super::heap::{Heap, HeapKey, HeapValue};
use super::instruction::{Instruction, Register, ReturnMode};
use super::interpreted_function::{Function, FunctionDefinition};
use super::multi::MultiValue;
use super::value_stack::{StackValue, ValueStack};
use super::vm::{ExecutionAccessibleData, Vm};
use super::{TypeName, UpValueSource, Value};
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData, StackTrace};
use crate::languages::lua::coerce_integer;
use std::borrow::Cow;
use std::rc::Rc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

enum CallResult {
    Call(usize, ReturnMode),
    Return(usize),
    StepGc,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct ExecutionContext {
    pub(crate) call_stack: Vec<CallContext>,
    pub(crate) value_stack: ValueStack,
}

impl ExecutionContext {
    pub(crate) fn new_function_call(
        function_key: HeapKey,
        function: Function,
        args: MultiValue,
        vm: &mut Vm,
    ) -> Self {
        let exec_data = &mut vm.execution_data;
        let mut value_stack = exec_data.cache_pools.create_value_stack();

        value_stack.push(function_key.into());
        args.push_stack_multi(&mut value_stack);
        exec_data.cache_pools.store_multi(args);

        let call_stack = vec![CallContext {
            up_values: function
                .up_values
                .clone_using(&exec_data.cache_pools.short_value_stacks),
            function_definition: function.definition.clone(),
            next_instruction_index: 0,
            stack_start: 0,
            register_base: value_stack.len(),
            return_mode: ReturnMode::Multi,
        }];

        Self {
            call_stack,
            value_stack,
        }
    }

    pub(crate) fn new_value_call(
        value: StackValue,
        mut args: MultiValue,
        vm: &mut Vm,
    ) -> Result<Self, RuntimeErrorData> {
        let exec_data = &mut vm.execution_data;

        let function_key = resolve_call(exec_data, value, |heap, value| {
            args.push_front(Value::from_stack_value(heap, value));
        })?;

        let mut call_stack = Vec::new();
        let mut value_stack = exec_data.cache_pools.create_value_stack();

        match exec_data.heap.get(function_key).unwrap() {
            HeapValue::NativeFunction(function) => {
                let results = match function.shallow_clone().call(args, &mut vm.context()) {
                    Ok(result) => result,
                    Err(err) => {
                        vm.execution_data.cache_pools.store_value_stack(value_stack);
                        return Err(err.data);
                    }
                };
                results.push_stack_multi(&mut value_stack);
            }
            HeapValue::Function(function) => {
                value_stack.push(value);
                args.push_stack_multi(&mut value_stack);

                let call_context = CallContext {
                    up_values: function
                        .up_values
                        .clone_using(&exec_data.cache_pools.short_value_stacks),
                    function_definition: function.definition.clone(),
                    next_instruction_index: 0,
                    stack_start: 0,
                    register_base: value_stack.len(),
                    return_mode: ReturnMode::Multi,
                };
                call_stack.push(call_context);

                exec_data.cache_pools.store_multi(args);
            }
            _ => unreachable!(),
        }

        Ok(Self {
            call_stack,
            value_stack,
        })
    }

    pub(crate) fn resume(vm: &mut Vm) -> Result<MultiValue, RuntimeError> {
        let mut execution = vm.execution_stack.last_mut().unwrap();

        let mut last_definition = None;

        let mut exec_data = &mut vm.execution_data;

        while let Some(call) = execution.call_stack.last_mut() {
            let result = match call.resume(&mut execution.value_stack, exec_data) {
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

                    let stack_start = call.register_base + registry_index;

                    let function_value = execution.value_stack.get(stack_start);
                    let StackValue::HeapValue(heap_key) = function_value else {
                        let type_name = function_value.type_name(&exec_data.heap);
                        return Err(Self::unwind_error(
                            vm,
                            RuntimeErrorData::InvalidCall(type_name),
                        ));
                    };

                    let StackValue::Integer(mut arg_count) =
                        execution.value_stack.get(stack_start + 1)
                    else {
                        return Err(Self::unwind_error(
                            vm,
                            IllegalInstruction::MissingArgCount.into(),
                        ));
                    };

                    let function_key = resolve_call(exec_data, heap_key.into(), |_, value| {
                        execution.value_stack.insert(stack_start + 2, value);

                        arg_count += 1;
                        execution
                            .value_stack
                            .set(stack_start + 1, StackValue::Integer(arg_count));
                    });

                    let function_key = match function_key {
                        Ok(key) => key,
                        Err(err) => return Err(Self::unwind_error(vm, err)),
                    };

                    let function_value = exec_data.heap.get(function_key);

                    match function_value {
                        Some(HeapValue::NativeFunction(callback)) => {
                            let callback = callback.shallow_clone();

                            // load args
                            let mut args = exec_data.cache_pools.create_multi();

                            if let Err(err) = args.copy_stack_multi(
                                &mut exec_data.heap,
                                &mut execution.value_stack,
                                stack_start + 1,
                                IllegalInstruction::MissingArgCount,
                            ) {
                                return Err(Self::unwind_error(vm, err.into()));
                            };

                            // handle tail call
                            let mut return_mode = return_mode;
                            let mut stack_start = stack_start;

                            if return_mode == ReturnMode::TailCall && execution.call_stack.len() > 1
                            {
                                // if the callstack == 0 we retain the TailCall return mode and handle it later

                                // remove caller and recycle value stacks
                                let call = execution.call_stack.pop().unwrap();
                                exec_data
                                    .cache_pools
                                    .store_short_value_stack(call.up_values);

                                // adopt caller's return mode and stack placement
                                return_mode = call.return_mode;
                                stack_start = call.stack_start;
                                execution.value_stack.chip(stack_start, 0);
                            }

                            // update tracked stack size in case the native function calls an interpreted function
                            let old_stack_size = exec_data.tracked_stack_size;
                            exec_data.tracked_stack_size =
                                old_stack_size + execution.value_stack.len();

                            // call the function
                            let result = callback.call(args, &mut vm.context());

                            // revert tracked stack size before handling the result
                            vm.execution_data.tracked_stack_size = old_stack_size;

                            let mut return_values = match result {
                                Ok(values) => values,
                                Err(mut err) => {
                                    // handle yielding
                                    if let RuntimeErrorData::Yield(_) = &mut err.data {
                                        if !vm.coroutine_data.yield_permissions.allows_yield {
                                            // we can't yield here
                                            err.data = RuntimeErrorData::InvalidYield;
                                            return Err(Self::continue_unwind(vm, err));
                                        }

                                        let execution = vm.execution_stack.pop().unwrap();

                                        vm.coroutine_data.in_progress_yield.push((
                                            Continuation::Execution {
                                                execution,
                                                return_mode,
                                                stack_start,
                                            },
                                            true,
                                        ));

                                        return Err(err);
                                    } else {
                                        return Err(Self::continue_unwind(vm, err));
                                    }
                                }
                            };

                            // juggling lifetimes
                            execution = vm.execution_stack.last_mut().unwrap();
                            exec_data = &mut vm.execution_data;

                            match return_mode {
                                ReturnMode::TailCall => {
                                    // we retained the TailCall return mode, this is our final return
                                    let execution = vm.execution_stack.pop().unwrap();
                                    exec_data
                                        .cache_pools
                                        .store_value_stack(execution.value_stack);

                                    // directly returning values instead of temporarily storing in value stack
                                    return Ok(return_values);
                                }
                                _ => {
                                    if let Err(err) = execution.handle_external_return(
                                        return_mode,
                                        stack_start,
                                        &mut return_values,
                                    ) {
                                        return Err(Self::unwind_error(vm, err));
                                    }
                                }
                            }

                            exec_data.cache_pools.store_multi(return_values);
                        }
                        Some(HeapValue::Function(func)) => {
                            if return_mode == ReturnMode::TailCall {
                                // transform the caller
                                call.up_values.clone_from(&func.up_values);
                                call.function_definition = func.definition.clone();
                                call.next_instruction_index = 0;
                                call.register_base = call.stack_start + 2 + arg_count as usize;

                                execution.value_stack.chip(
                                    call.stack_start,
                                    execution.value_stack.len() - stack_start,
                                );
                            } else {
                                let new_call = CallContext {
                                    up_values: func
                                        .up_values
                                        .clone_using(&exec_data.cache_pools.short_value_stacks),
                                    function_definition: func.definition.clone(),
                                    next_instruction_index: 0,
                                    stack_start,
                                    register_base: stack_start + 2 + arg_count as usize,
                                    return_mode,
                                };
                                execution.call_stack.push(new_call);
                            }
                        }
                        _ => {
                            let type_name = function_value
                                .map(|value| value.type_name(&exec_data.heap))
                                .unwrap_or(TypeName::Nil);

                            return Err(Self::unwind_error(
                                vm,
                                RuntimeErrorData::InvalidCall(type_name),
                            ));
                        }
                    }
                }
                CallResult::Return(registry_index) => {
                    let call = execution.call_stack.pop().unwrap();
                    let stack_index = call.register_base + registry_index;
                    let parent_base = execution
                        .call_stack
                        .last()
                        .map(|call| call.register_base)
                        .unwrap_or_default();

                    // get return count
                    let StackValue::Integer(return_count) = execution.value_stack.get(stack_index)
                    else {
                        // put the call back
                        execution.call_stack.push(call);

                        return Err(Self::unwind_error(
                            vm,
                            IllegalInstruction::MissingReturnCount.into(),
                        ));
                    };

                    // recycle value stacks
                    exec_data
                        .cache_pools
                        .store_short_value_stack(call.up_values);

                    let mut return_count = return_count as usize;

                    match call.return_mode {
                        ReturnMode::Multi => {
                            // remove extra values past the return values
                            execution
                                .value_stack
                                .chip(call.register_base + registry_index + return_count + 1, 0);

                            // chip under the return values and count
                            execution
                                .value_stack
                                .chip(call.stack_start, return_count + 1);
                        }
                        ReturnMode::Static(expected_count) => {
                            return_count = return_count.min(expected_count as _);

                            // remove extra values past the return values
                            execution
                                .value_stack
                                .chip(call.register_base + registry_index + return_count + 1, 0);

                            // chip under the return values
                            execution.value_stack.chip(call.stack_start, return_count);
                        }
                        ReturnMode::Destination(dest) => {
                            // copy value
                            let value = execution
                                .value_stack
                                .get(call.register_base + registry_index + 1);

                            // remove all values
                            execution.value_stack.chip(call.stack_start, 0);

                            // store value
                            execution
                                .value_stack
                                .set(parent_base + dest as usize, value);
                        }
                        ReturnMode::Extend(len_index) => {
                            // remove extra values past the return values
                            execution
                                .value_stack
                                .chip(call.register_base + registry_index + return_count + 1, 0);

                            // get the return count
                            let len_register = parent_base + len_index as usize;
                            let StackValue::Integer(stored_return_count) =
                                execution.value_stack.get(len_register)
                            else {
                                return Err(Self::unwind_error(
                                    vm,
                                    IllegalInstruction::MissingReturnCount.into(),
                                ));
                            };

                            // chip under the return values
                            execution.value_stack.chip(call.stack_start, return_count);
                            // add the return count
                            let count = stored_return_count + return_count as i64 - 1;
                            let count_value = StackValue::Integer(count);
                            execution.value_stack.set(len_register, count_value);
                        }
                        ReturnMode::UnsizedDestinationPreserve(dest) => {
                            let mut values = exec_data.cache_pools.create_short_value_stack();

                            let start = stack_index + 1;
                            for value in
                                execution.value_stack.get_slice(start..start + return_count)
                            {
                                values.push(*value);
                            }

                            let dest_index = parent_base + dest as usize;
                            execution.value_stack.chip(dest_index, 0);
                            execution
                                .value_stack
                                .extend(values.get_slice(0..return_count).iter().cloned());
                            exec_data.cache_pools.store_short_value_stack(values);
                        }
                        ReturnMode::TailCall => unreachable!(),
                    }

                    last_definition = Some(call.function_definition);
                }
                CallResult::StepGc => {
                    exec_data.gc.step(
                        &exec_data.metatable_keys,
                        &exec_data.cache_pools,
                        &vm.execution_stack,
                        &mut exec_data.heap,
                    );

                    execution = vm.execution_stack.last_mut().unwrap();
                }
            }
        }

        let mut return_values = exec_data.cache_pools.create_multi();

        if let Some(definition) = last_definition {
            return_values
                .copy_stack_multi(
                    &mut exec_data.heap,
                    &mut execution.value_stack,
                    0,
                    IllegalInstruction::MissingReturnCount,
                )
                .map_err(|err| {
                    let instruction_index = definition.instructions.len().saturating_sub(1);
                    let mut trace = StackTrace::default();
                    let frame = definition.create_stack_trace_frame(instruction_index);
                    trace.push_frame(frame);

                    RuntimeError {
                        data: err.into(),
                        trace,
                    }
                })?;
        }

        let context = vm.execution_stack.pop().unwrap();
        exec_data.cache_pools.store_value_stack(context.value_stack);

        Ok(return_values)
    }

    pub(crate) fn handle_external_return(
        &mut self,
        return_mode: ReturnMode,
        stack_start: usize,
        return_values: &mut MultiValue,
    ) -> Result<(), RuntimeErrorData> {
        match return_mode {
            ReturnMode::Multi => {
                self.value_stack.chip(stack_start, 0);
                return_values.push_stack_multi(&mut self.value_stack);
            }
            ReturnMode::Static(return_count) => {
                self.value_stack.chip(stack_start, 0);

                self.value_stack.extend(
                    std::iter::from_fn(|| return_values.pop_front())
                        .map(|v| v.to_stack_value())
                        .chain(std::iter::repeat(StackValue::default()))
                        .take(return_count as usize),
                );
            }
            ReturnMode::Destination(dest) => {
                self.value_stack.chip(stack_start, 0);

                let value = return_values.pop_front().unwrap_or_default();

                let parent_register_base = self.call_stack.last().unwrap().register_base;
                let dest_index = parent_register_base + dest as usize;
                self.value_stack.set(dest_index, value.to_stack_value());
            }
            ReturnMode::Extend(len_index) => {
                self.value_stack.chip(stack_start, 0);

                let return_count = return_values.len();

                // append return values
                self.value_stack.extend(std::iter::from_fn(|| {
                    return_values.pop_front().map(|v| v.to_stack_value())
                }));

                // add the return count
                let parent_register_base = self.call_stack.last().unwrap().register_base;
                let len_register = parent_register_base + len_index as usize;

                let StackValue::Integer(stored_return_count) = self.value_stack.get(len_register)
                else {
                    return Err(IllegalInstruction::MissingReturnCount.into());
                };

                let count = stored_return_count + return_count as i64 - 1;
                let count_value = StackValue::Integer(count);
                self.value_stack.set(len_register, count_value);
            }
            ReturnMode::UnsizedDestinationPreserve(dest) => {
                let parent_register_base = self.call_stack.last().unwrap().register_base;
                let dest_index = parent_register_base + dest as usize;

                // clear everything at the destination and beyond before placing values
                self.value_stack.chip(dest_index, 0);

                let return_count = return_values.len();

                for i in 0..return_count {
                    let Some(value) = return_values.pop_front() else {
                        break;
                    };

                    self.value_stack.set(dest_index + i, value.to_stack_value());
                }
            }
            ReturnMode::TailCall => {
                // we're going to assume this is the final call
                // the return mode should've been resolved to something else earlier
                self.value_stack.chip(2, 0);
                return_values.push_stack_multi(&mut self.value_stack)
            }
        }

        Ok(())
    }

    pub(crate) fn unwind_error(vm: &mut Vm, data: RuntimeErrorData) -> RuntimeError {
        Self::continue_unwind(
            vm,
            RuntimeError {
                trace: StackTrace::default(),
                data,
            },
        )
    }

    pub(crate) fn continue_unwind(vm: &mut Vm, mut err: RuntimeError) -> RuntimeError {
        let execution = vm.execution_stack.pop().unwrap();

        let exec_data = &mut vm.execution_data;

        // recycle value stacks
        for call in execution.call_stack.into_iter().rev() {
            let instruction_index = call.next_instruction_index.saturating_sub(1);
            let definition = call.function_definition;
            let frame = definition.create_stack_trace_frame(instruction_index);
            err.trace.push_frame(frame);

            exec_data
                .cache_pools
                .store_short_value_stack(call.up_values);
        }

        exec_data
            .cache_pools
            .store_value_stack(execution.value_stack);

        err
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct CallContext {
    pub(crate) up_values: ValueStack,
    #[cfg_attr(feature = "serde", serde(with = "super::serde_function_definition_rc"))]
    pub(crate) function_definition: Rc<FunctionDefinition>,
    pub(crate) next_instruction_index: usize,
    pub(crate) stack_start: usize,
    pub(crate) register_base: usize,
    pub(crate) return_mode: ReturnMode,
}

impl CallContext {
    fn resume(
        &mut self,
        value_stack: &mut ValueStack,
        exec_data: &mut ExecutionAccessibleData,
    ) -> Result<CallResult, RuntimeErrorData> {
        let definition = &self.function_definition;
        let mut for_loop_jump = false;

        while let Some(&instruction) = definition.instructions.get(self.next_instruction_index) {
            if exec_data.tracked_stack_size + value_stack.len() > exec_data.limits.stack_size {
                return Err(RuntimeErrorData::StackOverflow);
            }

            #[cfg(feature = "instruction_exec_counts")]
            exec_data.instruction_counter.track(instruction);

            let gc = &mut exec_data.gc;
            let heap = &mut exec_data.heap;
            self.next_instruction_index += 1;

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
                Instruction::LoadInt(dest, index) => {
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = StackValue::Integer(*number);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadFloat(dest, index) => {
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = StackValue::Float(f64::from_bits(*number as u64));
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadBytes(dest, index) => {
                    let Some(&heap_key) = definition.byte_strings.get(index as usize) else {
                        return Err(IllegalInstruction::MissingByteStringConstant(index).into());
                    };

                    value_stack.set(self.register_base + dest as usize, heap_key.into());
                }
                Instruction::ClearFrom(dest) => {
                    let dest_index = self.register_base + dest as usize;
                    value_stack.chip(dest_index, 0);
                }
                Instruction::PrepMulti(dest, index) => {
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = StackValue::Integer(*number);

                    let dest_index = self.register_base + dest as usize;
                    value_stack.set(dest_index, value);
                    value_stack.chip(dest_index + 1, 0);
                }
                Instruction::CreateTable(dest, len_index) => {
                    let Some(&len) = definition.numbers.get(len_index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(len_index).into());
                    };

                    let heap_key = heap.create_table(gc, len as _, 0);

                    value_stack.set(self.register_base + dest as usize, heap_key.into());

                    if gc.should_step() {
                        return Ok(CallResult::StepGc);
                    }
                }
                Instruction::FlushToTable(dest, total, index_offset) => {
                    let err = RuntimeErrorData::InvalidRef;

                    // get the table
                    let dest_index = self.register_base + dest as usize;

                    let StackValue::HeapValue(heap_key) = value_stack.get(dest_index) else {
                        return Err(err.clone());
                    };

                    let Some(table_value) = heap.get_mut(gc, heap_key) else {
                        return Err(err.clone());
                    };

                    let HeapValue::Table(table) = table_value else {
                        return Err(err.clone());
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
                        return Ok(CallResult::StepGc);
                    }
                }
                Instruction::VariadicToTable(dest, src_start, index_offset) => {
                    let table_err = RuntimeErrorData::InvalidRef;

                    // grab the table
                    let dest_index = self.register_base + dest as usize;

                    let StackValue::HeapValue(heap_key) = value_stack.get(dest_index) else {
                        return Err(table_err.clone());
                    };

                    let Some(table_value) = heap.get_mut(gc, heap_key) else {
                        return Err(table_err.clone());
                    };

                    let HeapValue::Table(table) = table_value else {
                        return Err(table_err.clone());
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

                    // grab the count
                    let count_index = dest_index + 1;
                    let StackValue::Integer(count) = value_stack.get(count_index) else {
                        return Err(table_err.clone());
                    };

                    let start = self.register_base + src_start as usize;
                    let end = start + count as usize;

                    let original_size = table.heap_size();

                    table.reserve_list(count as usize);
                    table.flush(index_offset, value_stack.get_slice(start..end));

                    let new_size = table.heap_size();
                    gc.modify_used_memory(new_size as isize - original_size as isize);

                    if gc.should_step() {
                        return Ok(CallResult::StepGc);
                    }
                }
                Instruction::CopyTableField(dest, table_index) => {
                    // resolve field key
                    let Some(Instruction::Constant(bytes_index)) =
                        definition.instructions.get(self.next_instruction_index)
                    else {
                        return Err(IllegalInstruction::ExpectingConstant.into());
                    };
                    self.next_instruction_index += 1;

                    let Some(&heap_key) = definition.byte_strings.get(*bytes_index as usize) else {
                        return Err(
                            IllegalInstruction::MissingByteStringConstant(*bytes_index).into()
                        );
                    };

                    // table
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);

                    if let Some(call_result) =
                        self.copy_from_table(exec_data, value_stack, dest, base, heap_key.into())?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyToTableField(table_index, src) => {
                    // resolve field key
                    let Some(Instruction::Constant(bytes_index)) =
                        definition.instructions.get(self.next_instruction_index)
                    else {
                        return Err(IllegalInstruction::ExpectingConstant.into());
                    };
                    self.next_instruction_index += 1;

                    let Some(&heap_key) = definition.byte_strings.get(*bytes_index as usize) else {
                        return Err(
                            IllegalInstruction::MissingByteStringConstant(*bytes_index).into()
                        );
                    };

                    // table
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);

                    if let Some(call_result) =
                        self.copy_to_table(exec_data, value_stack, base, heap_key.into(), src)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyTableValue(dest, table_index, key_index) => {
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);
                    let key = value_stack.get_deref(heap, self.register_base + key_index as usize);

                    if let Some(call_result) =
                        self.copy_from_table(exec_data, value_stack, dest, base, key)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyToTableValue(table_index, key_index, src) => {
                    let base =
                        value_stack.get_deref(heap, self.register_base + table_index as usize);
                    let key = value_stack.get_deref(heap, self.register_base + key_index as usize);

                    if let Some(call_result) =
                        self.copy_to_table(exec_data, value_stack, base, key, src)?
                    {
                        return Ok(call_result);
                    }
                }
                Instruction::CopyArg(dest, index) => {
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
                Instruction::CopyArgs(dest, count) => {
                    self.copy_args(value_stack, dest, count);
                }
                Instruction::CopyVariadic(dest, count_register, skip) => {
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
                }
                Instruction::CopyUnsizedVariadic(dest, skip) => {
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
                Instruction::Closure(dest, function_index) => {
                    let Some(&heap_key) = definition.functions.get(function_index as usize) else {
                        return Err(RuntimeErrorData::IllegalInstruction(
                            IllegalInstruction::MissingFunctionConstant(function_index),
                        ));
                    };

                    let HeapValue::Function(func) = heap.get(heap_key).unwrap() else {
                        unreachable!()
                    };

                    if func.definition.up_values.is_empty() {
                        value_stack.set(self.register_base + dest as usize, heap_key.into());
                    } else {
                        // copy the function to pass captures
                        let mut func = func.clone();

                        // resolve captures
                        let mut up_values = exec_data.cache_pools.create_short_value_stack();

                        for capture_source in &func.definition.up_values {
                            let value = match capture_source {
                                UpValueSource::Stack(src) => {
                                    let src_index = self.register_base + *src as usize;
                                    let mut value = value_stack.get(src_index);

                                    if !matches!(value, StackValue::Pointer(_)) {
                                        // move the stack value to the heap
                                        let heap_key =
                                            heap.create(gc, HeapValue::StackValue(value));
                                        value = StackValue::Pointer(heap_key);
                                        value_stack.set(src_index, value);
                                    }

                                    value
                                }
                                UpValueSource::UpValue(src) => {
                                    let src_index = *src as usize;
                                    let mut value = self.up_values.get(src_index);

                                    if !matches!(value, StackValue::Pointer(_)) {
                                        // move the stack value to the heap
                                        let heap_key =
                                            heap.create(gc, HeapValue::StackValue(value));
                                        value = StackValue::Pointer(heap_key);
                                        self.up_values.set(src_index, value);
                                    }

                                    value
                                }
                            };

                            up_values.push(value);
                        }

                        func.up_values = Rc::new(up_values);

                        // store the new function
                        let heap_key = heap.create(gc, HeapValue::Function(func));
                        value_stack.set(self.register_base + dest as usize, heap_key.into());

                        if gc.should_step() {
                            return Ok(CallResult::StepGc);
                        }
                    }
                }
                Instruction::ClearUpValue(dest) => {
                    self.up_values.set(dest as usize, StackValue::Nil);
                }
                Instruction::CopyUpValue(dest, src) => {
                    let value = self.up_values.get_deref(heap, src as usize);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::CopyToUpValue(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);
                    self.up_values.set(dest as usize, value);
                }
                Instruction::CopyToUpValueDeref(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);

                    if let StackValue::Pointer(heap_key) = self.up_values.get(dest as usize) {
                        // pointing to another stack value
                        heap.set(gc, heap_key, HeapValue::StackValue(value));
                    } else {
                        self.up_values.set(dest as usize, value);
                    }
                }
                Instruction::Copy(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::CopyToDeref(dest, src) => {
                    let value = value_stack.get_deref(heap, self.register_base + src as usize);
                    let dest_index = self.register_base + dest as usize;

                    if let StackValue::Pointer(heap_key) = value_stack.get(dest_index) {
                        heap.set(gc, heap_key, HeapValue::StackValue(value));
                    } else {
                        value_stack.set(dest_index, value);
                    }
                }
                Instruction::CopyRangeToDeref(dest, src, count) => {
                    self.copy_range_to_deref(exec_data, value_stack, dest, src, count);
                }
                Instruction::Not(dest, src) => {
                    let value = !value_stack.is_truthy(self.register_base + src as usize);
                    value_stack.set(self.register_base + dest as usize, StackValue::Bool(value));
                }
                Instruction::Len(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.len.0.key().into();

                    let value_a = value_stack.get_deref(heap, self.register_base + src as usize);

                    let StackValue::HeapValue(heap_key) = value_a else {
                        return Err(RuntimeErrorData::NoLength(value_a.type_name(heap)));
                    };

                    // default behavior
                    let heap_value = heap.get(heap_key).unwrap();
                    let len = match heap_value {
                        HeapValue::Table(table) => table.list_len(),
                        HeapValue::Bytes(bytes) => bytes.len(),
                        _ => return Err(RuntimeErrorData::NoLength(heap_value.type_name(heap))),
                    };

                    // try metamethod before using the default value
                    if matches!(heap_value, HeapValue::Table(_)) {
                        if let Some(call_result) = self.unary_metamethod(
                            heap,
                            value_stack,
                            (heap_key, metamethod_key),
                            (dest, value_a),
                        ) {
                            return Ok(call_result);
                        }
                    }

                    let value = StackValue::Integer(len as i64);
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::UnaryMinus(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.unm.0.key().into();

                    if let Some(call_result) = self.unary_number_operation(
                        (heap, value_stack),
                        (dest, src),
                        metamethod_key,
                        |type_name| RuntimeErrorData::InvalidArithmetic(type_name),
                        |heap, value| match value {
                            StackValue::Integer(n) => Ok(StackValue::Integer(-n)),
                            StackValue::Float(n) => Ok(StackValue::Float(-n)),
                            _ => Err(RuntimeErrorData::InvalidArithmetic(value.type_name(heap))),
                        },
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseNot(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.bnot.0.key().into();

                    if let Some(call_result) = self.unary_number_operation(
                        (heap, value_stack),
                        (dest, src),
                        metamethod_key,
                        |type_name| RuntimeErrorData::InvalidArithmetic(type_name),
                        |heap, value| {
                            Ok(StackValue::Integer(-arithmetic_cast_integer(heap, value)?))
                        },
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Add(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.add.0.key().into();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a + b,
                        |a, b| a + b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Subtract(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.sub.0.key().into();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a - b,
                        |a, b| a - b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Multiply(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.mul.0.key().into();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a * b,
                        |a, b| a * b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Division(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.div.0.key().into();

                    if let Some(call_result) = self.binary_float_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a / b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::IntegerDivision(dest, a, b) => {
                    let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
                    let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

                    let value = match (value_a, value_b) {
                        (StackValue::Integer(a), StackValue::Integer(b)) => {
                            if b == 0 {
                                return Err(RuntimeErrorData::DivideByZero);
                            }
                            StackValue::Integer(a / b)
                        }
                        (StackValue::Float(a), StackValue::Float(b)) => StackValue::Float(a / b),
                        (StackValue::Float(a), StackValue::Integer(b)) => {
                            StackValue::Float((a / b as f64).trunc())
                        }
                        (StackValue::Integer(a), StackValue::Float(b)) => {
                            StackValue::Float((a as f64 / b).trunc())
                        }
                        _ => {
                            let metamethod_key = exec_data.metatable_keys.idiv.0.key().into();

                            return self
                                .try_binary_metamethods(
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
                                    _ => {
                                        RuntimeErrorData::InvalidArithmetic(value_a.type_name(heap))
                                    }
                                });
                        }
                    };

                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::Modulus(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.modulus.0.key().into();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a % b,
                        |a, b| a % b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Power(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.pow.0.key().into();

                    if let Some(call_result) = self.binary_float_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a.powf(b),
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseAnd(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.band.0.key().into();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a & b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseOr(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.bor.0.key().into();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a | b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitwiseXor(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.bxor.0.key().into();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a ^ b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitShiftLeft(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.shl.0.key().into();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a << b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::BitShiftRight(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.shr.0.key().into();

                    if let Some(call_result) = self.binary_integer_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a >> b,
                    )? {
                        return Ok(call_result);
                    }
                }
                Instruction::Equal(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.eq.0.key().into();

                    let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
                    let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

                    if let Some(call_result) = self.try_binary_metamethods(
                        (heap, value_stack),
                        metamethod_key,
                        dest,
                        value_a,
                        value_b,
                    ) {
                        return Ok(call_result);
                    }

                    let equal = match (value_a, value_b) {
                        (StackValue::Float(float), StackValue::Integer(int))
                        | (StackValue::Integer(int), StackValue::Float(float)) => {
                            int as f64 == float
                        }
                        _ => value_a == value_b,
                    };

                    value_stack.set(self.register_base + dest as usize, StackValue::Bool(equal));
                }
                Instruction::LessThan(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.lt.0.key().into();

                    if let Some(call_result) = self.comparison_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a < b,
                        |a, b| a < b,
                        |a, b| {
                            if let (HeapValue::Bytes(bytes_a), HeapValue::Bytes(bytes_b)) = (a, b) {
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
                    let metamethod_key = exec_data.metatable_keys.le.0.key().into();

                    if let Some(call_result) = self.comparison_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a <= b,
                        |a, b| a <= b,
                        |a, b| {
                            if let (HeapValue::Bytes(bytes_a), HeapValue::Bytes(bytes_b)) = (a, b) {
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
                    let metamethod_key = exec_data.metatable_keys.concat.0.key();

                    let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
                    let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

                    // default behavior
                    let string_a = stringify(heap, value_a);
                    let string_b = stringify(heap, value_b);

                    if let (Some(string_a), Some(string_b)) = (string_a, string_b) {
                        let mut bytes = Vec::<u8>::with_capacity(string_a.len() + string_b.len());

                        bytes.extend(string_a.iter());
                        bytes.extend(string_b.iter());
                        let heap_key = heap.intern_bytes(gc, &bytes);
                        value_stack.set(self.register_base + dest as usize, heap_key.into());

                        if gc.should_step() {
                            return Ok(CallResult::StepGc);
                        }
                    } else {
                        // try metamethod as a fallback using the default value
                        let call_result = self
                            .try_binary_metamethods(
                                (heap, value_stack),
                                metamethod_key.into(),
                                dest,
                                value_a,
                                value_b,
                            )
                            .ok_or_else(|| {
                                let type_name_a = value_a.type_name(heap);

                                match type_name_a {
                                    TypeName::String => {
                                        RuntimeErrorData::AttemptToConcat(value_b.type_name(heap))
                                    }
                                    _ => RuntimeErrorData::AttemptToConcat(type_name_a),
                                }
                            })?;

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
                Instruction::NumericFor(src, local) => {
                    let limit = coerce_stack_value_to_integer(
                        heap,
                        value_stack.get(self.register_base + src as usize),
                        |type_name| RuntimeErrorData::InvalidForLimit(type_name),
                    )?;
                    let step = coerce_stack_value_to_integer(
                        heap,
                        value_stack.get(self.register_base + src as usize + 1),
                        |type_name| RuntimeErrorData::InvalidForStep(type_name),
                    )?;
                    let mut value = coerce_stack_value_to_integer(
                        heap,
                        value_stack.get(self.register_base + local as usize),
                        |type_name| RuntimeErrorData::InvalidForInitialValue(type_name),
                    )?;

                    if for_loop_jump {
                        value += step;
                        for_loop_jump = false;
                    }

                    let stop = match step.is_positive() {
                        true => value > limit,
                        false => value < limit,
                    };

                    if !stop {
                        value_stack.set(
                            self.register_base + local as usize,
                            StackValue::Integer(value),
                        );
                        self.next_instruction_index += 1;
                    }
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

    /// Resolves stack value pointers, calls metamethods for heap values, calls coerce functions if necessary
    #[allow(clippy::too_many_arguments)]
    fn resolve_binary_operand<T>(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        metamethod_key: StackValue,
        dest: Register,
        value_a: StackValue,
        value_b: StackValue,
        value: StackValue,
        coerce_value: impl Fn(&Heap, StackValue) -> Result<T, RuntimeErrorData>,
    ) -> Result<ValueOrCallResult<T>, RuntimeErrorData> {
        match value {
            StackValue::HeapValue(heap_key) => {
                match self.binary_metamethod(
                    heap,
                    value_stack,
                    (heap_key, metamethod_key),
                    dest,
                    value_a,
                    value_b,
                ) {
                    Some(call_result) => Ok(ValueOrCallResult::CallResult(call_result)),
                    None => Err(RuntimeErrorData::InvalidArithmetic(value.type_name(heap))),
                }
            }
            StackValue::Pointer(heap_key) => {
                let HeapValue::StackValue(value) = heap.get(heap_key).unwrap() else {
                    unreachable!()
                };

                let value = *value;

                self.resolve_binary_operand(
                    (heap, value_stack),
                    metamethod_key,
                    dest,
                    value_a,
                    value_b,
                    value,
                    coerce_value,
                )
            }
            _ => match coerce_value(heap, value) {
                Ok(coerced_value) => Ok(ValueOrCallResult::Value(coerced_value)),
                Err(err) => Err(err),
            },
        }
    }

    /// Converts integers to floats if any operand is a float, preserves type otherwise
    fn binary_number_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: StackValue,
        integer_operation: impl Fn(i64, i64) -> i64,
        float_operation: impl Fn(f64, f64) -> f64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        // using get since we resolve pointers in self.resolve_binary_operand()
        let value_a = value_stack.get(self.register_base + a as usize);
        let value_b = value_stack.get(self.register_base + b as usize);

        let resolved_a = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            dest,
            value_a,
            value_b,
            value_a,
            |_, value| Ok(value),
        )? {
            ValueOrCallResult::Value(value) => value,
            ValueOrCallResult::CallResult(call_result) => return Ok(Some(call_result)),
        };

        let resolved_b = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            dest,
            value_a,
            value_b,
            value_b,
            |_, value| Ok(value),
        )? {
            ValueOrCallResult::Value(value) => value,
            ValueOrCallResult::CallResult(call_result) => return Ok(Some(call_result)),
        };

        let value = match (resolved_a, resolved_b) {
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
            (StackValue::Integer(_) | StackValue::Float(_), _) => {
                return Err(RuntimeErrorData::InvalidArithmetic(
                    resolved_b.type_name(heap),
                ));
            }
            _ => {
                return Err(RuntimeErrorData::InvalidArithmetic(
                    resolved_a.type_name(heap),
                ));
            }
        };

        value_stack.set(self.register_base + dest as usize, value);

        Ok(None)
    }

    /// Converts integers to floats
    fn binary_float_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: StackValue,
        operation: impl Fn(f64, f64) -> f64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        // using get since we resolve pointers in self.resolve_binary_operand()
        let value_a = value_stack.get(self.register_base + a as usize);
        let value_b = value_stack.get(self.register_base + b as usize);

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
        metamethod_key: StackValue,
        operation: impl Fn(i64, i64) -> i64,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        // using get since we resolve pointers in self.resolve_binary_operand()
        let value_a = value_stack.get(self.register_base + a as usize);
        let value_b = value_stack.get(self.register_base + b as usize);

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

    fn comparison_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a, b): (Register, Register, Register),
        metamethod_key: StackValue,
        integer_comparison: impl Fn(i64, i64) -> bool,
        float_comparison: impl Fn(f64, f64) -> bool,
        heap_comparison: impl Fn(&HeapValue, &HeapValue) -> Option<bool>,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        if let Some(call_result) =
            self.try_binary_metamethods((heap, value_stack), metamethod_key, dest, value_a, value_b)
        {
            return Ok(Some(call_result));
        }

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
            (StackValue::HeapValue(key_a), StackValue::HeapValue(key_b)) => {
                let a = heap.get(key_a).unwrap();
                let b = heap.get(key_b).unwrap();

                let Some(result) = heap_comparison(a, b) else {
                    return Err(RuntimeErrorData::InvalidCompare(
                        a.type_name(heap),
                        b.type_name(heap),
                    ));
                };

                result
            }
            _ => {
                return Err(RuntimeErrorData::InvalidCompare(
                    value_a.type_name(heap),
                    value_b.type_name(heap),
                ))
            }
        };

        value_stack.set(self.register_base + dest as usize, StackValue::Bool(result));

        Ok(None)
    }

    fn try_binary_metamethods(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        metamethod_key: StackValue,
        dest: Register,
        value_a: StackValue,
        value_b: StackValue,
    ) -> Option<CallResult> {
        if let StackValue::HeapValue(heap_key) = value_a {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (heap_key, metamethod_key),
                dest,
                value_a,
                value_b,
            ) {
                return Some(call_result);
            }
        }

        if let StackValue::HeapValue(heap_key) = value_b {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (heap_key, metamethod_key),
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
        (heap_key, metamethod_key): (HeapKey, StackValue),
        dest: Register,
        value_a: StackValue,
        value_b: StackValue,
    ) -> Option<CallResult> {
        let function_key = heap.get_metamethod(heap_key, metamethod_key)?;

        let function_index = value_stack.len() - self.register_base;

        value_stack.extend([
            function_key.into(),
            StackValue::Integer(2),
            value_a,
            value_b,
        ]);

        Some(CallResult::Call(
            function_index,
            ReturnMode::Destination(dest),
        ))
    }

    fn unary_number_operation(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        (dest, a): (Register, Register),
        metamethod_key: StackValue,
        generate_error: impl Fn(TypeName) -> RuntimeErrorData,
        operation: impl Fn(&Heap, StackValue) -> Result<StackValue, RuntimeErrorData>,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);

        let result = match value_a {
            StackValue::HeapValue(heap_key) => {
                return Ok(Some(
                    self.unary_metamethod(
                        heap,
                        value_stack,
                        (heap_key, metamethod_key),
                        (dest, value_a),
                    )
                    .ok_or_else(|| generate_error(value_a.type_name(heap)))?,
                ));
            }
            // already resolved the pointer
            StackValue::Pointer(_) => unreachable!(),
            _ => operation(heap, value_a)?,
        };

        value_stack.set(self.register_base + dest as usize, result);

        Ok(None)
    }

    fn unary_metamethod(
        &self,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        (heap_key, metamethod_key): (HeapKey, StackValue),
        (dest, value_a): (Register, StackValue),
    ) -> Option<CallResult> {
        let function_key = heap.get_metamethod(heap_key, metamethod_key)?;
        let function_index = value_stack.len() - self.register_base;

        value_stack.extend([function_key.into(), StackValue::Integer(1), value_a]);

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
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        // initial test
        let StackValue::HeapValue(base_heap_key) = base else {
            return Err(RuntimeErrorData::AttemptToIndex(
                base.type_name(&exec_data.heap),
            ));
        };

        let heap_value = exec_data.heap.get(base_heap_key).unwrap();
        let mut value = match heap_value {
            HeapValue::Table(table) => table.get(key),
            HeapValue::Bytes(_) => StackValue::Nil,
            _ => {
                return Err(RuntimeErrorData::AttemptToIndex(
                    heap_value.type_name(&exec_data.heap),
                ))
            }
        };

        if value == StackValue::Nil {
            // resolve using __index
            let metamethod_key = exec_data.metatable_keys.index.0.key().into();
            let max_chain_depth = exec_data.limits.metatable_chain_depth;
            let mut chain_depth = 0;

            let mut index_base = base;
            let mut next_index_base = index_base;

            while next_index_base != StackValue::Nil {
                index_base = next_index_base;

                let StackValue::HeapValue(heap_key) = index_base else {
                    return Err(RuntimeErrorData::AttemptToIndex(
                        index_base.type_name(&exec_data.heap),
                    ));
                };

                let heap_value = exec_data.heap.get(heap_key).unwrap();

                match heap_value {
                    HeapValue::Table(table) => {
                        value = table.get(key);

                        if value != StackValue::Nil {
                            break;
                        }
                    }
                    HeapValue::NativeFunction(_) | HeapValue::Function(_) => {
                        let function_index = value_stack.len() - self.register_base;
                        value_stack.extend([heap_key.into(), StackValue::Integer(2), base, key]);

                        return Ok(Some(CallResult::Call(
                            function_index,
                            ReturnMode::Destination(dest),
                        )));
                    }
                    _ => {}
                };

                next_index_base = exec_data.heap.get_metavalue(heap_key, metamethod_key);
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
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let StackValue::HeapValue(heap_key) = table_stack_value else {
            return Err(RuntimeErrorData::AttemptToIndex(
                table_stack_value.type_name(&exec_data.heap),
            ));
        };

        let metamethod_key = exec_data.metatable_keys.newindex.0.key().into();
        let src_value = value_stack.get_deref(&exec_data.heap, self.register_base + src as usize);

        if let Some(function_key) = exec_data.heap.get_metamethod(heap_key, metamethod_key) {
            let function_index = value_stack.len() - self.register_base;

            value_stack.extend([
                function_key.into(),
                StackValue::Integer(3),
                heap_key.into(),
                key,
                src_value,
            ]);

            Ok(Some(CallResult::Call(
                function_index,
                ReturnMode::Static(0),
            )))
        } else {
            let gc = &mut exec_data.gc;
            let heap = &mut exec_data.heap;
            let table_value = heap.get_mut(gc, heap_key).unwrap();

            let HeapValue::Table(table) = table_value else {
                let value = heap.get(heap_key).unwrap();
                let type_name = value.type_name(heap);
                return Err(RuntimeErrorData::AttemptToIndex(type_name));
            };

            let original_size = table.heap_size();

            table.set(key, src_value);

            let new_size = table.heap_size();
            gc.modify_used_memory(new_size as isize - original_size as isize);

            if gc.should_step() {
                Ok(Some(CallResult::StepGc))
            } else {
                Ok(None)
            }
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

    fn copy_range_to_deref(
        &self,
        exec_data: &mut ExecutionAccessibleData,
        value_stack: &mut ValueStack,
        dest: Register,
        src: Register,
        count: Register,
    ) {
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

            if let StackValue::Pointer(heap_key) = slice[dest_index] {
                heap.set(gc, heap_key, HeapValue::StackValue(value));
            } else {
                slice[dest_index] = value;
            }
        }
    }
}

fn stringify(heap: &Heap, value: StackValue) -> Option<Cow<[u8]>> {
    let heap_key = match value {
        StackValue::HeapValue(heap_key) => heap_key,
        StackValue::Integer(i) => return Some(i.to_string().into_bytes().into()),
        StackValue::Float(f) => return Some(format!("{f:?}").into_bytes().into()),
        StackValue::Pointer(key) => {
            let HeapValue::StackValue(value) = *heap.get(key).unwrap() else {
                unreachable!();
            };

            return stringify(heap, value);
        }
        StackValue::Nil | StackValue::Bool(_) => return None,
    };

    let HeapValue::Bytes(bytes) = heap.get(heap_key).unwrap() else {
        return None;
    };

    Some(Cow::Borrowed(bytes.as_bytes()))
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

fn coerce_stack_value_to_integer(
    heap: &Heap,
    value: StackValue,
    generate_err: impl FnOnce(TypeName) -> RuntimeErrorData,
) -> Result<i64, RuntimeErrorData> {
    match value {
        StackValue::Float(float) => {
            coerce_integer(float).ok_or(RuntimeErrorData::NoIntegerRepresentation)
        }
        StackValue::Integer(int) => Ok(int),
        StackValue::Pointer(key) => {
            let HeapValue::StackValue(value) = *heap.get(key).unwrap() else {
                unreachable!();
            };

            coerce_stack_value_to_integer(heap, value, generate_err)
        }
        StackValue::HeapValue(_) | StackValue::Nil | StackValue::Bool(_) => {
            Err(generate_err(value.type_name(heap)))
        }
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
) -> Result<HeapKey, RuntimeErrorData> {
    let call_key = exec_data.metatable_keys.call.0.key().into();
    let max_chain_depth = exec_data.limits.metatable_chain_depth;
    let mut chain_depth = 0;

    loop {
        let StackValue::HeapValue(heap_key) = value else {
            return Err(RuntimeErrorData::InvalidCall(
                value.type_name(&exec_data.heap),
            ));
        };

        let Some(heap_value) = exec_data.heap.get(heap_key) else {
            return Err(RuntimeErrorData::InvalidRef);
        };

        if matches!(
            heap_value,
            HeapValue::Function(_) | HeapValue::NativeFunction(_)
        ) {
            break Ok(heap_key);
        }

        let next_value = exec_data.heap.get_metavalue(heap_key, call_key);
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
