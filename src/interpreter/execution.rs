use super::heap::{Heap, HeapKey, HeapValue};
use super::instruction::{Instruction, Register, ReturnMode};
use super::interpreted_function::{Function, FunctionDefinition};
use super::multi::MultiValue;
use super::value_stack::{Primitive, StackValue, ValueStack};
use super::vm::{ExecutionAccessibleData, Vm};
use super::Value;
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData, StackTrace};
use crate::languages::lua::{coerce_integer, parse_number};
use std::borrow::Cow;
use std::rc::Rc;

enum CallResult {
    Call(usize, ReturnMode),
    Return(usize),
}

pub(crate) struct ExecutionContext {
    pub(crate) call_stack: Vec<CallContext>,
    pub(crate) value_stack: ValueStack,
    pub(crate) pending_captures: ValueStack,
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
            pending_captures: exec_data.cache_pools.create_short_value_stack(),
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
                let results = match function.shallow_clone().call(args, vm) {
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

        let exec_data = &mut vm.execution_data;

        Ok(Self {
            call_stack,
            value_stack,
            pending_captures: exec_data.cache_pools.create_short_value_stack(),
        })
    }

    pub(crate) fn resume(vm: &mut Vm) -> Result<MultiValue, RuntimeError> {
        let mut execution = vm.execution_stack.last_mut().unwrap();

        let mut last_definition = None;

        let mut exec_data = &mut vm.execution_data;

        while let Some(call) = execution.call_stack.last_mut() {
            let result = match call.resume(
                &mut execution.value_stack,
                &mut execution.pending_captures,
                exec_data,
            ) {
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

                    let register_base = call.register_base;
                    let stack_start = register_base + registry_index;

                    let StackValue::HeapValue(heap_key) = execution.value_stack.get(stack_start)
                    else {
                        return Err(Self::unwind_error(vm, RuntimeErrorData::NotAFunction));
                    };

                    let StackValue::Primitive(Primitive::Integer(mut arg_count)) =
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
                            .set(stack_start + 1, Primitive::Integer(arg_count).into());
                    });

                    let function_key = match function_key {
                        Ok(key) => key,
                        Err(err) => return Err(Self::unwind_error(vm, err)),
                    };

                    match exec_data.heap.get(function_key) {
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

                            let arg_count = args.len();

                            // handle tail call
                            let mut return_mode = return_mode;
                            let mut stack_start = stack_start;
                            let mut register_base = register_base;

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

                                // adopt the register base of the caller's caller
                                let grand_call = execution
                                    .call_stack
                                    .last()
                                    .expect("a root native call is rewritten as ReturnMode::Multi");
                                register_base = grand_call.register_base;
                            }

                            // update tracked stack size in case the native function calls an interpreted function
                            let old_stack_size = exec_data.tracked_stack_size;
                            exec_data.tracked_stack_size =
                                old_stack_size + execution.value_stack.len();

                            // call the function and handle return values
                            let mut return_values = match callback.call(args, vm) {
                                Ok(values) => values,
                                Err(err) => return Err(Self::continue_unwind(vm, err)),
                            };

                            // juggling lifetimes
                            execution = vm.execution_stack.last_mut().unwrap();
                            exec_data = &mut vm.execution_data;

                            // revert tracked stack size
                            exec_data.tracked_stack_size = old_stack_size;

                            match return_mode {
                                ReturnMode::Multi => {
                                    execution.value_stack.chip(stack_start, 0);
                                    return_values.push_stack_multi(&mut execution.value_stack);
                                }
                                ReturnMode::Static(return_count) => {
                                    execution.value_stack.chip(stack_start, 0);

                                    execution.value_stack.extend(
                                        std::iter::from_fn(|| return_values.pop_front())
                                            .map(|v| v.to_stack_value())
                                            .chain(std::iter::repeat(StackValue::default()))
                                            .take(return_count as usize),
                                    );
                                }
                                ReturnMode::Destination(dest) => {
                                    execution.value_stack.chip(stack_start, 0);

                                    let value = return_values.pop_front().unwrap_or_default();

                                    let dest_index = register_base + dest as usize;
                                    execution
                                        .value_stack
                                        .set(dest_index, value.to_stack_value());
                                }
                                ReturnMode::Extend(len_index) => {
                                    execution.value_stack.chip(stack_start, 0);

                                    let return_count = return_values.len();

                                    // append return values
                                    execution.value_stack.extend(std::iter::from_fn(|| {
                                        return_values.pop_front().map(|v| v.to_stack_value())
                                    }));

                                    // add the return count
                                    let len_register = register_base + len_index as usize;

                                    let StackValue::Primitive(Primitive::Integer(
                                        stored_return_count,
                                    )) = execution.value_stack.get(len_register)
                                    else {
                                        return Err(Self::unwind_error(
                                            vm,
                                            IllegalInstruction::MissingReturnCount.into(),
                                        ));
                                    };

                                    let count = stored_return_count + return_count as i64 - 1;
                                    let count_value = Primitive::Integer(count).into();
                                    execution.value_stack.set(len_register, count_value);
                                }
                                ReturnMode::UnsizedDestinationPreserve(dest) => {
                                    // keep the function and args
                                    execution.value_stack.chip(stack_start + arg_count + 2, 0);

                                    let return_count = return_values.len();

                                    for i in 0..return_count {
                                        let Some(value) = return_values.pop_front() else {
                                            break;
                                        };

                                        execution.value_stack.set(
                                            register_base + dest as usize + i,
                                            value.to_stack_value(),
                                        );
                                    }
                                }
                                ReturnMode::TailCall => {
                                    // we retained the TailCall return mode, this is our final return
                                    let execution = vm.execution_stack.pop().unwrap();
                                    exec_data
                                        .cache_pools
                                        .store_value_stack(execution.value_stack);
                                    return Ok(return_values);
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
                            return Err(Self::unwind_error(vm, RuntimeErrorData::NotAFunction));
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
                    let StackValue::Primitive(Primitive::Integer(return_count)) =
                        execution.value_stack.get(stack_index)
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
                            let StackValue::Primitive(Primitive::Integer(stored_return_count)) =
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
                            let count_value = Primitive::Integer(count).into();
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

    fn unwind_error(vm: &mut Vm, data: RuntimeErrorData) -> RuntimeError {
        Self::continue_unwind(
            vm,
            RuntimeError {
                trace: StackTrace::default(),
                data,
            },
        )
    }

    fn continue_unwind(vm: &mut Vm, mut err: RuntimeError) -> RuntimeError {
        let context = vm.execution_stack.pop().unwrap();
        let exec_data = &mut vm.execution_data;

        // recycle value stacks
        for call in context.call_stack.into_iter().rev() {
            let instruction_index = call.next_instruction_index.saturating_sub(1);
            let definition = call.function_definition;
            let frame = definition.create_stack_trace_frame(instruction_index);
            err.trace.push_frame(frame);

            exec_data
                .cache_pools
                .store_short_value_stack(call.up_values);
        }

        exec_data.cache_pools.store_value_stack(context.value_stack);

        err
    }
}

pub(crate) struct CallContext {
    pub(crate) up_values: ValueStack,
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
        pending_captures: &mut ValueStack,
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

            let heap = &mut exec_data.heap;
            self.next_instruction_index += 1;

            match instruction {
                Instruction::Constant(_) => {
                    return Err(IllegalInstruction::UnexpectedConstant.into())
                }
                Instruction::SetNil(dest) => {
                    value_stack.set(self.register_base + dest as usize, Primitive::Nil.into());
                }
                Instruction::SetBool(dest, b) => {
                    let value = Primitive::Bool(b).into();
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadInt(dest, index) => {
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = Primitive::Integer(*number).into();
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::LoadFloat(dest, index) => {
                    let Some(number) = definition.numbers.get(index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(index).into());
                    };
                    let value = Primitive::Float(f64::from_bits(*number as u64)).into();
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
                    let value = Primitive::Integer(*number).into();

                    let dest_index = self.register_base + dest as usize;
                    value_stack.set(dest_index, value);
                    value_stack.chip(dest_index + 1, 0);
                }
                Instruction::CreateTable(dest, len_index) => {
                    let Some(&len) = definition.numbers.get(len_index as usize) else {
                        return Err(IllegalInstruction::MissingNumberConstant(len_index).into());
                    };

                    let heap_key = heap.create_table(len as _, 0);

                    value_stack.set(self.register_base + dest as usize, heap_key.into());
                }
                Instruction::FlushToTable(dest, total, index_offset) => {
                    let err: RuntimeErrorData = IllegalInstruction::InvalidHeapKey.into();

                    // get the table
                    let dest_index = self.register_base + dest as usize;

                    let StackValue::HeapValue(heap_key) = value_stack.get(dest_index) else {
                        return Err(err.clone());
                    };

                    let Some(table_value) = heap.get_mut(heap_key) else {
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

                    table.flush(index_offset, value_stack.get_slice(start..end));
                }
                Instruction::VariadicToTable(dest, src_start, index_offset) => {
                    let table_err: RuntimeErrorData = IllegalInstruction::InvalidHeapKey.into();

                    // grab the table
                    let dest_index = self.register_base + dest as usize;

                    let StackValue::HeapValue(heap_key) = value_stack.get(dest_index) else {
                        return Err(table_err.clone());
                    };

                    let Some(table_value) = heap.get_mut(heap_key) else {
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
                    let StackValue::Primitive(Primitive::Integer(count)) =
                        value_stack.get(count_index)
                    else {
                        return Err(table_err.clone());
                    };

                    let start = self.register_base + src_start as usize;
                    let end = start + count as usize;

                    table.reserve_list(count as usize);
                    table.flush(index_offset, value_stack.get_slice(start..end));
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

                    let StackValue::Primitive(Primitive::Integer(count)) =
                        value_stack.get(count_index)
                    else {
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

                        let count_value = Primitive::Integer(count + total as i64).into();
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
                Instruction::Capture(dest, src) => {
                    let mut value = value_stack.get(self.register_base + src as usize);

                    if !matches!(value, StackValue::Pointer(_)) {
                        // move the stack value to the heap
                        let heap_key = heap.create(HeapValue::StackValue(value));
                        value = StackValue::Pointer(heap_key);
                        value_stack.set(self.register_base + src as usize, value);
                    }

                    pending_captures.set(dest as usize, value);
                }
                Instruction::CaptureUpValue(dest, src) => {
                    let mut value = self.up_values.get(src as usize);

                    if !matches!(value, StackValue::Pointer(_)) {
                        // move the stack value to the heap
                        let heap_key = heap.create(HeapValue::StackValue(value));
                        value = StackValue::Pointer(heap_key);
                        self.up_values.set(src as usize, value);
                    }

                    pending_captures.set(dest as usize, value);
                }
                Instruction::Closure(dest, function_index) => {
                    let Some(&heap_key) = definition.functions.get(function_index as usize) else {
                        return Err(RuntimeErrorData::IllegalInstruction(
                            IllegalInstruction::MissingFunctionConstant(function_index),
                        ));
                    };

                    if pending_captures.is_empty() {
                        value_stack.set(self.register_base + dest as usize, heap_key.into());
                    } else {
                        let HeapValue::Function(func) = heap.get(heap_key).unwrap() else {
                            unreachable!()
                        };

                        // copy the function to pass captures
                        let mut func = func.clone();

                        let mut up_values = exec_data.cache_pools.create_short_value_stack();
                        std::mem::swap(pending_captures, &mut up_values);
                        func.up_values = Rc::new(up_values);

                        let heap_key = heap.create(HeapValue::Function(func));

                        value_stack.set(self.register_base + dest as usize, heap_key.into());
                    }
                }
                Instruction::ClearUpValue(dest) => {
                    self.up_values.set(dest as usize, Primitive::Nil.into());
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
                        heap.set(heap_key, HeapValue::StackValue(value)).unwrap();
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
                        heap.set(heap_key, HeapValue::StackValue(value)).unwrap();
                    } else {
                        value_stack.set(dest_index, value);
                    }
                }
                Instruction::CopyRangeToDeref(dest, src, count) => {
                    self.copy_range_to_deref(heap, value_stack, dest, src, count);
                }
                Instruction::Not(dest, src) => {
                    let value = !value_stack.is_truthy(self.register_base + src as usize);
                    value_stack.set(
                        self.register_base + dest as usize,
                        Primitive::Bool(value).into(),
                    );
                }
                Instruction::Len(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.len.0.key().into();

                    let value_a = value_stack.get_deref(heap, self.register_base + src as usize);

                    let StackValue::HeapValue(heap_key) = value_a else {
                        return Err(RuntimeErrorData::NoLength);
                    };

                    // default behavior
                    let heap_value = heap.get(heap_key).unwrap();
                    let len = match heap_value {
                        HeapValue::Table(table) => table.list_len(),
                        HeapValue::Bytes(bytes) => bytes.len(),
                        _ => return Err(RuntimeErrorData::NoLength),
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

                    let value = Primitive::Integer(len as i64).into();
                    value_stack.set(self.register_base + dest as usize, value);
                }
                Instruction::UnaryMinus(dest, src) => {
                    let metamethod_key = exec_data.metatable_keys.unm.0.key().into();

                    if let Some(call_result) = self.unary_number_operation(
                        (heap, value_stack),
                        (dest, src),
                        metamethod_key,
                        || RuntimeErrorData::InvalidArithmetic,
                        |primitive| match primitive {
                            Primitive::Integer(n) => Ok(Primitive::Integer(-n)),
                            Primitive::Float(n) => Ok(Primitive::Float(-n)),
                            _ => Err(RuntimeErrorData::InvalidArithmetic),
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
                        || RuntimeErrorData::InvalidArithmetic,
                        |primitive| Ok(Primitive::Integer(-arithmetic_cast_integer(primitive)?)),
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
                    let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

                    match value_b {
                        StackValue::Primitive(Primitive::Integer(0)) => {
                            return Err(RuntimeErrorData::DivideByZero)
                        }
                        StackValue::Primitive(Primitive::Float(float)) => {
                            if float as i64 == 0 {
                                return Err(RuntimeErrorData::DivideByZero);
                            }
                        }
                        _ => {}
                    }

                    let metamethod_key = exec_data.metatable_keys.idiv.0.key().into();

                    if let Some(call_result) = self.binary_number_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a / b,
                        // lua seems to preserve floats unlike bitwise operators
                        |a, b| (a / b).trunc(),
                    )? {
                        return Ok(call_result);
                    }
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
                        (dest, value_a, value_b),
                    ) {
                        return Ok(call_result);
                    }

                    let equal = match (value_a, value_b) {
                        (
                            StackValue::Primitive(Primitive::Float(float)),
                            StackValue::Primitive(Primitive::Integer(int)),
                        )
                        | (
                            StackValue::Primitive(Primitive::Integer(int)),
                            StackValue::Primitive(Primitive::Float(float)),
                        ) => int as f64 == float,
                        _ => value_a == value_b,
                    };

                    value_stack.set(
                        self.register_base + dest as usize,
                        Primitive::Bool(equal).into(),
                    );
                }
                Instruction::LessThan(dest, a, b) => {
                    let metamethod_key = exec_data.metatable_keys.lt.0.key().into();

                    if let Some(call_result) = self.comparison_operation(
                        (heap, value_stack),
                        (dest, a, b),
                        metamethod_key,
                        |a, b| a < b,
                        |a, b| a < b,
                        |heap, a, b| {
                            if let (HeapValue::Bytes(bytes_a), HeapValue::Bytes(bytes_b)) =
                                (heap.get(a).unwrap(), heap.get(b).unwrap())
                            {
                                Ok(bytes_a < bytes_b)
                            } else {
                                Err(RuntimeErrorData::InvalidCompare)
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
                        |heap, a, b| {
                            if let (HeapValue::Bytes(bytes_a), HeapValue::Bytes(bytes_b)) =
                                (heap.get(a).unwrap(), heap.get(b).unwrap())
                            {
                                Ok(bytes_a <= bytes_b)
                            } else {
                                Err(RuntimeErrorData::InvalidCompare)
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
                        let heap_key = heap.intern_bytes(&bytes);
                        value_stack.set(self.register_base + dest as usize, heap_key.into());
                    } else {
                        // try metamethod as a fallback using the default value
                        let call_result = self
                            .try_binary_metamethods(
                                (heap, value_stack),
                                metamethod_key.into(),
                                (dest, value_a, value_b),
                            )
                            .ok_or(RuntimeErrorData::AttemptToConcatInvalid)?;

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
                        != StackValue::Primitive(Primitive::Nil)
                    {
                        self.next_instruction_index += 1;
                    }
                }
                Instruction::NumericFor(src, local) => {
                    let limit = coerce_stack_value_to_integer(
                        heap,
                        value_stack.get(self.register_base + src as usize),
                        || RuntimeErrorData::LimitMustBeNumber,
                    )?;
                    let step = coerce_stack_value_to_integer(
                        heap,
                        value_stack.get(self.register_base + src as usize + 1),
                        || RuntimeErrorData::StepMustBeNumber,
                    )?;
                    let mut value = coerce_stack_value_to_integer(
                        heap,
                        value_stack.get(self.register_base + local as usize),
                        || RuntimeErrorData::InitialValueMustBeNumber,
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
                            Primitive::Integer(value).into(),
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

        value_stack.set(self.register_base, Primitive::Integer(0).into());

        // exhausted instructions
        Ok(CallResult::Return(0))
    }

    /// Resolves stack value pointers, calls metamethods for heap values, calls coerce functions if necessary
    fn resolve_binary_operand<T>(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        metamethod_key: StackValue,
        metamethod_params: (Register, StackValue, StackValue),
        value: StackValue,
        coerce_primitive: impl Fn(Primitive) -> T,
        coerce_heap_value: impl Fn(&mut Heap, HeapKey) -> T,
    ) -> Result<T, CallResult> {
        match value {
            StackValue::Primitive(primitive) => Ok(coerce_primitive(primitive)),
            StackValue::HeapValue(heap_key) => {
                if let Some(call_result) = self.binary_metamethod(
                    heap,
                    value_stack,
                    (heap_key, metamethod_key),
                    metamethod_params,
                ) {
                    return Err(call_result);
                }

                Ok(coerce_heap_value(heap, heap_key))
            }
            StackValue::Pointer(heap_key) => {
                let HeapValue::StackValue(value) = heap.get(heap_key).unwrap() else {
                    unreachable!()
                };

                let value = *value;

                self.resolve_binary_operand(
                    (heap, value_stack),
                    metamethod_key,
                    metamethod_params,
                    value,
                    coerce_primitive,
                    coerce_heap_value,
                )
            }
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

        let primitive_a = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            (dest, value_a, value_b),
            value_a,
            |primitive| primitive,
            |heap, heap_key| heap_value_as_number(heap, heap_key),
        ) {
            Ok(primitive) => primitive,
            Err(call_result) => return Ok(Some(call_result)),
        };

        let primitive_b = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            (dest, value_a, value_b),
            value_b,
            |primitive| primitive,
            |heap, heap_key| heap_value_as_number(heap, heap_key),
        ) {
            Ok(primitive) => primitive,
            Err(call_result) => return Ok(Some(call_result)),
        };

        let value = match (primitive_a, primitive_b) {
            (Primitive::Integer(a), Primitive::Integer(b)) => {
                Primitive::Integer(integer_operation(a, b)).into()
            }
            (Primitive::Float(a), Primitive::Float(b)) => {
                Primitive::Float(float_operation(a, b)).into()
            }
            (Primitive::Float(a), Primitive::Integer(b)) => {
                Primitive::Float(float_operation(a, b as f64)).into()
            }
            (Primitive::Integer(a), Primitive::Float(b)) => {
                Primitive::Float(float_operation(a as f64, b)).into()
            }
            _ => {
                return Err(RuntimeErrorData::InvalidArithmetic);
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
            (dest, value_a, value_b),
            value_a,
            arithmetic_cast_float,
            heap_value_as_float,
        ) {
            Ok(result) => result?,
            Err(call_result) => return Ok(Some(call_result)),
        };

        let float_b = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            (dest, value_a, value_b),
            value_b,
            arithmetic_cast_float,
            heap_value_as_float,
        ) {
            Ok(result) => result?,
            Err(call_result) => return Ok(Some(call_result)),
        };

        value_stack.set(
            self.register_base + dest as usize,
            Primitive::Float(operation(float_a, float_b)).into(),
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
            (dest, value_a, value_b),
            value_a,
            arithmetic_cast_integer,
            heap_value_as_integer,
        ) {
            Ok(result) => result?,
            Err(call_result) => return Ok(Some(call_result)),
        };

        let int_b = match self.resolve_binary_operand(
            (heap, value_stack),
            metamethod_key,
            (dest, value_a, value_b),
            value_b,
            arithmetic_cast_integer,
            heap_value_as_integer,
        ) {
            Ok(result) => result?,
            Err(call_result) => return Ok(Some(call_result)),
        };

        value_stack.set(
            self.register_base + dest as usize,
            Primitive::Integer(operation(int_a, int_b)).into(),
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
        heap_comparison: impl Fn(&mut Heap, HeapKey, HeapKey) -> Result<bool, RuntimeErrorData>,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);
        let value_b = value_stack.get_deref(heap, self.register_base + b as usize);

        if let Some(call_result) = self.try_binary_metamethods(
            (heap, value_stack),
            metamethod_key,
            (dest, value_a, value_b),
        ) {
            return Ok(Some(call_result));
        }

        let result = match (value_a, value_b) {
            (
                StackValue::Primitive(Primitive::Integer(int_a)),
                StackValue::Primitive(Primitive::Integer(int_b)),
            ) => integer_comparison(int_a, int_b),
            (
                StackValue::Primitive(Primitive::Float(float)),
                StackValue::Primitive(Primitive::Integer(int)),
            )
            | (
                StackValue::Primitive(Primitive::Integer(int)),
                StackValue::Primitive(Primitive::Float(float)),
            ) => float_comparison(int as f64, float),
            (
                StackValue::Primitive(Primitive::Float(float_a)),
                StackValue::Primitive(Primitive::Float(float_b)),
            ) => float_comparison(float_a, float_b),
            (StackValue::HeapValue(key_a), StackValue::HeapValue(key_b)) => {
                heap_comparison(heap, key_a, key_b)?
            }
            _ => return Err(RuntimeErrorData::InvalidCompare),
        };

        value_stack.set(
            self.register_base + dest as usize,
            Primitive::Bool(result).into(),
        );

        Ok(None)
    }

    fn try_binary_metamethods(
        &self,
        (heap, value_stack): (&mut Heap, &mut ValueStack),
        metamethod_key: StackValue,
        (dest, value_a, value_b): (Register, StackValue, StackValue),
    ) -> Option<CallResult> {
        if let StackValue::HeapValue(heap_key) = value_a {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (heap_key, metamethod_key),
                (dest, value_a, value_b),
            ) {
                return Some(call_result);
            }
        }

        if let StackValue::HeapValue(heap_key) = value_b {
            if let Some(call_result) = self.binary_metamethod(
                heap,
                value_stack,
                (heap_key, metamethod_key),
                (dest, value_a, value_b),
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
        (dest, value_a, value_b): (Register, StackValue, StackValue),
    ) -> Option<CallResult> {
        let function_key = heap.get_metamethod(heap_key, metamethod_key)?;

        let function_index = value_stack.len() - self.register_base;

        value_stack.extend([
            function_key.into(),
            Primitive::Integer(2).into(),
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
        generate_error: impl Fn() -> RuntimeErrorData,
        operation: impl Fn(Primitive) -> Result<Primitive, RuntimeErrorData>,
    ) -> Result<Option<CallResult>, RuntimeErrorData> {
        let value_a = value_stack.get_deref(heap, self.register_base + a as usize);

        let primitive = match value_a {
            StackValue::Primitive(primitive) => operation(primitive)?,
            StackValue::HeapValue(heap_key) => {
                return Ok(Some(
                    self.unary_metamethod(
                        heap,
                        value_stack,
                        (heap_key, metamethod_key),
                        (dest, value_a),
                    )
                    .ok_or_else(generate_error)?,
                ));
            }
            // already resolved the pointer
            StackValue::Pointer(_) => unreachable!(),
        };

        value_stack.set(self.register_base + dest as usize, primitive.into());

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

        value_stack.extend([function_key.into(), Primitive::Integer(1).into(), value_a]);

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
            return Err(RuntimeErrorData::AttemptToIndexInvalid);
        };

        let mut value = match exec_data.heap.get(base_heap_key).unwrap() {
            HeapValue::Table(table) => table.get(key),
            HeapValue::Bytes(_) => StackValue::Primitive(Primitive::Nil),
            _ => return Err(RuntimeErrorData::AttemptToIndexInvalid),
        };

        let mut index_base = base;

        if value == StackValue::Primitive(Primitive::Nil) {
            // resolve using __index
            let metamethod_key = exec_data.metatable_keys.index.0.key().into();
            let max_chain_depth = exec_data.limits.metatable_chain_depth;
            let mut chain_depth = 0;

            let mut next_index_base = index_base;

            while next_index_base != StackValue::Primitive(Primitive::Nil) {
                index_base = next_index_base;

                let StackValue::HeapValue(heap_key) = index_base else {
                    return Err(RuntimeErrorData::AttemptToIndexInvalid);
                };

                let heap_value = exec_data.heap.get(heap_key).unwrap();

                match heap_value {
                    HeapValue::Table(table) => {
                        value = table.get(key);

                        if value != StackValue::Primitive(Primitive::Nil) {
                            break;
                        }
                    }
                    HeapValue::NativeFunction(_) | HeapValue::Function(_) => {
                        let function_index = value_stack.len() - self.register_base;
                        value_stack.extend([
                            heap_key.into(),
                            Primitive::Integer(2).into(),
                            base,
                            key,
                        ]);

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
                    return Err(RuntimeErrorData::MetatableChainTooLong);
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
            return Err(RuntimeErrorData::AttemptToIndexInvalid);
        };

        let metamethod_key = exec_data.metatable_keys.newindex.0.key().into();
        let src_value = value_stack.get_deref(&exec_data.heap, self.register_base + src as usize);

        if let Some(function_key) = exec_data.heap.get_metamethod(heap_key, metamethod_key) {
            let function_index = value_stack.len() - self.register_base;

            value_stack.extend([
                function_key.into(),
                Primitive::Integer(3).into(),
                heap_key.into(),
                key,
                src_value,
            ]);

            Ok(Some(CallResult::Call(
                function_index,
                ReturnMode::Static(0),
            )))
        } else {
            let table_value = exec_data.heap.get_mut(heap_key).unwrap();

            let HeapValue::Table(table) = table_value else {
                return Err(RuntimeErrorData::AttemptToIndexInvalid);
            };

            table.set(key, src_value);

            Ok(None)
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
            *value = StackValue::Primitive(Primitive::Nil);
        }
    }

    fn copy_range_to_deref(
        &self,
        heap: &mut Heap,
        value_stack: &mut ValueStack,
        dest: Register,
        src: Register,
        count: Register,
    ) {
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
                heap.set(heap_key, HeapValue::StackValue(value)).unwrap();
            } else {
                slice[dest_index] = value;
            }
        }
    }
}

fn stringify(heap: &Heap, value: StackValue) -> Option<Cow<[u8]>> {
    let heap_key = match value {
        StackValue::HeapValue(heap_key) => heap_key,
        StackValue::Primitive(primitive) => match primitive {
            Primitive::Integer(i) => return Some(i.to_string().into_bytes().into()),
            Primitive::Float(f) => return Some(format!("{f:?}").into_bytes().into()),
            _ => return None,
        },
        StackValue::Pointer(key) => {
            let HeapValue::StackValue(value) = *heap.get(key).unwrap() else {
                unreachable!();
            };

            return stringify(heap, value);
        }
    };

    let HeapValue::Bytes(bytes) = heap.get(heap_key).unwrap() else {
        return None;
    };

    Some(Cow::Borrowed(bytes.as_bytes()))
}

fn cast_integer(
    primitive: Primitive,
    generate_err: impl FnOnce() -> RuntimeErrorData,
) -> Result<i64, RuntimeErrorData> {
    match primitive {
        Primitive::Float(float) => {
            coerce_integer(float).ok_or(RuntimeErrorData::NoIntegerRepresentation(float))
        }
        Primitive::Integer(int) => Ok(int),
        _ => Err(generate_err()),
    }
}

fn cast_float(
    primitive: Primitive,
    generate_err: impl FnOnce() -> RuntimeErrorData,
) -> Result<f64, RuntimeErrorData> {
    match primitive {
        Primitive::Integer(int) => Ok(int as f64),
        Primitive::Float(float) => Ok(float),
        _ => Err(generate_err()),
    }
}

fn coerce_stack_value_to_integer(
    heap: &Heap,
    value: StackValue,
    generate_err: impl FnOnce() -> RuntimeErrorData,
) -> Result<i64, RuntimeErrorData> {
    match value {
        StackValue::Primitive(primitive) => cast_integer(primitive, generate_err),
        StackValue::HeapValue(key) => cast_integer(heap_value_as_number(heap, key), generate_err),
        StackValue::Pointer(key) => {
            let HeapValue::StackValue(value) = *heap.get(key).unwrap() else {
                unreachable!();
            };

            coerce_stack_value_to_integer(heap, value, generate_err)
        }
    }
}

fn arithmetic_cast_float(primitive: Primitive) -> Result<f64, RuntimeErrorData> {
    cast_float(primitive, || RuntimeErrorData::InvalidArithmetic)
}

fn arithmetic_cast_integer(primitive: Primitive) -> Result<i64, RuntimeErrorData> {
    cast_integer(primitive, || RuntimeErrorData::InvalidArithmetic)
}

fn heap_value_as_number(heap: &Heap, heap_key: HeapKey) -> Primitive {
    let Some(HeapValue::Bytes(string)) = heap.get(heap_key) else {
        return Primitive::Nil;
    };

    let Ok(s) = std::str::from_utf8(string.as_bytes()) else {
        return Primitive::Nil;
    };

    parse_number(s)
}

fn heap_value_as_float(heap: &mut Heap, heap_key: HeapKey) -> Result<f64, RuntimeErrorData> {
    arithmetic_cast_float(heap_value_as_number(heap, heap_key))
}

fn heap_value_as_integer(heap: &mut Heap, heap_key: HeapKey) -> Result<i64, RuntimeErrorData> {
    arithmetic_cast_integer(heap_value_as_number(heap, heap_key))
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
            return Err(RuntimeErrorData::NotAFunction);
        };

        let Some(heap_value) = exec_data.heap.get(heap_key) else {
            return Err(RuntimeErrorData::NotAFunction);
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
            return Err(RuntimeErrorData::MetatableChainTooLong);
        }
    }
}