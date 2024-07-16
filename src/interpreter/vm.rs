use super::cache_pools::CachePools;
use super::execution::ExecutionContext;
use super::heap::{Heap, HeapKey, HeapRef, HeapValue};
use super::metatable_keys::MetatableKeys;
use super::value_stack::{StackValue, ValueStack};
use super::{FromMulti, FunctionRef, IntoMulti, Module, MultiValue, StringRef, TableRef};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::interpreted_function::{Function, FunctionDefinition};
use crate::FastHashMap;
use downcast::downcast;
use std::any::TypeId;
use std::rc::Rc;

#[cfg(feature = "instruction_exec_counts")]
use super::instruction::InstructionCounter;

#[derive(Clone)]
pub struct VmLimits {
    pub stack_size: usize,
    pub metatable_chain_depth: usize,
}

impl Default for VmLimits {
    fn default() -> Self {
        Self {
            stack_size: 1000000,
            metatable_chain_depth: 2000,
        }
    }
}

trait AppData: downcast::Any {
    fn clone_box(&self) -> Box<dyn AppData>;
}

impl<T: Clone + 'static> AppData for T {
    fn clone_box(&self) -> Box<dyn AppData> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn AppData> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

downcast!(dyn AppData);

pub(crate) struct ExecutionAccessibleData {
    pub(crate) limits: VmLimits,
    pub(crate) heap: Heap,
    pub(crate) metatable_keys: Rc<MetatableKeys>,
    pub(crate) cache_pools: Rc<CachePools>,
    pub(crate) tracked_stack_size: usize,
    #[cfg(feature = "instruction_exec_counts")]
    pub(crate) instruction_counter: InstructionCounter,
}

impl Clone for ExecutionAccessibleData {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits.clone(),
            heap: self.heap.clone(),
            metatable_keys: self.metatable_keys.clone(),
            cache_pools: self.cache_pools.clone(),
            // reset to 0, since there's no active call on the new vm
            tracked_stack_size: 0,
            #[cfg(feature = "instruction_exec_counts")]
            instruction_counter: Default::default(),
        }
    }
}

pub struct Vm {
    pub(crate) execution_data: ExecutionAccessibleData,
    pub(crate) execution_stack: Vec<ExecutionContext>,
    default_environment: HeapRef,
    pub(crate) environment_up_value: Option<HeapKey>,
    app_data: FastHashMap<TypeId, Box<dyn AppData>>,
}

impl Clone for Vm {
    fn clone(&self) -> Self {
        Self {
            execution_data: self.execution_data.clone(),
            // we can clear the execution stack on the copy
            execution_stack: Default::default(),
            default_environment: self.default_environment.clone(),
            environment_up_value: self.environment_up_value,
            app_data: self.app_data.clone(),
        }
    }
}

impl Vm {
    pub fn new() -> Self {
        let mut heap = Heap::default();

        let default_environment = heap.create(HeapValue::Table(Default::default()));
        let default_environment = heap.create_ref(default_environment);

        let metatable_keys = MetatableKeys::new(&mut heap);

        Self {
            execution_data: ExecutionAccessibleData {
                limits: Default::default(),
                heap,
                metatable_keys: Rc::new(metatable_keys),
                cache_pools: Default::default(),
                tracked_stack_size: 0,
                #[cfg(feature = "instruction_exec_counts")]
                instruction_counter: Default::default(),
            },
            execution_stack: Default::default(),
            default_environment,
            environment_up_value: None,
            app_data: Default::default(),
        }
    }

    #[inline]
    pub fn create_multi(&mut self) -> MultiValue {
        self.execution_data.cache_pools.create_multi()
    }

    #[inline]
    pub fn store_multi(&mut self, multivalue: MultiValue) {
        self.execution_data.cache_pools.store_multi(multivalue)
    }

    #[cfg(feature = "instruction_exec_counts")]
    pub fn instruction_exec_counts(&mut self) -> Vec<(&'static str, usize)> {
        let mut results = self
            .execution_data
            .instruction_counter
            .data()
            .collect::<Vec<_>>();

        // sort by count reversed
        results.sort_by_key(|(_, count)| usize::MAX - count);
        results
    }

    #[cfg(feature = "instruction_exec_counts")]
    pub fn clear_instruction_exec_counts(&mut self) {
        self.execution_data.instruction_counter.clear();
    }

    #[inline]
    pub fn limits(&self) -> &VmLimits {
        &self.execution_data.limits
    }

    #[inline]
    pub fn set_limits(&mut self, limits: VmLimits) {
        self.execution_data.limits = limits;
    }

    #[inline]
    pub fn default_environment(&self) -> TableRef {
        TableRef(self.default_environment.clone())
    }

    #[inline]
    pub fn environment_up_value(&mut self) -> Option<TableRef> {
        let context = self.execution_stack.last()?;
        let call = context.call_stack.last()?;
        let env_index = call.function_definition.env?;
        let StackValue::HeapValue(env_key) = call.up_values.get(env_index) else {
            return None;
        };

        let heap = &mut self.execution_data.heap;

        if !matches!(heap.get(env_key), Some(HeapValue::Table(_))) {
            // not a table or was garbage collected
            return None;
        }

        Some(TableRef(heap.create_ref(env_key)))
    }

    #[inline]
    pub fn string_metatable(&self) -> TableRef {
        let heap = &self.execution_data.heap;
        TableRef(heap.string_metatable_ref().clone())
    }

    #[inline]
    pub fn metatable_keys(&self) -> &MetatableKeys {
        &self.execution_data.metatable_keys
    }

    pub fn set_app_data<T: Clone + 'static>(&mut self, value: T) -> Option<T> {
        self.app_data
            .insert(TypeId::of::<T>(), Box::new(value))
            .map(|b| *b.downcast::<T>().unwrap())
    }

    pub fn app_data<T: 'static>(&self) -> Option<&T> {
        self.app_data
            .get(&TypeId::of::<T>())
            .map(|b| b.downcast_ref::<T>().unwrap())
    }

    pub fn app_data_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.app_data
            .get_mut(&TypeId::of::<T>())
            .map(|b| b.downcast_mut::<T>().unwrap())
    }

    pub fn remove_app_data<T: 'static>(&mut self) -> Option<T> {
        self.app_data
            .remove(&TypeId::of::<T>())
            .map(|b| *b.downcast::<T>().unwrap())
    }

    pub fn intern_string(&mut self, bytes: &[u8]) -> StringRef {
        let heap = &mut self.execution_data.heap;
        let heap_key = heap.intern_bytes(bytes);
        let heap_ref = heap.create_ref(heap_key);

        StringRef(heap_ref)
    }

    pub fn create_table(&mut self) -> TableRef {
        let heap = &mut self.execution_data.heap;
        let heap_key = heap.create_table(0, 0);
        let heap_ref = heap.create_ref(heap_key);
        TableRef(heap_ref)
    }

    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        let heap = &mut self.execution_data.heap;
        let heap_key = heap.create_table(list, map);
        let heap_ref = heap.create_ref(heap_key);
        TableRef(heap_ref)
    }

    /// If the environment is unset, the function will use the default environment
    pub fn load_function<'a, Label, ByteStrings, B>(
        &mut self,
        label: Label,
        environment: Option<TableRef>,
        module: Module<ByteStrings>,
    ) -> Result<FunctionRef, RuntimeError>
    where
        Label: Into<Rc<str>>,
        B: AsRef<[u8]> + 'a,
        ByteStrings: IntoIterator<Item = B>,
    {
        let label = label.into();

        let heap = &mut self.execution_data.heap;
        let environment = environment
            .map(|table| table.0.key().into())
            .unwrap_or(self.default_environment.key().into());

        let mut keys = Vec::with_capacity(module.chunks.len());

        for (i, chunk) in module.chunks.into_iter().enumerate() {
            let byte_strings = chunk
                .byte_strings
                .into_iter()
                .map(|bytes| heap.intern_bytes(bytes.as_ref()))
                .collect();

            let functions = chunk
                .dependencies
                .into_iter()
                .map(|index| keys[index])
                .collect();

            let mut up_values = ValueStack::default();

            if i == module.main {
                if let Some(index) = chunk.env {
                    up_values.set(index, environment);
                }
            }

            let key = heap.create(HeapValue::Function(Function {
                up_values: up_values.into(),
                definition: Rc::new(FunctionDefinition {
                    label: label.clone(),
                    env: chunk.env,
                    byte_strings,
                    numbers: chunk.numbers,
                    functions,
                    instructions: chunk.instructions,
                    source_map: chunk.source_map,
                }),
            }));

            keys.push(key);
        }

        let key = keys.get(module.main).ok_or(RuntimeErrorData::MissingMain)?;
        let heap_ref = heap.create_ref(*key);

        Ok(FunctionRef(heap_ref))
    }

    pub fn create_native_function(
        &mut self,
        callback: impl Fn(MultiValue, &mut Vm) -> Result<MultiValue, RuntimeError> + Clone + 'static,
    ) -> FunctionRef {
        let heap = &mut self.execution_data.heap;
        let key = heap.create(HeapValue::NativeFunction(callback.into()));

        let heap_ref = heap.create_ref(key);

        FunctionRef(heap_ref)
    }

    pub(crate) fn call_function_key<A: IntoMulti, R: FromMulti>(
        &mut self,
        function_key: HeapKey,
        args: A,
    ) -> Result<R, RuntimeError> {
        let args = args.into_multi(self)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        let heap = &self.execution_data.heap;

        for value in &args.values {
            value.test_validity(heap)?;
        }

        let Some(value) = heap.get(function_key) else {
            return Err(RuntimeErrorData::NotAFunction.into());
        };

        let old_stack_size = self.execution_data.tracked_stack_size;

        let result = match value {
            HeapValue::NativeFunction(func) => func.shallow_clone().call(args, self),
            HeapValue::Function(func) => {
                let context =
                    ExecutionContext::new_function_call(function_key, func.clone(), args, self);
                self.execution_stack.push(context);
                ExecutionContext::resume(self)
            }
            _ => ExecutionContext::new_value_call(function_key.into(), args, self)
                .map_err(RuntimeError::from)
                .and_then(|context| {
                    self.execution_stack.push(context);
                    ExecutionContext::resume(self)
                }),
        };

        self.execution_data.tracked_stack_size = old_stack_size;

        let multi = result?;
        R::from_multi(multi, self)
    }

    pub fn gc_collect(&mut self) {
        let heap = &mut self.execution_data.heap;
        heap.gc_collect(
            &self.execution_data.metatable_keys,
            &self.execution_data.cache_pools,
            &self.execution_stack,
        );
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}
