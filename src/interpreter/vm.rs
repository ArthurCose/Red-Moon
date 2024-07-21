use super::cache_pools::CachePools;
use super::execution::ExecutionContext;
use super::heap::{GarbageCollector, GarbageCollectorConfig, Heap, HeapKey, HeapRef, HeapValue};
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
    pub(crate) gc: GarbageCollector,
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
            gc: self.gc.clone(),
            metatable_keys: self.metatable_keys.clone(),
            cache_pools: self.cache_pools.clone(),
            // reset to 0, since there's no active call on the new vm
            tracked_stack_size: 0,
            #[cfg(feature = "instruction_exec_counts")]
            instruction_counter: Default::default(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.limits.clone_from(&source.limits);
        self.heap.clone_from(&source.heap);
        self.gc.clone_from(&source.gc);
        self.metatable_keys.clone_from(&source.metatable_keys);
        self.cache_pools.clone_from(&source.cache_pools);
        // reset to 0, since there's no active call on the new vm
        self.tracked_stack_size = 0;

        #[cfg(feature = "instruction_exec_counts")]
        {
            self.instruction_counter.clear();
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

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}

impl Vm {
    pub fn new() -> Self {
        let mut gc = GarbageCollector::default();
        let mut heap = Heap::new(&mut gc);
        let default_environment = heap.create(&mut gc, HeapValue::Table(Default::default()));
        let default_environment = heap.create_ref(default_environment);

        let metatable_keys = MetatableKeys::new(&mut gc, &mut heap);

        Self {
            execution_data: ExecutionAccessibleData {
                limits: Default::default(),
                heap,
                gc,
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
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;
        let heap_key = heap.intern_bytes(gc, bytes);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                &self.execution_stack,
                heap,
            );
        }

        StringRef(heap_ref)
    }

    pub fn create_table(&mut self) -> TableRef {
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;
        let heap_key = heap.create_table(gc, 0, 0);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                &self.execution_stack,
                heap,
            );
        }

        TableRef(heap_ref)
    }

    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;
        let heap_key = heap.create_table(gc, list, map);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                &self.execution_stack,
                heap,
            );
        }

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

        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;
        let environment = environment
            .map(|table| table.0.key().into())
            .unwrap_or(self.default_environment.key().into());

        let mut keys = Vec::with_capacity(module.chunks.len());

        for (i, chunk) in module.chunks.into_iter().enumerate() {
            let byte_strings = chunk
                .byte_strings
                .into_iter()
                .map(|bytes| heap.intern_bytes(gc, bytes.as_ref()))
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

            let key = heap.create(
                gc,
                HeapValue::Function(Function {
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
                }),
            );

            keys.push(key);
        }

        let key = keys.get(module.main).ok_or(RuntimeErrorData::MissingMain)?;
        let heap_ref = heap.create_ref(*key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                &self.execution_stack,
                heap,
            );
        }

        Ok(FunctionRef(heap_ref))
    }

    pub fn create_native_function(
        &mut self,
        callback: impl Fn(MultiValue, &mut VmContext) -> Result<MultiValue, RuntimeError>
            + Clone
            + 'static,
    ) -> FunctionRef {
        let heap = &mut self.execution_data.heap;
        let gc = &mut self.execution_data.gc;
        let key = heap.create(gc, HeapValue::NativeFunction(callback.into()));

        let heap_ref = heap.create_ref(key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                &self.execution_stack,
                heap,
            );
        }

        FunctionRef(heap_ref)
    }

    #[inline]
    pub fn gc_used_memory(&self) -> usize {
        self.execution_data.gc.used_memory()
    }

    #[inline]
    pub fn gc_is_running(&self) -> bool {
        self.execution_data.gc.is_running()
    }

    #[inline]
    pub fn gc_stop(&mut self) {
        self.execution_data.gc.stop()
    }

    #[inline]
    pub fn gc_restart(&mut self) {
        self.execution_data.gc.restart()
    }

    pub fn gc_step(&mut self, bytes: usize) {
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;

        gc.modify_used_memory(bytes as _);

        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                &self.execution_stack,
                heap,
            );
        }

        gc.modify_used_memory(-(bytes as isize));
    }

    pub fn gc_collect(&mut self) {
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;

        gc.full_cycle(
            &self.execution_data.metatable_keys,
            &self.execution_data.cache_pools,
            &self.execution_stack,
            heap,
        );
    }

    #[inline]
    pub fn gc_config_mut(&mut self) -> &mut GarbageCollectorConfig {
        &mut self.execution_data.gc.config
    }

    #[inline]
    pub fn context(&mut self) -> VmContext {
        VmContext { vm: self }
    }
}

pub struct VmContext<'vm> {
    pub(crate) vm: &'vm mut Vm,
}

impl<'vm> VmContext<'vm> {
    pub fn clone_vm(&self) -> Vm {
        self.vm.clone()
    }

    #[inline]
    pub fn create_multi(&mut self) -> MultiValue {
        self.vm.create_multi()
    }

    #[inline]
    pub fn store_multi(&mut self, multivalue: MultiValue) {
        self.vm.store_multi(multivalue)
    }

    #[inline]
    #[cfg(feature = "instruction_exec_counts")]
    pub fn instruction_exec_counts(&mut self) -> Vec<(&'static str, usize)> {
        self.vm.instruction_exec_counts()
    }

    #[inline]
    #[cfg(feature = "instruction_exec_counts")]
    pub fn clear_instruction_exec_counts(&mut self) {
        self.vm.clear_instruction_exec_counts();
    }

    #[inline]
    pub fn limits(&self) -> &VmLimits {
        self.vm.limits()
    }

    #[inline]
    pub fn set_limits(&mut self, limits: VmLimits) {
        self.vm.set_limits(limits);
    }

    #[inline]
    pub fn default_environment(&self) -> TableRef {
        self.vm.default_environment()
    }

    #[inline]
    pub fn environment_up_value(&mut self) -> Option<TableRef> {
        self.vm.environment_up_value()
    }

    #[inline]
    pub fn string_metatable(&self) -> TableRef {
        self.vm.string_metatable()
    }

    #[inline]
    pub fn metatable_keys(&self) -> &MetatableKeys {
        self.vm.metatable_keys()
    }

    #[inline]
    pub fn set_app_data<T: Clone + 'static>(&mut self, value: T) -> Option<T> {
        self.vm.set_app_data(value)
    }

    #[inline]
    pub fn app_data<T: 'static>(&self) -> Option<&T> {
        self.vm.app_data()
    }

    #[inline]
    pub fn app_data_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.vm.app_data_mut()
    }

    #[inline]
    pub fn remove_app_data<T: 'static>(&mut self) -> Option<T> {
        self.vm.remove_app_data()
    }

    #[inline]
    pub fn intern_string(&mut self, bytes: &[u8]) -> StringRef {
        self.vm.intern_string(bytes)
    }

    #[inline]
    pub fn create_table(&mut self) -> TableRef {
        self.vm.create_table()
    }

    #[inline]
    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        self.vm.create_table_with_capacity(list, map)
    }

    /// If the environment is unset, the function will use the default environment
    #[inline]
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
        self.vm.load_function(label, environment, module)
    }

    #[inline]
    pub fn create_native_function(
        &mut self,
        callback: impl Fn(MultiValue, &mut VmContext) -> Result<MultiValue, RuntimeError>
            + Clone
            + 'static,
    ) -> FunctionRef {
        self.vm.create_native_function(callback)
    }

    #[inline]
    pub fn gc_used_memory(&self) -> usize {
        self.vm.gc_used_memory()
    }

    #[inline]
    pub fn gc_is_running(&self) -> bool {
        self.vm.gc_is_running()
    }

    #[inline]
    pub fn gc_stop(&mut self) {
        self.vm.gc_stop()
    }

    #[inline]
    pub fn gc_restart(&mut self) {
        self.vm.gc_restart()
    }

    #[inline]
    pub fn gc_step(&mut self, bytes: usize) {
        self.vm.gc_step(bytes)
    }

    #[inline]
    pub fn gc_collect(&mut self) {
        self.vm.gc_collect()
    }

    #[inline]
    pub fn gc_config_mut(&mut self) -> &mut GarbageCollectorConfig {
        self.vm.gc_config_mut()
    }

    pub(crate) fn call_function_key<A: IntoMulti, R: FromMulti>(
        &mut self,
        function_key: HeapKey,
        args: A,
    ) -> Result<R, RuntimeError> {
        let args = args.into_multi(self)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        let heap = &self.vm.execution_data.heap;

        for value in &args.values {
            value.test_validity(heap)?;
        }

        let Some(value) = heap.get(function_key) else {
            return Err(RuntimeErrorData::NotAFunction.into());
        };

        let old_stack_size = self.vm.execution_data.tracked_stack_size;

        let result = match value {
            HeapValue::NativeFunction(func) => func.shallow_clone().call(args, self),
            HeapValue::Function(func) => {
                let context =
                    ExecutionContext::new_function_call(function_key, func.clone(), args, self.vm);
                self.vm.execution_stack.push(context);
                ExecutionContext::resume(self.vm)
            }
            _ => ExecutionContext::new_value_call(function_key.into(), args, self.vm)
                .map_err(RuntimeError::from)
                .and_then(|context| {
                    self.vm.execution_stack.push(context);
                    ExecutionContext::resume(self.vm)
                }),
        };

        self.vm.execution_data.tracked_stack_size = old_stack_size;

        let multi = result?;
        R::from_multi(multi, self)
    }
}
