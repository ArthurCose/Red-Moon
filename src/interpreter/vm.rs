use super::cache_pools::CachePools;
use super::coroutine::Coroutine;
use super::execution::ExecutionContext;
use super::garbage_collector::{GarbageCollector, GarbageCollectorConfig};
use super::heap::{CoroutineObjectKey, Heap};
use super::metatable_keys::MetatableKeys;
use super::value_stack::StackValue;
use super::{Continuation, NativeCallContext};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::Module;
use crate::interpreter::debug_hooks::{DebugHook, HookMask};
use crate::interpreter::interpreted_function::{Function, FunctionDefinition};
use crate::interpreter::type_set::TypeSet;
pub use crate::values::{
    ForEachValue, FromValues, FunctionRef, MultiValue, NativeValue, StringRef, TableRef, ThreadRef,
};
use std::rc::Rc;

#[cfg(feature = "instruction_metrics")]
use super::instruction_metrics::InstructionMetricTracking;

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

#[derive(Default)]
pub(crate) struct CoroutineData {
    pub(crate) yield_permitted: bool,
    pub(crate) yield_pending: bool,
    pub(crate) resumed_result: Option<Result<MultiValue, RuntimeError>>,
    pub(crate) coroutine_stack: Vec<CoroutineObjectKey>,
    /// Vec<Continuation, parent_allows_yield>
    pub(crate) in_progress_yield: Vec<(Continuation, bool)>,
}

pub(crate) struct ExecutionAccessibleData {
    pub(crate) limits: VmLimits,
    pub(crate) heap: Heap,
    pub(crate) gc: GarbageCollector,
    pub(crate) coroutine_data: CoroutineData,
    pub(crate) metatable_keys: Rc<MetatableKeys>,
    pub(crate) cache_pools: Rc<CachePools>,
    pub(crate) tracked_stack_size: usize,
    pub(crate) debug_hook: DebugHook,
    #[cfg(feature = "instruction_metrics")]
    pub(crate) instruction_tracking: InstructionMetricTracking,
}

impl Clone for ExecutionAccessibleData {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits.clone(),
            heap: self.heap.clone(),
            gc: self.gc.clone(),
            coroutine_data: Default::default(),
            metatable_keys: self.metatable_keys.clone(),
            cache_pools: self.cache_pools.clone(),
            // reset, since there's no active call on the new vm
            tracked_stack_size: 0,
            debug_hook: self.debug_hook.clone(),
            #[cfg(feature = "instruction_metrics")]
            instruction_tracking: Default::default(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.limits.clone_from(&source.limits);
        self.heap.clone_from(&source.heap);
        self.gc.clone_from(&source.gc);
        self.metatable_keys.clone_from(&source.metatable_keys);
        self.cache_pools.clone_from(&source.cache_pools);
        self.debug_hook.clone_from(&source.debug_hook);
        // reset, since there's no active call on the new vm
        self.tracked_stack_size = 0;

        #[cfg(feature = "instruction_metrics")]
        {
            self.instruction_tracking.clear();
        }
    }
}

pub struct Vm {
    pub(crate) execution_data: ExecutionAccessibleData,
    pub(crate) execution_stack: Vec<ExecutionContext>,
    registry: TableRef,
    default_environment: TableRef,
    singletons: TypeSet,
}

#[cfg(feature = "serde")]
impl serde::Serialize for Vm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // enable deduplication
        crate::serde_util::begin_dedup();

        // serialize
        let result = (|| {
            use serde::ser::SerializeStruct;
            let mut state = serializer.serialize_struct("Vm", 5)?;
            state.serialize_field("limits", &self.execution_data.limits)?;
            state.serialize_field("heap", &self.execution_data.heap)?;
            state.serialize_field("gc", &self.execution_data.gc)?;
            state.serialize_field("metatable_keys", &*self.execution_data.metatable_keys)?;
            state.serialize_field("debug_hook", &self.execution_data.debug_hook)?;
            state.serialize_field("singletons", &self.singletons)?;
            state.serialize_field("registry", &self.registry)?;
            state.serialize_field("default_environment", &self.default_environment)?;
            state.end()
        })();

        // reset + disable deduplication
        crate::serde_util::end_dedup();

        result
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Vm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(rename = "Vm")]
        struct Data {
            limits: VmLimits,
            heap: Heap,
            gc: GarbageCollector,
            metatable_keys: MetatableKeys,
            debug_hook: DebugHook,
            singletons: TypeSet,
            registry: TableRef,
            default_environment: TableRef,
        }

        // enable deduplication
        crate::serde_util::begin_dedup();

        // deserialize
        let result = serde::Deserialize::deserialize(deserializer);

        // reset + disable deduplication
        crate::serde_util::end_dedup();

        let data: Data = result?;

        // apply
        Ok(Vm {
            execution_data: ExecutionAccessibleData {
                limits: data.limits,
                heap: data.heap,
                gc: data.gc,
                coroutine_data: Default::default(),
                metatable_keys: Rc::new(data.metatable_keys),
                cache_pools: Default::default(),
                tracked_stack_size: Default::default(),
                debug_hook: data.debug_hook,
            },
            execution_stack: Default::default(),
            registry: data.registry,
            default_environment: data.default_environment,
            singletons: data.singletons,
        })
    }
}

impl Clone for Vm {
    fn clone(&self) -> Self {
        Self {
            execution_data: self.execution_data.clone(),
            // we can clear the execution stack on the copy
            execution_stack: Default::default(),
            registry: self.registry.clone(),
            default_environment: self.default_environment.clone(),
            singletons: self.singletons.clone(),
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
        let registry_key = heap.create_table(&mut gc, 0, 0);
        let registry = heap.create_ref(registry_key);
        let default_environment_key = heap.create_table(&mut gc, 0, 0);
        let default_environment = heap.create_ref(default_environment_key);

        let metatable_keys = MetatableKeys::new(&mut gc, &mut heap);

        Self {
            execution_data: ExecutionAccessibleData {
                limits: Default::default(),
                heap,
                gc,
                coroutine_data: Default::default(),
                metatable_keys: Rc::new(metatable_keys),
                cache_pools: Default::default(),
                tracked_stack_size: 0,
                debug_hook: Default::default(),
                #[cfg(feature = "instruction_metrics")]
                instruction_tracking: Default::default(),
            },
            execution_stack: Default::default(),
            registry: TableRef(registry),
            default_environment: TableRef(default_environment),
            singletons: Default::default(),
        }
    }

    #[inline]
    pub fn context(&mut self) -> VmContext<'_> {
        VmContext { vm: self }
    }
}

pub struct VmContext<'vm> {
    pub(crate) vm: &'vm mut Vm,
}

impl VmContext<'_> {
    pub fn clone_vm(&self) -> Vm {
        self.vm.clone()
    }

    #[inline]
    pub fn create_multi(&mut self) -> MultiValue {
        self.vm.execution_data.cache_pools.create_multi()
    }

    #[inline]
    pub fn store_multi(&mut self, multivalue: MultiValue) {
        self.vm.execution_data.cache_pools.store_multi(multivalue)
    }

    #[inline]
    #[cfg(feature = "instruction_metrics")]
    pub fn instruction_metrics(&mut self) -> Vec<super::InstructionMetrics> {
        self.vm.execution_data.instruction_tracking.data()
    }

    #[inline]
    #[cfg(feature = "instruction_metrics")]
    pub fn clear_instruction_metrics(&mut self) {
        self.vm.execution_data.instruction_tracking.clear();
    }

    #[inline]
    pub fn limits(&self) -> &VmLimits {
        &self.vm.execution_data.limits
    }

    #[inline]
    pub fn set_limits(&mut self, limits: VmLimits) {
        self.vm.execution_data.limits = limits;
    }

    #[inline]
    pub fn registry(&self) -> TableRef {
        self.vm.registry.clone()
    }

    #[inline]
    pub fn default_environment(&self) -> TableRef {
        self.vm.default_environment.clone()
    }

    #[inline]
    pub fn environment_up_value(&mut self) -> Option<TableRef> {
        let context = self.vm.execution_stack.last()?;
        let interpreter = context.interpreter_stack.last()?;
        let env_index = interpreter.function.definition.env?;

        let heap = &mut self.vm.execution_data.heap;
        let env_stack_value_key = interpreter.function.up_values.get(env_index)?;

        let Some(StackValue::Table(env_table_key)) = heap.get_stack_value(*env_stack_value_key)
        else {
            return None;
        };

        Some(TableRef(heap.create_ref(*env_table_key)))
    }

    #[inline]
    pub fn string_metatable(&self) -> TableRef {
        let heap = &self.vm.execution_data.heap;
        TableRef(heap.string_metatable_ref().clone())
    }

    #[inline]
    pub fn metatable_keys(&self) -> &MetatableKeys {
        &self.vm.execution_data.metatable_keys
    }

    #[inline]
    pub fn set_singleton<T: NativeValue + Clone + 'static>(&mut self, value: T) -> Option<T> {
        self.vm.singletons.insert(value)
    }

    #[inline]
    pub fn singleton<T: 'static>(&self) -> Option<&T> {
        self.vm.singletons.get()
    }

    #[inline]
    pub fn singleton_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.vm.singletons.get_mut()
    }

    #[inline]
    pub fn remove_singleton<T: 'static>(&mut self) -> Option<T> {
        self.vm.singletons.remove()
    }

    pub fn intern_string(&mut self, bytes: &[u8]) -> StringRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.intern_bytes(gc, bytes);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.try_gc_step();

        StringRef(heap_ref)
    }

    pub fn create_table(&mut self) -> TableRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.create_table(gc, 0, 0);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.try_gc_step();

        TableRef(heap_ref)
    }

    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.create_table(gc, list, map);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.try_gc_step();

        TableRef(heap_ref)
    }

    /// Loads a compiled lua module as a function.
    ///
    /// If the environment is unset, the function will use the default environment.
    ///
    /// ```
    /// # use red_moon::errors::RuntimeError;
    /// use red_moon::interpreter::Vm;
    /// use red_moon::languages::lua::compile;
    ///
    /// let mut vm = Vm::default();
    /// let ctx = &mut vm.context();
    ///
    /// let module = compile("return 1 + 2").unwrap();
    /// let function = ctx.load_function("main", None, module)?;
    /// let result: i64 = function.call((), ctx)?;
    ///
    /// assert_eq!(result, 3);
    /// # Ok::<_, RuntimeError>(())
    /// ```
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
        let label = label.into();
        self.load_function_inner(label, environment, module)
    }

    // load_function with reduced type params
    fn load_function_inner<'a, ByteStrings, B>(
        &mut self,
        label: Rc<str>,
        environment: Option<TableRef>,
        module: Module<ByteStrings>,
    ) -> Result<FunctionRef, RuntimeError>
    where
        B: AsRef<[u8]> + 'a,
        ByteStrings: IntoIterator<Item = B>,
    {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let mut memory_increase = 0;

        // create environment stack value
        let environment = environment
            .map(|table| table.0.key().into())
            .unwrap_or(self.vm.default_environment.0.key().into());
        // storing in up values as StackValue::Pointer
        let environment = heap.store_stack_value(gc, environment);

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

            let mut up_values = Vec::new();

            if i == module.main
                && let Some(index) = chunk.env
            {
                if index != 0 {
                    return Err(RuntimeErrorData::InvalidMainEnvIndex.into());
                }

                up_values.push(environment);
            }

            let definition = Rc::new(FunctionDefinition {
                label: label.clone(),
                env: chunk.env,
                up_values: chunk.up_values,
                byte_strings,
                numbers: chunk.numbers,
                functions,
                instructions: chunk.instructions,
                source_map: chunk.source_map,
            });

            memory_increase += definition.heap_size();

            let key = heap.store_interpreted_fn(
                gc,
                Function {
                    up_values: up_values.into(),
                    definition,
                },
            );

            keys.push(key);
        }

        gc.modify_used_memory(memory_increase as _);

        let key = keys.get(module.main).ok_or(RuntimeErrorData::MissingMain)?;
        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        self.try_gc_step();

        Ok(FunctionRef(heap_ref))
    }

    pub fn create_function(
        &mut self,
        #[cfg(not(feature = "implicit_closures"))] callback: fn(
            &mut NativeCallContext,
            &mut VmContext<'_>,
        )
            -> Result<(), RuntimeError>,
        #[cfg(feature = "implicit_closures")] callback: impl Fn(
            &mut NativeCallContext,
            &mut VmContext<'_>,
        ) -> Result<(), RuntimeError>
        + Clone
        + 'static,
    ) -> FunctionRef {
        let heap = &mut self.vm.execution_data.heap;
        let gc = &mut self.vm.execution_data.gc;

        let key = heap.store_native_fn(gc, callback.into());
        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        self.try_gc_step();

        FunctionRef(heap_ref)
    }

    #[inline]
    pub fn top_coroutine(&mut self) -> Option<ThreadRef> {
        let coroutine_data = &mut self.vm.execution_data.coroutine_data;
        let key = *coroutine_data.coroutine_stack.last()?;

        Some(ThreadRef(self.vm.execution_data.heap.create_ref(key)))
    }

    #[inline]
    pub fn top_thread(&mut self) -> ThreadRef {
        self.top_coroutine().unwrap_or_else(|| self.main_thread())
    }

    #[inline]
    pub fn main_thread(&mut self) -> ThreadRef {
        ThreadRef::new_main_thread()
    }

    pub fn create_coroutine(&mut self, function: FunctionRef) -> Result<ThreadRef, RuntimeError> {
        let function_key = function.0.key();

        let heap = &self.vm.execution_data.heap;
        function.test_validity(heap)?;

        let coroutine = Coroutine::new(function_key);

        // move to the heap
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;

        let heap_key = heap.store_coroutine(gc, coroutine);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.try_gc_step();

        Ok(ThreadRef(heap_ref))
    }

    /// Returns true if the calling context allows yielding (Coroutine or resumable)
    #[inline]
    pub fn is_yieldable(&self) -> bool {
        let coroutine_data = &self.vm.execution_data.coroutine_data;
        coroutine_data.yield_permitted
    }

    #[inline]
    pub fn gc_used_memory(&self) -> usize {
        self.vm.execution_data.gc.used_memory()
    }

    #[inline]
    pub fn gc_is_running(&self) -> bool {
        self.vm.execution_data.gc.is_running()
    }

    #[inline]
    pub fn gc_stop(&mut self) {
        self.vm.execution_data.gc.stop()
    }

    #[inline]
    pub fn gc_restart(&mut self) {
        self.vm.execution_data.gc.restart()
    }

    pub fn gc_step(&mut self, bytes: usize) {
        let gc = &mut self.vm.execution_data.gc;
        gc.modify_used_memory(bytes as _);

        self.try_gc_step();

        let gc = &mut self.vm.execution_data.gc;
        gc.modify_used_memory(-(bytes as isize));
    }

    pub(crate) fn try_gc_step(&mut self) {
        let exec_data = &mut self.vm.execution_data;
        let gc = &mut exec_data.gc;
        let heap = &mut exec_data.heap;

        if gc.should_step() {
            gc.step(
                &exec_data.metatable_keys,
                &exec_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &exec_data.coroutine_data,
                &exec_data.debug_hook,
            );
        }
    }

    pub fn gc_collect(&mut self) {
        let exec_data = &mut self.vm.execution_data;
        let gc = &mut exec_data.gc;
        let heap = &mut exec_data.heap;

        gc.full_cycle(
            &exec_data.metatable_keys,
            &exec_data.cache_pools,
            heap,
            &self.vm.execution_stack,
            &exec_data.coroutine_data,
            &exec_data.debug_hook,
        );
    }

    #[inline]
    pub fn gc_config_mut(&mut self) -> &mut GarbageCollectorConfig {
        &mut self.vm.execution_data.gc.config
    }

    pub fn set_hook(
        &mut self,
        mask: HookMask,
        instruction_count: usize,
        callback: FunctionRef,
    ) -> Result<(), RuntimeErrorData> {
        let exec_data = &mut self.vm.execution_data;
        callback.test_validity(&exec_data.heap)?;

        let debug_hook = &mut exec_data.debug_hook;
        debug_hook.reset();
        debug_hook.mask = mask;
        debug_hook.after_instructions = instruction_count;
        debug_hook.callback = Some(callback.0.key());

        Ok(())
    }

    #[inline]
    pub fn remove_hook(&mut self) {
        self.vm.execution_data.debug_hook.reset();
    }

    pub fn hook(&mut self) -> Option<FunctionRef> {
        let storage_key = self.vm.execution_data.debug_hook.callback?;
        let heap_key = self.vm.execution_data.heap.create_ref(storage_key);
        Some(FunctionRef(heap_key))
    }

    #[inline]
    pub fn hook_mask(&self) -> HookMask {
        self.vm.execution_data.debug_hook.mask
    }

    #[inline]
    pub fn hook_count(&self) -> usize {
        self.vm.execution_data.debug_hook.after_instructions
    }

    pub(crate) fn call_function_key<A: ForEachValue, R: FromValues>(
        &mut self,
        function_value: StackValue,
        args: A,
    ) -> Result<R, RuntimeError> {
        let args = MultiValue::pack(args, self)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        let heap = &self.vm.execution_data.heap;

        for value in &args.values {
            value.test_validity(heap)?;
        }

        let execution_data = &mut self.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;
        let yield_prev_permitted = coroutine_data.yield_permitted;
        coroutine_data.yield_permitted = false;

        let result = match function_value {
            StackValue::NativeFunction(key) => ExecutionContext::call_native_fn(key, args, self.vm),
            StackValue::Function(key) => ExecutionContext::call_interpreted(key, args, self.vm),
            _ => ExecutionContext::call_value(function_value, args, self.vm),
        };

        let execution_data = &mut self.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;
        coroutine_data.yield_permitted = yield_prev_permitted;

        if let Err(
            mut err @ RuntimeError {
                data: RuntimeErrorData::Yield(_),
                ..
            },
        ) = result
        {
            // can't yield from this function
            coroutine_data.yield_pending = false;
            coroutine_data.in_progress_yield.clear();

            err.data = RuntimeErrorData::InvalidYield;

            return Err(err);
        }

        result?.unpack(self)
    }

    pub(crate) fn yieldable_call_function_key<A: ForEachValue, R: FromValues>(
        &mut self,
        function_value: StackValue,
        args: A,
        call_ctx: &mut NativeCallContext,
        yield_response: impl FnOnce(
            &mut NativeCallContext,
            &mut VmContext,
        ) -> Result<FunctionRef, RuntimeError>,
    ) -> Result<Result<R, RuntimeError>, RuntimeError> {
        // pack args
        let args = MultiValue::pack(args, self)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        let heap = &self.vm.execution_data.heap;

        for value in &args.values {
            value.test_validity(heap)?;
        }

        // call the function
        let result = match function_value {
            StackValue::NativeFunction(key) => ExecutionContext::call_native_fn(key, args, self.vm),
            StackValue::Function(key) => ExecutionContext::call_interpreted(key, args, self.vm),
            _ => ExecutionContext::call_value(function_value, args, self.vm),
        };

        // handle the result
        match result {
            Err(
                err @ RuntimeError {
                    data: RuntimeErrorData::Yield(_),
                    ..
                },
            ) => {
                // make sure we can yield
                let execution_data = &mut self.vm.execution_data;
                let coroutine_data = &mut execution_data.coroutine_data;

                if !coroutine_data.yield_pending || !coroutine_data.yield_permitted {
                    coroutine_data.yield_pending = false;
                    coroutine_data.in_progress_yield.clear();

                    return Err(RuntimeErrorData::InvalidYield.into());
                }

                let response_result = yield_response(call_ctx, self);

                let execution_data = &mut self.vm.execution_data;
                let coroutine_data = &mut execution_data.coroutine_data;

                let function_ref = match response_result {
                    Ok(function_ref) => function_ref,
                    Err(err) => {
                        return Err(err);
                    }
                };

                coroutine_data
                    .in_progress_yield
                    .push((Continuation::Callback(function_ref), true));

                Err(err)
            }
            Err(err) => Ok(Err(err)),
            Ok(values) => Ok(values.unpack(self)),
        }
    }
}
