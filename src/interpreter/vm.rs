use super::cache_pools::CachePools;
use super::coroutine::{Coroutine, YieldPermissions};
use super::execution::ExecutionContext;
use super::heap::{
    CoroutineObjectKey, GarbageCollector, GarbageCollectorConfig, Heap, NativeFnObjectKey,
};
use super::metatable_keys::MetatableKeys;
use super::native_function::{NativeCallContext, NativeFunction};
use super::value_stack::{StackValue, ValueStack};
use super::{
    Continuation, CoroutineRef, ForEachValue, FromValues, FunctionRef, Module, MultiValue,
    StringRef, TableRef,
};
use crate::FastHashMap;
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::debug_hooks::{DebugHook, HookMask};
use crate::interpreter::interpreted_function::{Function, FunctionDefinition};
use downcast::downcast;
use std::any::TypeId;
use std::rc::Rc;

#[cfg(feature = "instruction_metrics")]
use super::instruction_metrics::InstructionMetricTracking;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

#[derive(Default)]
pub(crate) struct CoroutineData {
    pub(crate) yield_permissions: YieldPermissions,
    pub(crate) continuation_state_set: bool,
    pub(crate) continuation_states: Vec<ValueStack>,
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
    app_data: FastHashMap<TypeId, Box<dyn AppData>>,
}

#[cfg(feature = "serde")]
impl Serialize for Vm {
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
            state.serialize_field("gc", &self.execution_data.gc)?;
            state.serialize_field("heap_storage", &self.execution_data.heap.storage)?;
            state.serialize_field("tags", &self.execution_data.heap.tags)?;
            state.serialize_field("byte_strings", &self.execution_data.heap.byte_strings)?;
            state.end()
        })();

        // reset + disable deduplication
        crate::serde_util::end_dedup();

        result
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for Vm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use crate::BuildFastHasher;
        use crate::interpreter::ByteString;
        use crate::interpreter::heap::{BytesObjectKey, NativeFnObjectKey, Storage};
        use indexmap::IndexMap;

        #[derive(Deserialize)]
        #[serde(rename = "Vm")]
        struct Data {
            limits: VmLimits,
            gc: GarbageCollector,
            heap_storage: Storage,
            tags: IndexMap<StackValue, NativeFnObjectKey, BuildFastHasher>,
            byte_strings: FastHashMap<ByteString, BytesObjectKey>,
        }

        // enable deduplication
        crate::serde_util::begin_dedup();

        // deserialize
        let result = Deserialize::deserialize(deserializer);

        // reset + disable deduplication
        crate::serde_util::end_dedup();

        let data: Data = result?;

        // apply
        let mut vm = Vm::default();
        vm.execution_data.limits = data.limits;
        vm.execution_data.gc = data.gc;
        vm.execution_data.heap.storage = data.heap_storage;
        vm.execution_data.heap.tags = data.tags;
        vm.execution_data.heap.byte_strings = data.byte_strings;

        Ok(vm)
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

    #[cfg(feature = "instruction_metrics")]
    pub fn instruction_metrics(&mut self) -> Vec<super::InstructionMetrics> {
        self.execution_data.instruction_tracking.data()
    }

    #[cfg(feature = "instruction_metrics")]
    pub fn clear_instruction_metrics(&mut self) {
        self.execution_data.instruction_tracking.clear();
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
    pub fn registry(&self) -> TableRef {
        self.registry.clone()
    }

    #[inline]
    pub fn default_environment(&self) -> TableRef {
        self.default_environment.clone()
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
        gc.modify_used_memory(bytes as _);

        self.try_gc_step();

        let gc = &mut self.execution_data.gc;
        gc.modify_used_memory(-(bytes as isize));
    }

    fn try_gc_step(&mut self) {
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;

        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                heap,
                &self.execution_stack,
                &self.execution_data.coroutine_data,
                &self.execution_data.debug_hook,
            );
        }
    }

    pub fn gc_collect(&mut self) {
        let gc = &mut self.execution_data.gc;
        let heap = &mut self.execution_data.heap;

        gc.full_cycle(
            &self.execution_data.metatable_keys,
            &self.execution_data.cache_pools,
            heap,
            &self.execution_stack,
            &self.execution_data.coroutine_data,
            &self.execution_data.debug_hook,
        );
    }

    #[inline]
    pub fn gc_config_mut(&mut self) -> &mut GarbageCollectorConfig {
        &mut self.execution_data.gc.config
    }

    pub fn set_hook(
        &mut self,
        mask: HookMask,
        instruction_count: usize,
        callback: FunctionRef,
    ) -> Result<(), RuntimeErrorData> {
        callback.test_validity(&self.execution_data.heap)?;

        self.execution_data.debug_hook.reset();
        self.execution_data.debug_hook.mask = mask;
        self.execution_data.debug_hook.after_instructions = instruction_count;
        self.execution_data.debug_hook.callback = Some(callback.0.key());

        Ok(())
    }

    #[inline]
    pub fn remove_hook(&mut self) {
        self.execution_data.debug_hook.reset();
    }

    pub fn hook(&mut self) -> Option<FunctionRef> {
        let storage_key = self.execution_data.debug_hook.callback?;
        let heap_key = self.execution_data.heap.create_ref(storage_key);
        Some(FunctionRef(heap_key))
    }

    #[inline]
    pub fn hook_mask(&self) -> HookMask {
        self.execution_data.debug_hook.mask
    }

    #[inline]
    pub fn hook_count(&self) -> usize {
        self.execution_data.debug_hook.after_instructions
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
        self.vm.create_multi()
    }

    #[inline]
    pub fn store_multi(&mut self, multivalue: MultiValue) {
        self.vm.store_multi(multivalue)
    }

    #[inline]
    #[cfg(feature = "instruction_metrics")]
    pub fn instruction_metrics(&mut self) -> Vec<super::InstructionMetrics> {
        self.vm.instruction_metrics()
    }

    #[inline]
    #[cfg(feature = "instruction_metrics")]
    pub fn clear_instruction_metrics(&mut self) {
        self.vm.clear_instruction_metrics();
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
    pub fn registry(&self) -> TableRef {
        self.vm.registry()
    }

    #[inline]
    pub fn default_environment(&self) -> TableRef {
        self.vm.default_environment()
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

    pub fn intern_string(&mut self, bytes: &[u8]) -> StringRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.intern_bytes(gc, bytes);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.vm.try_gc_step();

        StringRef(heap_ref)
    }

    pub fn create_table(&mut self) -> TableRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.create_table(gc, 0, 0);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.vm.try_gc_step();

        TableRef(heap_ref)
    }

    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.create_table(gc, list, map);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        self.vm.try_gc_step();

        TableRef(heap_ref)
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
        self.vm.try_gc_step();

        Ok(FunctionRef(heap_ref))
    }

    pub fn create_function(
        &mut self,
        callback: impl Fn(&mut NativeCallContext, &mut VmContext<'_>) -> Result<(), RuntimeError>
        + Clone
        + 'static,
    ) -> FunctionRef {
        let heap = &mut self.vm.execution_data.heap;
        let gc = &mut self.vm.execution_data.gc;
        let key = heap.store_native_fn_with_key(gc, move |_| {
            let wrapper = move |mut call_ctx, ctx: &mut VmContext| {
                callback(&mut call_ctx, ctx)?;
                Ok(call_ctx)
            };
            wrapper.into()
        });

        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        self.vm.try_gc_step();

        FunctionRef(heap_ref)
    }

    /// Creates a function that can be resumed if a yield occurs.
    /// [VmContext::resume_call_with_state()] must be called within the function's scope to allow yielding for the rest of the call.
    /// The function will be resumed immediately if a yield does not occur.
    ///
    /// Arguments passed to the call context will be from the initial call and `coroutine.resume()`
    ///
    /// The result received by this function is for handling errors raised by resumed sub calls.
    ///
    /// ```
    /// # use red_moon::interpreter::{FunctionRef, MultiValue, Vm};
    /// # use red_moon::errors::RuntimeError;
    /// # use red_moon::languages::lua::std::{impl_basic, impl_coroutine};
    /// # use red_moon::languages::lua::LuaCompiler;
    ///
    /// let mut vm = Vm::default();
    /// let ctx = &mut vm.context();
    ///
    /// impl_basic(ctx)?;
    /// impl_coroutine(ctx)?;
    ///
    /// let for_range = ctx.create_resumable_function(|(call_ctx, result, state), ctx| {
    ///     // forward error
    ///     result?;
    ///
    ///     let mut next_increment = 0;
    ///
    ///     let (mut i, end, f): (i64, i64, FunctionRef) = if state.is_empty() {
    ///         // just called, the result passed in are the args
    ///         call_ctx.get_args(ctx)?
    ///     } else {
    ///         // restore from state
    ///         let (mut i, end, f) = state.unpack(ctx)?;
    ///
    ///         // result is the return value from the call that passed yield to us
    ///         // increment i the same way we would in the loop
    ///         i += call_ctx.get_args::<i64>(ctx)?;
    ///
    ///         (i, end, f)
    ///     };
    ///
    ///     while i < end {
    ///         // set state to allow yielding and provide information on how to resume
    ///         ctx.resume_call_with_state((i, end, f.clone()))?;
    ///
    ///         // call a function that can yield
    ///         // use the return value to increment i
    ///         i += f.call::<_, i64>(i, ctx)?;
    ///     }
    ///
    ///     Ok(())
    /// });
    ///
    /// let env = ctx.default_environment();
    /// env.set("for_range", for_range, ctx)?;
    ///
    /// const SOURCE: &str = r#"
    ///   co = coroutine.create(function()
    ///     for_range(1, 10, function(i)
    ///       if i % 2 == 0 then
    ///         coroutine.yield(i)
    ///       end
    ///
    ///       return 1
    ///     end)
    ///   end)
    ///
    ///   assert(select(2, coroutine.resume(co)) == 2)
    ///   assert(select(2, coroutine.resume(co)) == 4)
    /// "#;
    ///
    /// let compiler = LuaCompiler::default();
    /// let module = compiler.compile(SOURCE).unwrap();
    /// ctx.load_function(file!(), None, module)?.call::<_, ()>((), ctx)?;
    ///
    /// # Ok::<_, RuntimeError>(())
    /// ```
    pub fn create_resumable_function(
        &mut self,
        callback: impl Fn(
            (&mut NativeCallContext, Result<(), RuntimeError>, MultiValue),
            &mut VmContext<'_>,
        ) -> Result<(), RuntimeError>
        + Clone
        + 'static,
    ) -> FunctionRef {
        let heap = &mut self.vm.execution_data.heap;
        let gc = &mut self.vm.execution_data.gc;

        let key = heap.store_native_fn_with_key(gc, move |key| {
            let function_callback = move |call_ctx, ctx: &mut VmContext<'_>| {
                let heap = &mut ctx.vm.execution_data.heap;

                let Some(callback) = heap.resume_callbacks.get(&key) else {
                    return Err(RuntimeErrorData::InvalidInternalState.into());
                };

                let callback = callback.shallow_clone();

                let state = MultiValue {
                    values: Default::default(),
                };

                (callback.callback)((call_ctx, Ok(()), state), ctx)
            };

            function_callback.into()
        });

        let callback = NativeFunction::from(
            move |(mut call_ctx, mut result, mut state): (
                NativeCallContext,
                Result<(), RuntimeError>,
                MultiValue,
            ),
                  ctx: &mut VmContext<'_>| {
                loop {
                    result = callback((&mut call_ctx, result, state), ctx);

                    let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                    if !coroutine_data.continuation_state_set {
                        return result.map(|_| call_ctx);
                    }

                    if let Err(err) = &result
                        && matches!(err.data, RuntimeErrorData::Yield(_))
                    {
                        break;
                    }

                    coroutine_data.continuation_state_set = false;
                    coroutine_data.yield_permissions.allows_yield = false;

                    let cache_pools = &ctx.vm.execution_data.cache_pools;

                    let heap = &mut ctx.vm.execution_data.heap;
                    let state_stack = coroutine_data.continuation_states.pop().unwrap();
                    state = MultiValue::from_value_stack(cache_pools, heap, &state_stack);

                    cache_pools.store_short_value_stack(state_stack);
                    call_ctx.flush_return_values_to_args(ctx.vm);
                }

                let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                if !coroutine_data.yield_permissions.parent_allows_yield
                    && coroutine_data.continuation_state_set
                {
                    // we don't want to leak data here
                    coroutine_data.continuation_states.pop();
                    coroutine_data.continuation_state_set = false;
                }

                result.map(|_| call_ctx)
            },
        );

        let size = std::mem::size_of::<NativeFnObjectKey>() + std::mem::size_of_val(&callback);

        let gc = &mut self.vm.execution_data.gc;

        if heap.resume_callbacks.insert(key, callback).is_none() {
            gc.modify_used_memory(size as _);
        }

        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        self.vm.try_gc_step();

        FunctionRef(heap_ref)
    }

    #[inline]
    pub fn top_coroutine(&mut self) -> Option<CoroutineRef> {
        let coroutine_data = &mut self.vm.execution_data.coroutine_data;
        let key = *coroutine_data.coroutine_stack.last()?;

        Some(CoroutineRef(self.vm.execution_data.heap.create_ref(key)))
    }

    pub fn create_coroutine(
        &mut self,
        function: FunctionRef,
    ) -> Result<CoroutineRef, RuntimeError> {
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
        self.vm.try_gc_step();

        Ok(CoroutineRef(heap_ref))
    }

    /// Returns true if the calling context allows yielding (Coroutine or resumable)
    #[inline]
    pub fn is_yieldable(&self) -> bool {
        let coroutine_data = &self.vm.execution_data.coroutine_data;
        coroutine_data.yield_permissions.parent_allows_yield
    }

    /// Sets values to carry to the next resume of a function created by [VmContext::create_resumable_function()].
    /// Also allows the function to yield if [VmContext::is_yieldable()] is true.
    pub fn resume_call_with_state<S: ForEachValue>(
        &mut self,
        state: S,
    ) -> Result<(), RuntimeError> {
        let execution_data = &mut self.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;

        if coroutine_data.continuation_state_set {
            // take existing state stack and update values
            let mut existing_stack =
                std::mem::take(coroutine_data.continuation_states.last_mut().unwrap());

            existing_stack.clear();
            state.for_each_value(self, |result, _| {
                existing_stack.push(result?.to_stack_value());
                Ok(())
            })?;

            // put the state back
            let execution_data = &mut self.vm.execution_data;
            let coroutine_data = &mut execution_data.coroutine_data;

            std::mem::swap(
                coroutine_data.continuation_states.last_mut().unwrap(),
                &mut existing_stack,
            );
        } else {
            // create a new stack to store state
            let mut stack = execution_data.cache_pools.create_short_value_stack();

            state.for_each_value(self, |result, _| {
                stack.push(result?.to_stack_value());
                Ok(())
            })?;

            let execution_data = &mut self.vm.execution_data;
            let coroutine_data = &mut execution_data.coroutine_data;
            coroutine_data.continuation_states.push(stack);
            coroutine_data.continuation_state_set = true;
            coroutine_data.yield_permissions.allows_yield =
                coroutine_data.yield_permissions.parent_allows_yield;
        }

        Ok(())
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

    #[inline]
    pub fn set_hook(
        &mut self,
        mask: HookMask,
        instruction_count: usize,
        callback: FunctionRef,
    ) -> Result<(), RuntimeErrorData> {
        self.vm.set_hook(mask, instruction_count, callback)
    }

    #[inline]
    pub fn remove_hook(&mut self) {
        self.vm.remove_hook();
    }

    #[inline]
    pub fn hook(&mut self) -> Option<FunctionRef> {
        self.vm.hook()
    }

    #[inline]
    pub fn hook_mask(&self) -> HookMask {
        self.vm.hook_mask()
    }

    #[inline]
    pub fn hook_count(&self) -> usize {
        self.vm.hook_count()
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

        let return_values = match function_value {
            StackValue::NativeFunction(key) => ExecutionContext::call_native_fn(key, args, self.vm),
            StackValue::Function(key) => ExecutionContext::call_interpreted(key, args, self.vm),
            _ => ExecutionContext::call_value(function_value, args, self.vm),
        }?;

        return_values.unpack(self)
    }
}
