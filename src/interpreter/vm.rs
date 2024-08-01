use super::cache_pools::CachePools;
use super::coroutine::{Coroutine, YieldPermissions};
use super::execution::ExecutionContext;
use super::heap::{
    CoroutineObjectKey, GarbageCollector, GarbageCollectorConfig, Heap, NativeFnObjectKey,
};
use super::metatable_keys::MetatableKeys;
use super::native_function::NativeFunction;
use super::value_stack::{StackValue, ValueStack};
use super::{
    Continuation, CoroutineRef, FromMulti, FunctionRef, IntoMulti, Module, MultiValue, StringRef,
    TableRef,
};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::interpreted_function::{Function, FunctionDefinition};
use crate::FastHashMap;
use downcast::downcast;
use std::any::TypeId;
use std::rc::Rc;

#[cfg(feature = "instruction_exec_counts")]
use super::instruction::InstructionCounter;

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
    #[cfg(feature = "instruction_exec_counts")]
    pub(crate) instruction_counter: InstructionCounter,
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
        // reset, since there's no active call on the new vm
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
        super::serde_byte_string_rc::enable();
        super::serde_function_definition_rc::enable();
        super::serde_value_stack_rc::enable();

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
        super::serde_byte_string_rc::reset();
        super::serde_function_definition_rc::reset();
        super::serde_value_stack_rc::reset();

        result
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for Vm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use crate::interpreter::heap::{BytesObjectKey, NativeFnObjectKey, Storage};
        use crate::interpreter::ByteString;
        use indexmap::IndexMap;
        use rustc_hash::FxBuildHasher;

        #[derive(Deserialize)]
        #[serde(rename = "Vm")]
        struct Data {
            limits: VmLimits,
            gc: GarbageCollector,
            heap_storage: Storage,
            tags: IndexMap<StackValue, NativeFnObjectKey, FxBuildHasher>,
            byte_strings: FastHashMap<ByteString, BytesObjectKey>,
        }

        // enable deduplication
        super::serde_byte_string_rc::enable();
        super::serde_function_definition_rc::enable();
        super::serde_value_stack_rc::enable();

        // deserialize
        let result = Deserialize::deserialize(deserializer);

        // reset + disable deduplication
        super::serde_byte_string_rc::reset();
        super::serde_function_definition_rc::reset();
        super::serde_value_stack_rc::reset();

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
                #[cfg(feature = "instruction_exec_counts")]
                instruction_counter: Default::default(),
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
        let heap = &mut self.execution_data.heap;

        gc.modify_used_memory(bytes as _);

        if gc.should_step() {
            gc.step(
                &self.execution_data.metatable_keys,
                &self.execution_data.cache_pools,
                heap,
                &self.execution_stack,
                &self.execution_data.coroutine_data,
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
            heap,
            &self.execution_stack,
            &self.execution_data.coroutine_data,
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
        let call = context.call_stack.last()?;
        let env_index = call.function_definition.env?;
        let StackValue::Table(env_key) = call.up_values.get(env_index) else {
            return None;
        };

        let heap = &mut self.vm.execution_data.heap;

        Some(TableRef(heap.create_ref(env_key)))
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
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
            );
        }

        StringRef(heap_ref)
    }

    pub fn create_table(&mut self) -> TableRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.create_table(gc, 0, 0);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
            );
        }

        TableRef(heap_ref)
    }

    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let heap_key = heap.create_table(gc, list, map);
        let heap_ref = heap.create_ref(heap_key);

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
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

        let gc = &mut self.vm.execution_data.gc;
        let heap = &mut self.vm.execution_data.heap;
        let environment = environment
            .map(|table| table.0.key().into())
            .unwrap_or(self.vm.default_environment.0.key().into());

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

            let key = heap.store_interpreted_fn(
                gc,
                Function {
                    up_values: up_values.into(),
                    definition: Rc::new(FunctionDefinition {
                        label: label.clone(),
                        env: chunk.env,
                        up_values: chunk.up_values,
                        byte_strings,
                        numbers: chunk.numbers,
                        functions,
                        instructions: chunk.instructions,
                        source_map: chunk.source_map,
                    }),
                },
            );

            keys.push(key);
        }

        let key = keys.get(module.main).ok_or(RuntimeErrorData::MissingMain)?;
        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
            );
        }

        Ok(FunctionRef(heap_ref))
    }

    pub fn create_function(
        &mut self,
        callback: impl Fn(MultiValue, &mut VmContext) -> Result<MultiValue, RuntimeError>
            + Clone
            + 'static,
    ) -> FunctionRef {
        let heap = &mut self.vm.execution_data.heap;
        let gc = &mut self.vm.execution_data.gc;
        let key = heap.store_native_fn_with_key(gc, |_| callback.into());

        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
            );
        }

        FunctionRef(heap_ref)
    }

    /// Creates a function that can be resumed if a yield occurs.
    /// Allows coroutine yielding within the scope of calls to this function.
    ///
    /// If the function was just called, the arguments will be passed through the first value in the tuple
    /// the second value will be empty.
    ///
    /// If [VmContext::resume_call_with_state()] is called,
    /// this function will be resumed using it's own return results after completing as long as it isn't yielding.
    ///
    /// If the function was resumed, the result of the last call made before the yield occured will be passed as the first value in the tuple,
    /// the second value will be any data passed to [VmContext::resume_call_with_state()]
    ///
    /// If a yield was directly created by this function,
    /// the first value in the tuple will be the arguments passed to [CoroutineRef::resume()]
    /// and the second value will be any data passed to [VmContext::resume_call_with_state()]
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
    /// let for_range = ctx.create_resumable_function(|(result, state), ctx| {
    ///     let mut next_increment = 0;
    ///
    ///     let (mut i, end, f): (i64, i64, FunctionRef) = if state.is_empty() {
    ///         // just called, the result passed in are the args
    ///         let args = result?;
    ///         args.unpack_args(ctx)?
    ///     } else {
    ///         // restore from state
    ///         let (mut i, end, f) = state.unpack(ctx)?;
    ///
    ///         // result is the return value from the call that passed yield to us
    ///         // increment i the same way we would in the loop
    ///         i += result?.unpack_args::<i64>(ctx)?;
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
    ///     MultiValue::pack((), ctx)
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
    /// ctx.load_function(file!(), None, module)?.call((), ctx)?;
    ///
    /// # Ok::<_, RuntimeError>(())
    /// ```
    pub fn create_resumable_function(
        &mut self,
        callback: impl Fn(
                (Result<MultiValue, RuntimeError>, MultiValue),
                &mut VmContext,
            ) -> Result<MultiValue, RuntimeError>
            + Clone
            + 'static,
    ) -> FunctionRef {
        let heap = &mut self.vm.execution_data.heap;
        let gc = &mut self.vm.execution_data.gc;

        let key = heap.store_native_fn_with_key(gc, move |key| {
            let function_callback = move |args, ctx: &mut VmContext| {
                let heap = &mut ctx.vm.execution_data.heap;

                let Some(callback) = heap.resume_callbacks.get(&key) else {
                    return Err(RuntimeErrorData::InvalidInternalState.into());
                };

                let callback = callback.shallow_clone();

                let state = MultiValue {
                    values: Default::default(),
                };

                (callback.callback)((Ok(args), state), ctx)
            };

            function_callback.into()
        });

        let callback = NativeFunction::from(
            move |(mut result, mut state): (Result<MultiValue, RuntimeError>, MultiValue),
                  ctx: &mut VmContext| {
                loop {
                    result = callback((result, state), ctx);

                    let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                    if !coroutine_data.continuation_state_set {
                        return result;
                    }

                    if let Err(err) = &result {
                        if matches!(err.data, RuntimeErrorData::Yield(_)) {
                            break;
                        }
                    }

                    coroutine_data.continuation_state_set = false;
                    coroutine_data.yield_permissions.allows_yield = false;

                    let cache_pools = &ctx.vm.execution_data.cache_pools;

                    let heap = &mut ctx.vm.execution_data.heap;
                    let state_stack = coroutine_data.continuation_states.pop().unwrap();
                    state = MultiValue::from_value_stack(cache_pools, heap, &state_stack);

                    cache_pools.store_short_value_stack(state_stack);
                }

                let coroutine_data = &mut ctx.vm.execution_data.coroutine_data;

                if !coroutine_data.yield_permissions.parent_allows_yield
                    && coroutine_data.continuation_state_set
                {
                    // we don't want to leak data here
                    coroutine_data.continuation_states.pop();
                    coroutine_data.continuation_state_set = false;
                }

                result
            },
        );

        let size = std::mem::size_of::<NativeFnObjectKey>() + std::mem::size_of_val(&callback);

        let gc = &mut self.vm.execution_data.gc;

        if heap.resume_callbacks.insert(key, callback).is_none() {
            gc.modify_used_memory(size as _);
        }

        let heap_ref = heap.create_ref(key.into());

        // test after creating ref to avoid immediately collecting the generated value
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
            );
        }

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
        if gc.should_step() {
            gc.step(
                &self.vm.execution_data.metatable_keys,
                &self.vm.execution_data.cache_pools,
                heap,
                &self.vm.execution_stack,
                &self.vm.execution_data.coroutine_data,
            );
        }

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
    pub fn resume_call_with_state<S: IntoMulti>(&mut self, state: S) -> Result<(), RuntimeError> {
        let multi = state.into_multi(self)?;

        let execution_data = &mut self.vm.execution_data;
        let coroutine_data = &mut execution_data.coroutine_data;

        let mut stack = execution_data.cache_pools.create_short_value_stack();
        multi.extend_stack(&mut stack);

        if coroutine_data.continuation_state_set {
            *coroutine_data.continuation_states.last_mut().unwrap() = stack;
        } else {
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

    pub(crate) fn call_function_key<A: IntoMulti, R: FromMulti>(
        &mut self,
        function_value: StackValue,
        args: A,
    ) -> Result<R, RuntimeError> {
        let args = args.into_multi(self)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        let heap = &self.vm.execution_data.heap;

        for value in &args.values {
            value.test_validity(heap)?;
        }

        let result = match function_value {
            StackValue::NativeFunction(key) => {
                let Some(func) = heap.get_native_fn(key) else {
                    return Err(RuntimeErrorData::InvalidRef.into());
                };

                func.shallow_clone().call(key, args, self)
            }
            StackValue::Function(key) => ExecutionContext::new_function_call(key, args, self.vm)
                .and_then(|execution| {
                    self.vm.execution_stack.push(execution);
                    ExecutionContext::resume(self.vm)
                }),
            _ => ExecutionContext::new_value_call(function_value, args, self.vm).and_then(
                |execution| {
                    self.vm.execution_stack.push(execution);
                    ExecutionContext::resume(self.vm)
                },
            ),
        };

        let multi = result?;
        R::from_multi(multi, self)
    }
}
