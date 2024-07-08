use super::heap::{Heap, HeapKey, HeapRef, HeapValue};
use super::metatable_keys::MetatableKeys;
use super::thread::Thread;
use super::value_stack::ValueStack;
use super::{FromMulti, FunctionRef, IntoMulti, Module, MultiValue, StringRef, TableRef};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::interpreted_function::{Function, FunctionDefinition};
use crate::vec_cell::VecCell;
use crate::FastHashMap;
use downcast::downcast;
use std::any::TypeId;
use std::rc::Rc;

#[cfg(feature = "instruction_exec_counts")]
use super::instruction::{Instruction, InstructionCounter};

const RECYCLE_LIMIT: usize = 64;

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

pub struct Vm {
    limits: VmLimits,
    heap: Heap,
    default_environment: HeapRef,
    metatable_keys: Rc<MetatableKeys>,
    recycled_multivalues: Rc<VecCell<MultiValue>>,
    recycled_value_stacks: Rc<VecCell<ValueStack>>,
    recycled_short_value_stacks: Rc<VecCell<ValueStack>>,
    tracked_stack_size: usize,
    app_data: FastHashMap<TypeId, Box<dyn AppData>>,
    #[cfg(feature = "instruction_exec_counts")]
    instruction_counter: Rc<Cell<InstructionCounter>>,
}

impl Clone for Vm {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits.clone(),
            heap: self.heap.clone(),
            default_environment: self.default_environment.clone(),
            metatable_keys: self.metatable_keys.clone(),
            recycled_multivalues: self.recycled_multivalues.clone(),
            recycled_value_stacks: self.recycled_value_stacks.clone(),
            recycled_short_value_stacks: self.recycled_short_value_stacks.clone(),
            // reset to 0, since there's no active call on the new vm
            tracked_stack_size: 0,
            app_data: self.app_data.clone(),
            #[cfg(feature = "instruction_exec_counts")]
            instruction_counter: self.instruction_counter.clone(),
        }
    }
}

impl Vm {
    pub fn new() -> Self {
        let mut heap = Heap::default();

        let default_environment = heap.create(HeapValue::Table(Default::default()));
        let default_environment = heap.create_ref(default_environment).unwrap();

        let metatable_keys = MetatableKeys::new(&mut heap);

        Self {
            limits: Default::default(),
            heap,
            default_environment,
            metatable_keys: Rc::new(metatable_keys),
            recycled_multivalues: Default::default(),
            recycled_value_stacks: Default::default(),
            recycled_short_value_stacks: Default::default(),
            tracked_stack_size: 0,
            app_data: Default::default(),
            #[cfg(feature = "instruction_exec_counts")]
            instruction_counter: Default::default(),
        }
    }

    pub(crate) fn heap(&self) -> &Heap {
        &self.heap
    }

    pub(crate) fn heap_mut(&mut self) -> &mut Heap {
        &mut self.heap
    }

    pub(crate) fn tracked_stack_size(&self) -> usize {
        self.tracked_stack_size
    }

    pub(crate) fn update_stack_size(&mut self, stack_size: usize) {
        self.tracked_stack_size = stack_size;
    }

    #[cfg(feature = "instruction_exec_counts")]
    pub(crate) fn track_instruction(&mut self, instruction: Instruction) {
        let mut instruction_counter = self.instruction_counter.take();
        instruction_counter.track(instruction);
        self.instruction_counter.set(instruction_counter);
    }

    #[cfg(feature = "instruction_exec_counts")]
    pub fn instruction_exec_counts(&mut self) -> Vec<(&'static str, usize)> {
        let instruction_counter = self.instruction_counter.take();
        let mut results = instruction_counter.data().collect::<Vec<_>>();
        self.instruction_counter.set(instruction_counter);

        // sort by count reversed
        results.sort_by_key(|(_, count)| usize::MAX - count);
        results
    }

    #[cfg(feature = "instruction_exec_counts")]
    pub fn clear_instruction_exec_counts(&mut self) {
        let mut instruction_counter = self.instruction_counter.take();
        instruction_counter.clear();
        self.instruction_counter.set(instruction_counter);
    }

    #[inline]
    pub fn limits(&self) -> &VmLimits {
        &self.limits
    }

    #[inline]
    pub fn set_limits(&mut self, limits: VmLimits) {
        self.limits = limits;
    }

    #[inline]
    pub fn default_environment(&self) -> TableRef {
        TableRef(self.default_environment.clone())
    }

    #[inline]
    pub fn string_metatable(&self) -> TableRef {
        TableRef(self.heap.string_metatable_ref().clone())
    }

    #[inline]
    pub fn metatable_keys(&self) -> &MetatableKeys {
        &self.metatable_keys
    }

    pub fn create_multi(&mut self) -> MultiValue {
        self.recycled_multivalues
            .pop()
            .unwrap_or_else(|| MultiValue { values: Vec::new() })
    }

    pub fn store_multi(&mut self, mut multivalue: MultiValue) {
        if self.recycled_multivalues.len() < RECYCLE_LIMIT {
            multivalue.clear();
            self.recycled_multivalues.push(multivalue);
        }
    }

    pub(crate) fn create_value_stack(&mut self) -> ValueStack {
        self.recycled_value_stacks.pop().unwrap_or_default()
    }

    pub(crate) fn store_value_stack(&mut self, mut value_stack: ValueStack) {
        if self.recycled_value_stacks.len() < RECYCLE_LIMIT {
            value_stack.clear();
            self.recycled_value_stacks.push(value_stack);
        }
    }

    pub(crate) fn create_short_value_stack(&mut self) -> ValueStack {
        self.recycled_short_value_stacks.pop().unwrap_or_default()
    }

    pub(crate) fn store_short_value_stack(&mut self, mut value_stack: ValueStack) {
        if self.recycled_short_value_stacks.len() < RECYCLE_LIMIT {
            value_stack.clear();
            self.recycled_short_value_stacks.push(value_stack);
        }
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
        let heap_key = self.heap.intern_bytes(bytes);
        let heap_ref = self
            .heap
            .create_ref(heap_key)
            .expect("just created the key");

        StringRef(heap_ref)
    }

    pub fn create_table(&mut self) -> TableRef {
        let heap_key = self.heap.create_table(0, 0);
        let heap_ref = self.heap.create_ref(heap_key).unwrap();
        TableRef(heap_ref)
    }

    pub fn create_table_with_capacity(&mut self, list: usize, map: usize) -> TableRef {
        let heap_key = self.heap.create_table(list, map);
        let heap_ref = self.heap.create_ref(heap_key).unwrap();
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

        let environment = environment
            .map(|table| table.0.key().into())
            .unwrap_or(self.default_environment.key().into());

        let mut keys = Vec::with_capacity(module.chunks.len());

        for (i, block) in module.chunks.into_iter().enumerate() {
            let byte_strings = block
                .byte_strings
                .into_iter()
                .map(|bytes| self.heap.intern_bytes(bytes.as_ref()))
                .collect();

            let functions = block
                .dependencies
                .into_iter()
                .map(|index| keys[index])
                .collect();

            let mut up_values = ValueStack::default();

            if i == module.main {
                up_values.push(environment);
            }

            let key = self.heap.create(HeapValue::Function(Function {
                up_values: up_values.into(),
                definition: Rc::new(FunctionDefinition {
                    label: label.clone(),
                    byte_strings,
                    numbers: block.numbers,
                    functions,
                    instructions: block.instructions,
                    source_map: block.source_map,
                }),
            }));

            keys.push(key);
        }

        let key = keys.get(module.main).ok_or(RuntimeErrorData::MissingMain)?;
        let heap_ref = self.heap.create_ref(*key).unwrap();

        Ok(FunctionRef(heap_ref))
    }

    pub fn create_native_function(
        &mut self,
        callback: impl Fn(MultiValue, &mut Vm) -> Result<MultiValue, RuntimeError> + Clone + 'static,
    ) -> FunctionRef {
        let key = self.heap.create(HeapValue::NativeFunction(callback.into()));

        let heap_ref = self.heap.create_ref(key).unwrap();

        FunctionRef(heap_ref)
    }

    pub(crate) fn call_function_key<A: IntoMulti, R: FromMulti>(
        &mut self,
        function_key: HeapKey,
        args: A,
    ) -> Result<R, RuntimeError> {
        let args = args.into_multi(self)?;

        // must test validity of every arg, since invalid keys in the vm will cause a panic
        for value in &args.values {
            value.test_validity(&self.heap)?;
        }

        let Some(value) = self.heap.get(function_key) else {
            return Err(RuntimeErrorData::NotAFunction.into());
        };

        let old_stack_size = self.tracked_stack_size;

        let result = match value {
            HeapValue::NativeFunction(func) => func.shallow_clone().call(args, self),
            HeapValue::Function(func) => {
                let thread = Thread::new_function_call(function_key, func.clone(), args, self);
                thread.resume(self)
            }
            _ => Thread::new_value_call(function_key.into(), args, self)
                .map_err(RuntimeError::from)
                .and_then(|thread| thread.resume(self)),
        };

        self.tracked_stack_size = old_stack_size;

        let multi = result?;
        R::from_multi(multi, self)
    }

    pub fn gc_collect(&mut self) {
        self.heap.collect_garbage();
    }
}

impl Default for Vm {
    fn default() -> Self {
        Self::new()
    }
}
