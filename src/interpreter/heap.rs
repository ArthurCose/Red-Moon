use super::byte_string::ByteString;
use super::cache_pools::CachePools;
use super::execution::ExecutionContext;
use super::interpreted_function::Function;
use super::metatable_keys::MetatableKeys;
use super::native_function::NativeFunction;
use super::table::Table;
use super::value_stack::StackValue;
use super::Primitive;
use crate::interpreter::cache_pools::RECYCLE_LIMIT;
use crate::vec_cell::VecCell;
use crate::{FastHashMap, FastHashSet};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use std::rc::{Rc, Weak};

#[derive(Clone)]
pub(crate) enum HeapValue {
    StackValue(StackValue),
    Bytes(ByteString),
    Table(Box<Table>),
    NativeFunction(NativeFunction),
    Function(Function),
}

#[derive(Default, Clone)]
struct StrongRef {
    rc: Rc<()>,
}

#[derive(Clone)]
struct RefCounter {
    weak: Weak<()>,
}

impl RefCounter {
    fn from_strong(strong_ref: &StrongRef) -> Self {
        Self {
            weak: Rc::downgrade(&strong_ref.rc),
        }
    }

    fn create_strong(&mut self) -> StrongRef {
        let rc = self.weak.upgrade().unwrap_or_else(|| {
            let rc = Default::default();
            self.weak = Rc::downgrade(&rc);
            rc
        });

        StrongRef { rc }
    }

    fn count(&self) -> usize {
        self.weak.strong_count()
    }
}

slotmap::new_key_type! {
    pub(crate) struct HeapKey;
}

#[derive(Clone)]
pub(crate) struct HeapRef {
    key: HeapKey,
    #[allow(dead_code)]
    strong_ref: StrongRef,
}

impl HeapRef {
    pub(crate) fn key(&self) -> HeapKey {
        self.key
    }
}

impl PartialEq for HeapRef {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapRef {}

impl std::fmt::Debug for HeapRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.key)
    }
}

impl std::hash::Hash for HeapRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

#[derive(Clone)]
enum Mark {
    Black,
    Gray,
}

#[derive(Default, Clone)]
struct GcState {
    traversed_definitions: FastHashSet<usize>,
    // gray + black, excluded keys are white
    marked: slotmap::SecondaryMap<HeapKey, Mark>,
    // gray
    gray_queue: Vec<HeapKey>,
    // element_key/value -> Vec<(table_key, element_key)>
    weak_associations: FastHashMap<HeapKey, Vec<(HeapKey, StackValue)>>,
}

#[derive(Clone)]
pub(crate) struct Heap {
    storage: slotmap::SlotMap<HeapKey, HeapValue>,
    byte_strings: FastHashMap<ByteString, HeapKey>,
    ref_roots: IndexMap<HeapKey, RefCounter, FxBuildHasher>,
    gc_state: GcState,
    #[allow(clippy::vec_box)]
    recycled_tables: Rc<VecCell<Box<Table>>>,
    // feels a bit weird in here and not on VM, but easier to work with here
    string_metatable_ref: HeapRef,
}

impl Default for Heap {
    fn default() -> Self {
        let mut storage: slotmap::SlotMap<_, _> = Default::default();

        // create string metatable
        let string_metatable_key = storage.insert(HeapValue::Table(Default::default()));

        let mut ref_roots = IndexMap::<HeapKey, RefCounter, FxBuildHasher>::default();
        let strong_ref = StrongRef::default();
        ref_roots.insert(string_metatable_key, RefCounter::from_strong(&strong_ref));

        let string_metatable_ref = HeapRef {
            key: string_metatable_key,
            strong_ref,
        };

        Self {
            storage,
            byte_strings: Default::default(),
            ref_roots,
            gc_state: GcState::default(),
            recycled_tables: Default::default(),
            string_metatable_ref,
        }
    }
}

impl Heap {
    pub(crate) fn string_metatable_ref(&self) -> &HeapRef {
        &self.string_metatable_ref
    }

    pub(crate) fn create_table(&mut self, list: usize, map: usize) -> HeapKey {
        // try to recycle
        let mut table = self.recycled_tables.pop().unwrap_or_default();

        table.reserve_list(list);
        table.reserve_map(map);

        self.create(HeapValue::Table(table))
    }

    pub(crate) fn create(&mut self, value: HeapValue) -> HeapKey {
        let key = self.storage.insert(value);
        self.gc_state.acknowledge_write(key);
        key
    }

    pub(crate) fn create_ref(&mut self, heap_key: HeapKey) -> HeapRef {
        let strong_ref = match self.ref_roots.entry(heap_key) {
            indexmap::map::Entry::Occupied(mut entry) => entry.get_mut().create_strong(),
            indexmap::map::Entry::Vacant(entry) => {
                debug_assert!(self.storage.contains_key(heap_key));
                let strong_ref = StrongRef::default();
                entry.insert(RefCounter::from_strong(&strong_ref));
                strong_ref
            }
        };

        HeapRef {
            key: heap_key,
            strong_ref,
        }
    }

    /// Creates a new string in the heap if it doesn't already exist,
    /// otherwise returns a key to an existing string
    pub(crate) fn intern_bytes(&mut self, bytes: &[u8]) -> HeapKey {
        if let Some(&key) = self.byte_strings.get(bytes) {
            return key;
        }

        let string = ByteString::from(bytes);
        let key = self.create(HeapValue::Bytes(string.clone()));
        self.byte_strings.insert(string, key);
        key
    }

    pub(crate) fn intern_bytes_to_ref(&mut self, bytes: &[u8]) -> HeapRef {
        let key = self.intern_bytes(bytes);
        self.create_ref(key)
    }

    pub(crate) fn get(&self, heap_key: HeapKey) -> Option<&HeapValue> {
        self.storage.get(heap_key)
    }

    pub(crate) fn get_mut(&mut self, heap_key: HeapKey) -> Option<&mut HeapValue> {
        self.gc_state.acknowledge_write(heap_key);
        self.storage.get_mut(heap_key)
    }

    #[must_use]
    pub(crate) fn set(&mut self, heap_key: HeapKey, value: HeapValue) -> Option<()> {
        *self.storage.get_mut(heap_key)? = value;

        Some(())
    }

    pub(crate) fn get_metavalue(&self, heap_key: HeapKey, name: StackValue) -> StackValue {
        let Some(value) = self.storage.get(heap_key) else {
            return Primitive::Nil.into();
        };

        let metatable_key = match value {
            HeapValue::Table(table) => {
                let Some(key) = table.metatable() else {
                    return Primitive::Nil.into();
                };

                key
            }
            HeapValue::Bytes(_) => self.string_metatable_ref.key(),
            _ => return Primitive::Nil.into(),
        };

        let Some(metatable_value) = &self.storage.get(metatable_key) else {
            return Primitive::Nil.into();
        };

        let HeapValue::Table(metatable) = &metatable_value else {
            return Primitive::Nil.into();
        };

        metatable.get(name)
    }

    pub(crate) fn get_metamethod(&self, heap_key: HeapKey, name: StackValue) -> Option<HeapKey> {
        let StackValue::HeapValue(method_key) = self.get_metavalue(heap_key, name) else {
            return None;
        };

        if !matches!(
            self.storage.get(method_key)?,
            HeapValue::Function(_) | HeapValue::NativeFunction(_)
        ) {
            return None;
        }

        Some(method_key)
    }

    pub(crate) fn gc_collect(
        &mut self,
        metatable_keys: &MetatableKeys,
        cache_pools: &CachePools,
        execution_stack: &[ExecutionContext],
    ) {
        if !self.gc_state.marking() {
            self.gc_mark_roots();
        }

        self.gc_mark_stack(execution_stack);

        while self.gc_state.marking() {
            self.gc_traverse_gray(metatable_keys, cache_pools);
        }

        self.gc_sweep(cache_pools);
    }

    fn gc_mark_roots(&mut self) {
        // identify ref roots and mark them
        self.ref_roots.retain(|&key, counter| {
            let keep = counter.count() > 0;

            if keep {
                self.gc_state.mark_heap_key_root(key);
            }

            keep
        });
    }

    fn gc_mark_stack(&mut self, execution_stack: &[ExecutionContext]) {
        for execution in execution_stack {
            for value in execution.pending_captures.iter() {
                self.gc_state.mark_stack_value(value);
            }

            for value in execution.value_stack.iter() {
                self.gc_state.mark_stack_value(value);
            }

            for call in &execution.call_stack {
                for value in call.up_values.iter() {
                    self.gc_state.mark_stack_value(value);
                }
            }
        }
    }

    fn gc_traverse_gray(&mut self, metatable_keys: &MetatableKeys, cache_pools: &CachePools) {
        // move gray_queue to work_queue
        let mut gc_work_queue = cache_pools.gc_work_queue.take();
        std::mem::swap(&mut gc_work_queue, &mut self.gc_state.gray_queue);

        for &key in &gc_work_queue {
            self.gc_state.marked.insert(key, Mark::Black);
            self.gc_state
                .traverse_heap_value(metatable_keys, cache_pools, &self.storage, key);
        }

        gc_work_queue.clear();
        cache_pools.gc_work_queue.set(gc_work_queue);
    }

    fn gc_sweep(&mut self, cache_pools: &CachePools) {
        let mut gc_work_queue = cache_pools.gc_work_queue.take();

        for key in self.storage.keys() {
            if !self.gc_state.marked.contains_key(key) {
                gc_work_queue.push(key);
            }
        }

        // todo: handle finalizers and resurrection, likely using gc_work_queue.retain()

        // clear weak associations
        for &key in &gc_work_queue {
            let Some(list) = self.gc_state.weak_associations.remove(&key) else {
                continue;
            };

            for &(table_key, element_key) in &list {
                let HeapValue::Table(table) = &mut self.storage[table_key] else {
                    unreachable!();
                };

                // we need to test to see if the association is still true

                // test key
                if element_key == StackValue::HeapValue(key) {
                    table.set(element_key, StackValue::default());
                    continue;
                }

                // test value
                if table.get(element_key) == StackValue::HeapValue(key) {
                    table.set(element_key, StackValue::default());
                }
            }

            cache_pools.store_weak_associations_list(list);
        }

        // delete
        for &key in &gc_work_queue {
            let value = self.storage.remove(key).unwrap();

            match value {
                HeapValue::Bytes(bytes) => {
                    self.byte_strings.remove(&bytes);
                }
                HeapValue::Table(mut table) => {
                    if self.recycled_tables.len() < RECYCLE_LIMIT {
                        table.reset();
                        self.recycled_tables.push(table);
                    }
                }
                _ => {}
            }
        }

        gc_work_queue.clear();
        cache_pools.gc_work_queue.set(gc_work_queue);

        self.gc_state.marked.clear();
        self.gc_state.traversed_definitions.clear();
        self.gc_state.weak_associations.clear();
    }
}

impl GcState {
    fn marking(&self) -> bool {
        !self.gray_queue.is_empty()
    }

    fn acknowledge_write(&mut self, heap_key: HeapKey) {
        let Some(mark) = self.marked.get_mut(heap_key) else {
            // not marked, we'll handle this through normal traversal
            return;
        };

        if matches!(mark, Mark::Gray) {
            // already in the gray queue
            return;
        }

        // mark as gray again and place in the queue
        *mark = Mark::Gray;
        self.gray_queue.push(heap_key);
    }

    fn mark_table_value(&mut self, value: &StackValue) {
        let StackValue::HeapValue(key) = value else {
            return;
        };

        self.mark_heap_key(*key);
    }

    fn mark_stack_value(&mut self, value: &StackValue) {
        let (StackValue::HeapValue(key) | StackValue::Pointer(key)) = value else {
            return;
        };

        self.mark_heap_key(*key);
    }

    fn mark_heap_key(&mut self, key: HeapKey) {
        if self.marked.contains_key(key) {
            return;
        }

        self.marked.insert(key, Mark::Gray);
        self.gray_queue.push(key);
    }

    fn traverse_heap_value(
        &mut self,
        metatable_keys: &MetatableKeys,
        cache_pools: &CachePools,
        storage: &slotmap::SlotMap<HeapKey, HeapValue>,
        key: HeapKey,
    ) {
        let value = storage.get(key).unwrap();

        match value {
            HeapValue::Function(function) => {
                for value in function.up_values.iter() {
                    self.mark_stack_value(value);
                }

                let definition_key = Rc::as_ptr(&function.definition) as usize;

                if self.traversed_definitions.insert(definition_key) {
                    for key in &function.definition.byte_strings {
                        self.mark_heap_key(*key);
                    }

                    for key in &function.definition.functions {
                        self.mark_heap_key(*key);
                    }
                }
            }
            HeapValue::Table(table) => {
                let mut weak_keys = false;
                let mut weak_values = false;

                if let Some(key) = table.metatable {
                    self.mark_heap_key(key);

                    let HeapValue::Table(metatable) = storage.get(key).unwrap() else {
                        unreachable!();
                    };

                    let mode_key = metatable_keys.mode.0.key.into();
                    let mode_value = metatable.get(mode_key);

                    if let StackValue::HeapValue(mode_heap_key) = mode_value {
                        if let HeapValue::Bytes(bytes) = storage.get(mode_heap_key).unwrap() {
                            weak_keys = bytes.as_bytes().contains(&b'k');
                            weak_values = bytes.as_bytes().contains(&b'v');
                        }
                    }
                }

                if weak_keys {
                    let table_heap_key = key;

                    for &element_key in table.map.keys() {
                        if let StackValue::HeapValue(heap_key) = element_key {
                            self.acknowledge_weak_association(
                                cache_pools,
                                heap_key,
                                table_heap_key,
                                element_key,
                            );
                        }
                    }
                } else {
                    for key in table.map.keys() {
                        self.mark_table_value(key);
                    }
                }

                if weak_values {
                    let table_heap_key = key;

                    for (i, value) in table.list.iter().enumerate() {
                        let StackValue::HeapValue(heap_key) = value else {
                            continue;
                        };

                        self.acknowledge_weak_association(
                            cache_pools,
                            *heap_key,
                            table_heap_key,
                            Primitive::Integer((i + 1) as _).into(),
                        );
                    }

                    for (element_key, value) in &table.map {
                        let StackValue::HeapValue(heap_key) = value else {
                            continue;
                        };

                        self.acknowledge_weak_association(
                            cache_pools,
                            *heap_key,
                            table_heap_key,
                            *element_key,
                        );
                    }
                } else {
                    for value in &table.list {
                        self.mark_table_value(value);
                    }

                    for value in table.map.values() {
                        self.mark_table_value(value);
                    }
                }
            }
            HeapValue::StackValue(StackValue::HeapValue(key)) => self.mark_heap_key(*key),
            _ => {}
        }
    }

    fn mark_heap_key_root(&mut self, key: HeapKey) {
        self.marked.insert(key, Mark::Gray);
        self.gray_queue.push(key);
    }

    fn acknowledge_weak_association(
        &mut self,
        cache_pools: &CachePools,
        heap_key: HeapKey,
        table_heap_key: HeapKey,
        element_key: StackValue,
    ) {
        let list = self
            .weak_associations
            .entry(heap_key)
            .or_insert_with(|| cache_pools.create_weak_associations_list());

        list.push((table_heap_key, element_key));
    }
}
