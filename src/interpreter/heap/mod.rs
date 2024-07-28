mod garbage_collector;
mod heap_ref;
mod heap_value;
mod ref_counter;

pub use garbage_collector::*;
pub(crate) use heap_ref::*;
pub(crate) use heap_value::*;

use super::byte_string::ByteString;
use super::native_function::NativeFunction;
use super::table::Table;
use super::value_stack::StackValue;
use super::MultiValue;
use crate::errors::RuntimeError;
use crate::vec_cell::VecCell;
use crate::FastHashMap;
use indexmap::IndexMap;
use ref_counter::*;
use rustc_hash::FxBuildHasher;
use std::rc::Rc;

slotmap::new_key_type! {
    pub(crate) struct HeapKey;
}

pub(crate) struct Heap {
    pub(crate) storage: slotmap::SlotMap<HeapKey, HeapValue>,
    pub(crate) byte_strings: FastHashMap<ByteString, HeapKey>,
    pub(crate) ref_roots: IndexMap<HeapKey, RefCounter, FxBuildHasher>,
    #[cfg(feature = "serde")]
    pub(crate) tags: IndexMap<StackValue, HeapKey, FxBuildHasher>,
    pub(crate) resume_callbacks:
        FastHashMap<HeapKey, NativeFunction<(Result<MultiValue, RuntimeError>, MultiValue)>>,
    pub(crate) recycled_tables: Rc<VecCell<Box<Table>>>,
    // feels a bit weird in here and not on VM, but easier to work with here
    string_metatable_ref: HeapRef,
}

impl Clone for Heap {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            byte_strings: self.byte_strings.clone(),
            ref_roots: self.ref_roots.clone(),
            #[cfg(feature = "serde")]
            tags: self.tags.clone(),
            resume_callbacks: self.resume_callbacks.clone(),
            recycled_tables: self.recycled_tables.clone(),
            string_metatable_ref: self.string_metatable_ref.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.storage.clone_from(&source.storage);
        self.byte_strings.clone_from(&source.byte_strings);
        self.ref_roots.clone_from(&source.ref_roots);
        #[cfg(feature = "serde")]
        self.tags.clone_from(&source.tags);
        self.resume_callbacks.clone_from(&source.resume_callbacks);
        self.recycled_tables.clone_from(&source.recycled_tables);
        self.string_metatable_ref
            .clone_from(&source.string_metatable_ref);
    }
}

impl Heap {
    pub(crate) fn new(gc: &mut GarbageCollector) -> Self {
        let mut storage: slotmap::SlotMap<_, _> = Default::default();

        // create string metatable
        let string_metatable_heap_value = HeapValue::Table(Default::default());
        gc.modify_used_memory(string_metatable_heap_value.gc_size() as _);
        let string_metatable_key = storage.insert(string_metatable_heap_value);

        let mut ref_roots = IndexMap::<HeapKey, RefCounter, FxBuildHasher>::default();
        let ref_counter = RefCounter::default();
        let counter_ref = ref_counter.create_counter_ref();
        ref_roots.insert(string_metatable_key, ref_counter.clone());

        let string_metatable_ref = HeapRef {
            key: string_metatable_key,
            counter_ref,
        };

        Self {
            storage,
            byte_strings: Default::default(),
            ref_roots,
            #[cfg(feature = "serde")]
            tags: Default::default(),
            resume_callbacks: Default::default(),
            recycled_tables: Default::default(),
            string_metatable_ref,
        }
    }

    pub(crate) fn string_metatable_ref(&self) -> &HeapRef {
        &self.string_metatable_ref
    }

    pub(crate) fn create_table(
        &mut self,
        gc: &mut GarbageCollector,
        list: usize,
        map: usize,
    ) -> HeapKey {
        // try to recycle
        let mut table = self.recycled_tables.pop().unwrap_or_default();

        table.reserve_list(list);
        table.reserve_map(map);

        self.create(gc, HeapValue::Table(table))
    }

    pub(crate) fn create(&mut self, gc: &mut GarbageCollector, value: HeapValue) -> HeapKey {
        gc.modify_used_memory(value.gc_size() as _);

        let key = self.storage.insert(value);
        gc.acknowledge_write(key);
        key
    }

    pub(crate) fn create_ref(&mut self, heap_key: HeapKey) -> HeapRef {
        let counter_ref = match self.ref_roots.entry(heap_key) {
            indexmap::map::Entry::Occupied(mut entry) => entry.get_mut().create_counter_ref(),
            indexmap::map::Entry::Vacant(entry) => {
                debug_assert!(self.storage.contains_key(heap_key));
                let ref_counter = RefCounter::default();
                let counter_ref = ref_counter.create_counter_ref();
                entry.insert(ref_counter);
                counter_ref
            }
        };

        HeapRef {
            key: heap_key,
            counter_ref,
        }
    }

    /// Creates a new string in the heap if it doesn't already exist,
    /// otherwise returns a key to an existing string
    pub(crate) fn intern_bytes(&mut self, gc: &mut GarbageCollector, bytes: &[u8]) -> HeapKey {
        if let Some(&key) = self.byte_strings.get(bytes) {
            return key;
        }

        let string = ByteString::from(bytes);
        let key = self.create(gc, HeapValue::Bytes(string.clone()));
        self.byte_strings.insert(string, key);
        key
    }

    pub(crate) fn intern_bytes_to_ref(
        &mut self,
        gc: &mut GarbageCollector,
        bytes: &[u8],
    ) -> HeapRef {
        let key = self.intern_bytes(gc, bytes);
        self.create_ref(key)
    }

    pub(crate) fn get(&self, heap_key: HeapKey) -> Option<&HeapValue> {
        self.storage.get(heap_key)
    }

    pub(crate) fn get_mut(
        &mut self,
        gc: &mut GarbageCollector,
        heap_key: HeapKey,
    ) -> Option<&mut HeapValue> {
        gc.acknowledge_write(heap_key);
        self.storage.get_mut(heap_key)
    }

    pub(crate) fn get_mut_unmarked(&mut self, heap_key: HeapKey) -> Option<&mut HeapValue> {
        self.storage.get_mut(heap_key)
    }

    pub(crate) fn set(&mut self, gc: &mut GarbageCollector, heap_key: HeapKey, value: HeapValue) {
        gc.acknowledge_write(heap_key);
        self.storage[heap_key] = value;
    }

    pub(crate) fn get_metavalue(&self, heap_key: HeapKey, name: StackValue) -> StackValue {
        let Some(value) = self.storage.get(heap_key) else {
            return StackValue::Nil;
        };

        let metatable_key = match value {
            HeapValue::Table(table) => {
                let Some(key) = table.metatable() else {
                    return StackValue::Nil;
                };

                key
            }
            HeapValue::Bytes(_) => self.string_metatable_ref.key(),
            _ => return StackValue::Nil,
        };

        let Some(metatable_value) = &self.storage.get(metatable_key) else {
            return StackValue::Nil;
        };

        let HeapValue::Table(metatable) = &metatable_value else {
            return StackValue::Nil;
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
}
