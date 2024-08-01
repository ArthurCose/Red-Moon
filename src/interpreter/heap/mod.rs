mod garbage_collector;
mod heap_ref;
mod ref_counter;

pub use garbage_collector::*;
pub(crate) use heap_ref::*;

use super::byte_string::ByteString;
use super::coroutine::Coroutine;
use super::interpreted_function::Function;
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

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Storage {
    pub(super) stack_values: slotmap::SlotMap<StackObjectKey, StackValue>,
    pub(super) byte_strings: slotmap::SlotMap<BytesObjectKey, ByteString>,
    pub(super) tables: slotmap::SlotMap<TableObjectKey, Table>,
    pub(super) native_functions: slotmap::SlotMap<NativeFnObjectKey, NativeFunction<MultiValue>>,
    pub(super) functions: slotmap::SlotMap<FnObjectKey, Function>,
    pub(super) coroutines: slotmap::SlotMap<CoroutineObjectKey, Coroutine>,
}

impl Storage {
    pub(crate) const BYTE_STRINGS_TAG: u64 = 0;
    pub(crate) const TABLES_TAG: u64 = 1;
    pub(crate) const NATIVE_FUNCTIONS_TAG: u64 = 2;
    pub(crate) const FUNCTIONS_TAG: u64 = 3;
    pub(crate) const COROUTINES_TAG: u64 = 4;

    pub(crate) fn key_to_id(key: slotmap::KeyData, tag: u64) -> u64 {
        let mask = u32::MAX as u64;
        (key.as_ffi() & mask) | (tag << 32)
    }

    fn keys(&self) -> impl Iterator<Item = StorageKey> + '_ {
        self.stack_values
            .keys()
            .map(StorageKey::from)
            .chain(self.byte_strings.keys().map(StorageKey::from))
            .chain(self.tables.keys().map(StorageKey::from))
            .chain(self.native_functions.keys().map(StorageKey::from))
            .chain(self.functions.keys().map(StorageKey::from))
            .chain(self.coroutines.keys().map(StorageKey::from))
    }
}

macro_rules! object_key_struct {
    ($name:ident, $storage_variant:ident, $stack_variant:ident) => {
        slotmap::new_key_type! {
            pub(crate) struct $name;
        }

        impl From<$name> for StorageKey {
            fn from(key: $name) -> StorageKey {
                StorageKey::$storage_variant(key)
            }
        }

        impl From<&$name> for StorageKey {
            fn from(key: &$name) -> StorageKey {
                StorageKey::$storage_variant(*key)
            }
        }

        impl From<$name> for StackValue {
            fn from(key: $name) -> StackValue {
                StackValue::$stack_variant(key)
            }
        }
    };
}

object_key_struct!(StackObjectKey, StackValue, Pointer);
object_key_struct!(TableObjectKey, Table, Table);
object_key_struct!(BytesObjectKey, Bytes, Bytes);
object_key_struct!(NativeFnObjectKey, NativeFunction, NativeFunction);
object_key_struct!(FnObjectKey, Function, Function);
object_key_struct!(CoroutineObjectKey, Coroutine, Coroutine);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum StorageKey {
    StackValue(StackObjectKey),
    Bytes(BytesObjectKey),
    Table(TableObjectKey),
    NativeFunction(NativeFnObjectKey),
    Function(FnObjectKey),
    Coroutine(CoroutineObjectKey),
}

pub(crate) struct Heap {
    pub(crate) storage: Storage,
    pub(crate) byte_strings: FastHashMap<ByteString, BytesObjectKey>,
    pub(crate) ref_roots: IndexMap<StorageKey, RefCounter, FxBuildHasher>,
    #[cfg(feature = "serde")]
    pub(crate) tags: IndexMap<StackValue, NativeFnObjectKey, FxBuildHasher>,
    pub(crate) resume_callbacks: FastHashMap<
        NativeFnObjectKey,
        NativeFunction<(Result<MultiValue, RuntimeError>, MultiValue)>,
    >,
    pub(crate) recycled_tables: Rc<VecCell<Table>>,
    // feels a bit weird in here and not on VM, but easier to work with here
    string_metatable_ref: HeapRef<TableObjectKey>,
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
        let mut storage = Storage::default();

        // create string metatable
        let string_metatable: Table = Default::default();
        gc.modify_used_memory((string_metatable.heap_size() + std::mem::size_of::<Table>()) as _);
        let string_metatable_key = storage.tables.insert(string_metatable);

        let mut ref_roots = IndexMap::<StorageKey, RefCounter, FxBuildHasher>::default();
        let ref_counter = RefCounter::default();
        let counter_ref = ref_counter.create_counter_ref();
        ref_roots.insert(StorageKey::Table(string_metatable_key), ref_counter.clone());

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

    pub(crate) fn string_metatable_ref(&self) -> &HeapRef<TableObjectKey> {
        &self.string_metatable_ref
    }

    pub(crate) fn store_stack_value(
        &mut self,
        gc: &mut GarbageCollector,
        value: StackValue,
    ) -> StackObjectKey {
        gc.modify_used_memory(std::mem::size_of_val(&value) as _);

        self.storage.stack_values.insert(value)
    }

    pub(crate) fn create_table(
        &mut self,
        gc: &mut GarbageCollector,
        list: usize,
        map: usize,
    ) -> TableObjectKey {
        // try to recycle
        let mut table = self.recycled_tables.pop().unwrap_or_default();

        table.reserve_list(list);
        table.reserve_map(map);

        gc.modify_used_memory((table.heap_size() + std::mem::size_of_val(&table)) as _);

        self.storage.tables.insert(table)
    }

    pub(crate) fn store_interpreted_fn(
        &mut self,
        gc: &mut GarbageCollector,
        function: Function,
    ) -> FnObjectKey {
        gc.modify_used_memory((std::mem::size_of_val(&function) + function.heap_size()) as _);
        self.storage.functions.insert(function)
    }

    pub(crate) fn store_native_fn_with_key(
        &mut self,
        gc: &mut GarbageCollector,
        callback: impl FnOnce(NativeFnObjectKey) -> NativeFunction<MultiValue>,
    ) -> NativeFnObjectKey {
        self.storage.native_functions.insert_with_key(|key| {
            let value = callback(key);
            gc.modify_used_memory(std::mem::size_of_val(&value) as _);
            value
        })
    }

    pub(crate) fn store_coroutine(
        &mut self,
        gc: &mut GarbageCollector,
        coroutine: Coroutine,
    ) -> CoroutineObjectKey {
        gc.modify_used_memory((std::mem::size_of_val(&coroutine) + coroutine.heap_size()) as _);
        self.storage.coroutines.insert(coroutine)
    }

    pub(crate) fn create_ref<K: Copy + Into<StorageKey>>(&mut self, key: K) -> HeapRef<K> {
        let storage_key = key.into();
        let counter_ref = match self.ref_roots.entry(storage_key) {
            indexmap::map::Entry::Occupied(mut entry) => entry.get_mut().create_counter_ref(),
            indexmap::map::Entry::Vacant(entry) => {
                let ref_counter = RefCounter::default();
                let counter_ref = ref_counter.create_counter_ref();
                entry.insert(ref_counter);
                counter_ref
            }
        };

        HeapRef { key, counter_ref }
    }

    /// Creates a new string in the heap if it doesn't already exist,
    /// otherwise returns a key to an existing string
    pub(crate) fn intern_bytes(
        &mut self,
        gc: &mut GarbageCollector,
        bytes: &[u8],
    ) -> BytesObjectKey {
        if let Some(&key) = self.byte_strings.get(bytes) {
            return key;
        }

        let string = ByteString::from(bytes);
        let key = self.storage.byte_strings.insert(string.clone());
        gc.modify_used_memory((string.heap_size() + std::mem::size_of_val(&string)) as _);
        self.byte_strings.insert(string, key);
        key
    }

    pub(crate) fn intern_bytes_to_ref(
        &mut self,
        gc: &mut GarbageCollector,
        bytes: &[u8],
    ) -> HeapRef<BytesObjectKey> {
        let key = self.intern_bytes(gc, bytes);
        self.create_ref(key)
    }

    pub(crate) fn get_bytes(&self, key: BytesObjectKey) -> Option<&ByteString> {
        self.storage.byte_strings.get(key)
    }

    pub(crate) fn get_stack_value(&self, key: StackObjectKey) -> Option<&StackValue> {
        self.storage.stack_values.get(key)
    }

    pub(crate) fn get_table(&self, key: TableObjectKey) -> Option<&Table> {
        self.storage.tables.get(key)
    }

    pub(crate) fn get_table_mut(
        &mut self,
        gc: &mut GarbageCollector,
        key: TableObjectKey,
    ) -> Option<&mut Table> {
        gc.acknowledge_write(key.into());
        self.storage.tables.get_mut(key)
    }

    pub(crate) fn get_interpreted_fn(&self, key: FnObjectKey) -> Option<&Function> {
        self.storage.functions.get(key)
    }

    pub(crate) fn get_native_fn(
        &self,
        key: NativeFnObjectKey,
    ) -> Option<&NativeFunction<MultiValue>> {
        self.storage.native_functions.get(key)
    }

    pub(crate) fn get_coroutine(&self, key: CoroutineObjectKey) -> Option<&Coroutine> {
        self.storage.coroutines.get(key)
    }

    pub(crate) fn get_coroutine_mut(
        &mut self,
        gc: &mut GarbageCollector,
        key: CoroutineObjectKey,
    ) -> Option<&mut Coroutine> {
        gc.acknowledge_write(key.into());
        self.storage.coroutines.get_mut(key)
    }

    pub(crate) fn get_coroutine_mut_unmarked(
        &mut self,
        key: CoroutineObjectKey,
    ) -> Option<&mut Coroutine> {
        self.storage.coroutines.get_mut(key)
    }

    pub(crate) fn set_stack_value(
        &mut self,
        gc: &mut GarbageCollector,
        key: StackObjectKey,
        value: StackValue,
    ) {
        gc.acknowledge_write(key.into());
        self.storage.stack_values[key] = value;
    }

    #[cfg(feature = "serde")]
    pub(crate) fn rehydrate(
        &mut self,
        gc: &mut GarbageCollector,
        key: NativeFnObjectKey,
        value: NativeFunction<MultiValue>,
    ) {
        gc.acknowledge_write(key.into());
        self.storage.native_functions[key] = value;
    }

    pub(crate) fn get_metavalue(&self, value: StackValue, name: StackValue) -> StackValue {
        let metatable_key = match value {
            StackValue::Table(key) => {
                let table = &self.storage.tables[key];

                let Some(key) = table.metatable() else {
                    return StackValue::Nil;
                };

                key
            }
            StackValue::Bytes(_) => self.string_metatable_ref.key(),
            _ => return StackValue::Nil,
        };

        let metatable = self.storage.tables.get(metatable_key).unwrap();

        metatable.get(name)
    }

    pub(crate) fn get_metamethod(&self, value: StackValue, name: StackValue) -> Option<StackValue> {
        let value = self.get_metavalue(value, name);

        if !matches!(
            value,
            StackValue::Function(_) | StackValue::NativeFunction(_)
        ) {
            return None;
        }

        Some(value)
    }
}
