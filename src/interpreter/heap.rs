use super::coroutine::Coroutine;
use super::interpreted_function::Function;
use super::native_function::{NativeCallContext, NativeFunction};
use super::ref_counter::*;
use super::table::Table;
use super::value_stack::StackValue;
use crate::errors::RuntimeError;
use crate::interpreter::garbage_collector::GarbageCollector;
use crate::values::{ByteString, MultiValue, NativeValue};
use crate::vec_cell::VecCell;
use crate::{BuildFastHasher, FastHashMap};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::rc::Rc;

pub(crate) use crate::interpreter::heap_ref::HeapRef;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Storage {
    pub(super) stack_values: slotmap::SlotMap<StackObjectKey, StackValue>,
    pub(super) byte_strings: slotmap::SlotMap<BytesObjectKey, ByteString>,
    pub(super) tables: slotmap::SlotMap<TableObjectKey, Table>,
    pub(super) metatables: slotmap::SecondaryMap<TableObjectKey, TableObjectKey>,
    pub(super) native_functions:
        slotmap::SlotMap<NativeFnObjectKey, NativeFunction<NativeCallContext>>,
    pub(super) functions: slotmap::SlotMap<FnObjectKey, Function>,
    pub(super) coroutines: slotmap::SlotMap<CoroutineObjectKey, Coroutine>,
    pub(super) captures: HashMap<NativeFnObjectKey, Box<dyn NativeValue>>,
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

    pub(crate) fn keys(&self) -> impl Iterator<Item = StorageKey> + '_ {
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

        impl $name {
            pub(crate) fn as_ffi(self) -> u64 {
                self.0.as_ffi()
            }

            pub(crate) fn from_ffi(n: u64) -> Self {
                Self(slotmap::KeyData::from_ffi(n))
            }
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

/// Faster than StorageKey when used as a HashMap Key, since the variant and value can be compared directly
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct FastStorageKey {
    variant: u8,
    value: u64,
}

impl FastStorageKey {
    const VARIANT_STACK_VALUE: u8 = 0;
    const VARIANT_BYTES: u8 = 1;
    const VARIANT_TABLE: u8 = 2;
    const VARIANT_NATIVE_FN: u8 = 3;
    const VARIANT_FN: u8 = 4;
    const VARIANT_COROUTINE: u8 = 5;
}

impl From<StorageKey> for FastStorageKey {
    fn from(value: StorageKey) -> Self {
        fn from_pair(variant: u8, value: u64) -> FastStorageKey {
            FastStorageKey { variant, value }
        }

        match value {
            StorageKey::StackValue(key) => from_pair(Self::VARIANT_STACK_VALUE, key.as_ffi()),
            StorageKey::Bytes(key) => from_pair(Self::VARIANT_BYTES, key.as_ffi()),
            StorageKey::Table(key) => from_pair(Self::VARIANT_TABLE, key.as_ffi()),
            StorageKey::NativeFunction(key) => from_pair(Self::VARIANT_NATIVE_FN, key.as_ffi()),
            StorageKey::Function(key) => from_pair(Self::VARIANT_FN, key.as_ffi()),
            StorageKey::Coroutine(key) => from_pair(Self::VARIANT_COROUTINE, key.as_ffi()),
        }
    }
}

impl From<FastStorageKey> for StorageKey {
    fn from(key: FastStorageKey) -> Self {
        match key.variant {
            FastStorageKey::VARIANT_STACK_VALUE => {
                StorageKey::StackValue(StackObjectKey::from_ffi(key.value))
            }
            FastStorageKey::VARIANT_BYTES => StorageKey::Bytes(BytesObjectKey::from_ffi(key.value)),
            FastStorageKey::VARIANT_TABLE => StorageKey::Table(TableObjectKey::from_ffi(key.value)),
            FastStorageKey::VARIANT_NATIVE_FN => {
                StorageKey::NativeFunction(NativeFnObjectKey::from_ffi(key.value))
            }
            FastStorageKey::VARIANT_FN => StorageKey::Function(FnObjectKey::from_ffi(key.value)),
            FastStorageKey::VARIANT_COROUTINE => {
                StorageKey::Coroutine(CoroutineObjectKey::from_ffi(key.value))
            }
            _ => unreachable!(),
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Heap {
    pub(crate) storage: Storage,
    pub(crate) byte_strings: FastHashMap<ByteString, BytesObjectKey>,
    pub(crate) ref_roots: IndexMap<FastStorageKey, RefCounter, BuildFastHasher>,
    #[cfg(feature = "serde")]
    pub(crate) tags: IndexMap<StackValue, NativeFnObjectKey, BuildFastHasher>,
    pub(crate) resume_callbacks: FastHashMap<
        NativeFnObjectKey,
        NativeFunction<(NativeCallContext, Result<(), RuntimeError>, MultiValue)>,
    >,
    #[cfg_attr(feature = "serde", serde(skip))]
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

        let mut ref_roots = IndexMap::<FastStorageKey, RefCounter, BuildFastHasher>::default();
        let ref_counter = RefCounter::default();
        let counter_ref = ref_counter.create_counter_ref();
        ref_roots.insert(
            StorageKey::Table(string_metatable_key).into(),
            ref_counter.clone(),
        );

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

    pub(crate) fn store_native_fn(
        &mut self,
        gc: &mut GarbageCollector,
        value: NativeFunction<NativeCallContext>,
    ) -> NativeFnObjectKey {
        gc.modify_used_memory(std::mem::size_of_val(&value) as _);
        self.storage.native_functions.insert(value)
    }

    pub(crate) fn store_coroutine(
        &mut self,
        gc: &mut GarbageCollector,
        coroutine: Coroutine,
    ) -> CoroutineObjectKey {
        gc.modify_used_memory((std::mem::size_of_val(&coroutine) + coroutine.heap_size()) as _);
        self.storage.coroutines.insert(coroutine)
    }

    pub(crate) fn store_capture(&mut self, key: NativeFnObjectKey, value: impl NativeValue) {
        debug_assert!(self.storage.native_functions.contains_key(key));
        self.storage.captures.insert(key, Box::new(value));
    }

    pub(crate) fn create_ref<K: Copy + Into<StorageKey>>(&mut self, key: K) -> HeapRef<K> {
        let storage_key: StorageKey = key.into();
        let counter_ref = match self.ref_roots.entry(storage_key.into()) {
            indexmap::map::Entry::Occupied(entry) => entry.get().create_counter_ref(),
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
            gc.revive(key.into());
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

    pub(crate) fn get_disjoint_mut<const N: usize>(
        &mut self,
        gc: &mut GarbageCollector,
        keys: [TableObjectKey; N],
    ) -> Option<[&mut Table; N]> {
        for key in keys {
            gc.acknowledge_write(key.into());
        }

        self.storage.tables.get_disjoint_mut(keys)
    }

    pub(crate) fn get_interpreted_fn(&self, key: FnObjectKey) -> Option<&Function> {
        self.storage.functions.get(key)
    }

    pub(crate) fn get_native_fn(
        &self,
        key: NativeFnObjectKey,
    ) -> Option<&NativeFunction<NativeCallContext>> {
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

    pub(crate) fn get_stack_value_mut(
        &mut self,
        gc: &mut GarbageCollector,
        key: StackObjectKey,
    ) -> Option<&mut StackValue> {
        gc.acknowledge_write(key.into());
        self.storage.stack_values.get_mut(key)
    }

    #[cfg(feature = "serde")]
    pub(crate) fn rehydrate(
        &mut self,
        gc: &mut GarbageCollector,
        key: NativeFnObjectKey,
        value: NativeFunction<NativeCallContext>,
    ) {
        gc.acknowledge_write(key.into());
        self.storage.native_functions[key] = value;
    }

    pub(crate) fn get_table_metatable(&self, key: TableObjectKey) -> Option<TableObjectKey> {
        self.storage.metatables.get(key).cloned()
    }

    pub(crate) fn set_table_metatable(
        &mut self,
        gc: &mut GarbageCollector,
        table_key: TableObjectKey,
        metatable_key: Option<TableObjectKey>,
    ) {
        gc.acknowledge_write(table_key.into());

        match metatable_key {
            Some(metatable_key) => {
                self.storage.metatables.insert(table_key, metatable_key);
            }
            None => {
                self.storage.metatables.remove(table_key);
            }
        }
    }

    pub(crate) fn get_table_metavalue(
        &self,
        table_key: TableObjectKey,
        name: BytesObjectKey,
    ) -> StackValue {
        let Some(metatable_key) = self.storage.metatables.get(table_key) else {
            return StackValue::Nil;
        };

        let Some(metatable) = self.storage.tables.get(*metatable_key) else {
            #[cfg(debug_assertions)]
            unreachable!();
            #[cfg(not(debug_assertions))]
            return StackValue::Nil;
        };

        metatable.get(name.into())
    }

    pub(crate) fn get_table_metamethod(
        &self,
        table_key: TableObjectKey,
        name: BytesObjectKey,
    ) -> Option<StackValue> {
        let value = self.get_table_metavalue(table_key, name);

        if !matches!(
            value,
            StackValue::Function(_) | StackValue::NativeFunction(_)
        ) {
            return None;
        }

        Some(value)
    }

    pub(crate) fn get_metavalue(&self, value: StackValue, name: BytesObjectKey) -> StackValue {
        let metatable_key = match value {
            StackValue::Table(key) => {
                let Some(key) = self.storage.metatables.get(key) else {
                    return StackValue::Nil;
                };

                *key
            }
            StackValue::Bytes(_) => self.string_metatable_ref.key(),
            _ => return StackValue::Nil,
        };

        let Some(metatable) = self.storage.tables.get(metatable_key) else {
            #[cfg(debug_assertions)]
            unreachable!();
            #[cfg(not(debug_assertions))]
            return StackValue::Nil;
        };

        metatable.get(name.into())
    }

    pub(crate) fn get_metamethod(
        &self,
        value: StackValue,
        name: BytesObjectKey,
    ) -> Option<StackValue> {
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
