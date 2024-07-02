use super::byte_string::ByteString;
use super::interpreted_function::Function;
use super::native_function::NativeFunction;
use super::table::Table;
use super::value_stack::StackValue;
use super::Primitive;
use crate::FastHashMap;
use indexmap::IndexMap;
use std::cell::Cell;
use std::rc::Rc;

#[derive(Clone)]
pub(crate) enum HeapValue {
    StackValue(StackValue),
    Bytes(ByteString),
    Table(Table),
    NativeFunction(NativeFunction),
    Function(Function),
}

#[derive(Default, Clone)]
struct RefCounter {
    rc: Rc<()>,
}

impl RefCounter {
    fn count(&self) -> usize {
        Rc::strong_count(&self.rc)
    }
}

slotmap::new_key_type! {
    pub(crate) struct HeapKey;
}

#[derive(Clone)]
pub(crate) struct HeapRef {
    key: HeapKey,
    #[allow(dead_code)]
    ref_counter: RefCounter,
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
struct HeapRecord {
    value: HeapValue,
}

#[derive(Clone)]
pub(crate) struct Heap {
    storage: slotmap::SlotMap<HeapKey, HeapRecord>,
    byte_strings: FastHashMap<ByteString, HeapKey>,
    ref_roots: IndexMap<HeapKey, RefCounter>,
    recycled_tables: Rc<Cell<Vec<Table>>>,
    // feels a bit weird in here and not on VM, but easier to work with here
    string_metatable_ref: HeapRef,
}

impl Default for Heap {
    fn default() -> Self {
        let mut storage: slotmap::SlotMap<_, _> = Default::default();

        // create string metatable
        let string_metatable_key = storage.insert(HeapRecord {
            value: HeapValue::Table(Default::default()),
        });
        let counter = RefCounter::default();
        let string_metatable_ref = HeapRef {
            key: string_metatable_key,
            ref_counter: counter.clone(),
        };

        Self {
            storage,
            byte_strings: Default::default(),
            ref_roots: Default::default(),
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
        let mut tables = self.recycled_tables.take();
        let mut table = tables.pop().unwrap_or_default();
        self.recycled_tables.set(tables);

        table.reserve_list(list);
        table.reserve_map(map);

        self.create(HeapValue::Table(table))
    }

    pub(crate) fn create(&mut self, value: HeapValue) -> HeapKey {
        self.create_with_key(|_| value)
    }

    pub(crate) fn create_with_key(
        &mut self,
        callback: impl FnOnce(HeapKey) -> HeapValue,
    ) -> HeapKey {
        self.storage.insert_with_key(|key| HeapRecord {
            value: callback(key),
        })
    }

    pub(crate) fn create_ref(&mut self, heap_key: HeapKey) -> Option<HeapRef> {
        let counter = match self.ref_roots.entry(heap_key) {
            indexmap::map::Entry::Occupied(entry) => entry.get().clone(),
            indexmap::map::Entry::Vacant(entry) => {
                if !self.storage.contains_key(heap_key) {
                    return None;
                }
                let counter = RefCounter::default();
                entry.insert(counter.clone());
                counter
            }
        };

        Some(HeapRef {
            key: heap_key,
            ref_counter: counter.clone(),
        })
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
        self.create_ref(key).unwrap()
    }

    pub(crate) fn get(&self, heap_key: HeapKey) -> Option<&HeapValue> {
        Some(&self.storage.get(heap_key)?.value)
    }

    pub(crate) fn get_mut(&mut self, heap_key: HeapKey) -> Option<&mut HeapValue> {
        Some(&mut self.storage.get_mut(heap_key)?.value)
    }

    #[must_use]
    pub(crate) fn set(&mut self, heap_key: HeapKey, mut value: HeapValue) -> Option<()> {
        let record = self.storage.get_mut(heap_key)?;
        std::mem::swap(&mut record.value, &mut value);

        Some(())
    }

    pub(crate) fn get_metavalue(&self, heap_key: HeapKey, name: StackValue) -> StackValue {
        let Some(heap_record) = &self.storage.get(heap_key) else {
            return Primitive::Nil.into();
        };

        let metatable_key = match &heap_record.value {
            HeapValue::Table(table) => {
                let Some(key) = table.metatable() else {
                    return Primitive::Nil.into();
                };

                key
            }
            HeapValue::Bytes(_) => self.string_metatable_ref.key(),
            _ => return Primitive::Nil.into(),
        };

        let Some(metatable_record) = &self.storage.get(metatable_key) else {
            return Primitive::Nil.into();
        };

        let HeapValue::Table(metatable) = &metatable_record.value else {
            return Primitive::Nil.into();
        };

        metatable.get(name)
    }

    pub(crate) fn get_metamethod(&self, heap_key: HeapKey, name: StackValue) -> Option<HeapKey> {
        let StackValue::HeapValue(method_key) = self.get_metavalue(heap_key, name) else {
            return None;
        };

        if !matches!(
            self.storage.get(method_key)?.value,
            HeapValue::Function(_) | HeapValue::NativeFunction(_)
        ) {
            return None;
        }

        Some(method_key)
    }

    fn delete(&mut self, key: HeapKey) {
        let record = self.storage.remove(key).expect("should only delete once");

        match record.value {
            HeapValue::Bytes(bytes) => {
                self.byte_strings.remove(&bytes);
                println!("deleted string");
            }
            HeapValue::Function(_) => {
                println!("deleted function");
            }
            HeapValue::NativeFunction(_) => {
                println!("deleted native function");
            }
            HeapValue::Table(mut table) => {
                println!("deleted table");

                table.reset();

                let mut tables = self.recycled_tables.take();
                tables.push(table);
                self.recycled_tables.set(tables);
            }
            _ => {}
        }
    }

    pub(crate) fn collect_garbage(&mut self) {
        // self.storage
    }
}
