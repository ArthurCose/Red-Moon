use super::heap::{
    BytesObjectKey, CoroutineObjectKey, FnObjectKey, NativeFnObjectKey, StackObjectKey,
    TableObjectKey,
};
use super::value_stack::StackValue;
use crate::BuildFastHasher;
use crate::languages::lua::coerce_integer;
use indexmap::IndexMap;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct MapKey {
    variant: u8,
    value: u64,
}

impl MapKey {
    const VARIANT_NIL: u8 = 0;
    const VARIANT_BOOL: u8 = 1;
    const VARIANT_INT: u8 = 2;
    const VARIANT_FLOAT: u8 = 3;
    const VARIANT_PTR: u8 = 4;
    const VARIANT_BYTES: u8 = 5;
    const VARIANT_TABLE: u8 = 6;
    const VARIANT_NATIVE_FN: u8 = 7;
    const VARIANT_FN: u8 = 8;
    const VARIANT_COROUTINE: u8 = 9;

    fn as_float(&self) -> f64 {
        f64::from_le_bytes(self.value.to_le_bytes())
    }

    fn from_float(f: f64) -> Self {
        MapKey {
            variant: Self::VARIANT_FLOAT,
            value: u64::from_le_bytes(f.to_le_bytes()),
        }
    }
}

impl std::hash::Hash for MapKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl From<StackValue> for MapKey {
    fn from(value: StackValue) -> MapKey {
        fn from_pair(variant: u8, value: u64) -> MapKey {
            MapKey { variant, value }
        }

        match value {
            StackValue::Nil => from_pair(Self::VARIANT_NIL, 0),
            StackValue::Bool(b) => from_pair(Self::VARIANT_BOOL, b as _),
            StackValue::Integer(i) => from_pair(Self::VARIANT_INT, i as _),
            StackValue::Float(f) => Self::from_float(f),
            StackValue::Pointer(key) => from_pair(Self::VARIANT_PTR, key.as_ffi()),
            StackValue::Bytes(key) => from_pair(Self::VARIANT_BYTES, key.as_ffi()),
            StackValue::Table(key) => from_pair(Self::VARIANT_TABLE, key.as_ffi()),
            StackValue::NativeFunction(key) => from_pair(Self::VARIANT_NATIVE_FN, key.as_ffi()),
            StackValue::Function(key) => from_pair(Self::VARIANT_FN, key.as_ffi()),
            StackValue::Coroutine(key) => from_pair(Self::VARIANT_COROUTINE, key.as_ffi()),
        }
    }
}

impl From<&MapKey> for StackValue {
    fn from(key: &MapKey) -> StackValue {
        match key.variant {
            MapKey::VARIANT_BOOL => StackValue::Bool(key.value != 0),
            MapKey::VARIANT_INT => StackValue::Integer(key.value as _),
            MapKey::VARIANT_FLOAT => StackValue::Float(key.as_float()),
            MapKey::VARIANT_PTR => StackValue::Pointer(StackObjectKey::from_ffi(key.value)),
            MapKey::VARIANT_BYTES => StackValue::Bytes(BytesObjectKey::from_ffi(key.value)),
            MapKey::VARIANT_TABLE => StackValue::Table(TableObjectKey::from_ffi(key.value)),
            MapKey::VARIANT_NATIVE_FN => {
                StackValue::NativeFunction(NativeFnObjectKey::from_ffi(key.value))
            }
            MapKey::VARIANT_FN => StackValue::Function(FnObjectKey::from_ffi(key.value)),
            MapKey::VARIANT_COROUTINE => {
                StackValue::Coroutine(CoroutineObjectKey::from_ffi(key.value))
            }
            _ => StackValue::Nil,
        }
    }
}

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Table {
    pub(crate) map: IndexMap<MapKey, StackValue, BuildFastHasher>,
    pub(crate) list: Vec<StackValue>,
}

const BUCKET_SIZE: usize = std::mem::size_of::<usize>() + std::mem::size_of::<StackValue>() * 2;

impl Table {
    pub(crate) const LIST_ELEMENT_SIZE: usize = std::mem::size_of::<StackValue>();
    // index + bucket, could use verification
    pub(crate) const MAP_ELEMENT_SIZE: usize = std::mem::size_of::<usize>() + BUCKET_SIZE;

    pub(crate) fn heap_size(&self) -> usize {
        let mut size = 0;
        // map
        size += self.map.len() * Self::MAP_ELEMENT_SIZE;
        // list
        size += self.list.len() * Self::LIST_ELEMENT_SIZE;
        size
    }

    pub(crate) fn reserve_list(&mut self, additional: usize) {
        self.list.reserve(additional);
    }

    pub(crate) fn reserve_map(&mut self, additional: usize) {
        self.map.reserve(additional);
    }

    pub(crate) fn list_len(&self) -> usize {
        self.list.len()
    }

    pub(crate) fn flush(&mut self, previous_flush_count: usize, values: &[StackValue]) {
        let index_start = previous_flush_count;

        for i in 1..=values.len() {
            let index = index_start + i;
            let map_key = MapKey::from(StackValue::Integer(index as _));

            self.map.swap_remove(&map_key);
        }

        // remove existing overlapping values from the list by using splice to extend
        let index_end = (index_start + values.len()).min(self.list.len());
        self.list
            .splice(index_start..index_end, values.iter().cloned());

        self.merge_from_map_into_list();
    }

    pub(crate) fn get(&self, key: StackValue) -> StackValue {
        match key {
            StackValue::Integer(index) => {
                if index > 0
                    && let Some(value) = self.list.get(index as usize - 1)
                {
                    return *value;
                }
            }
            StackValue::Float(float) => {
                if let Some(i) = coerce_integer(float) {
                    if i > 0
                        && let Some(value) = self.list.get(i as usize - 1)
                    {
                        return *value;
                    }

                    return self.get_from_map(StackValue::Integer(i));
                }

                if !float.is_finite() {
                    // infinity and NaN always return nil
                    return StackValue::Nil;
                }
            }
            _ => {}
        }

        self.get_from_map(key)
    }

    pub(crate) fn get_from_map(&self, key: StackValue) -> StackValue {
        let key = MapKey::from(key);

        if let Some(value) = self.map.get(&key) {
            *value
        } else {
            StackValue::Nil
        }
    }

    pub(crate) fn set(&mut self, key: StackValue, value: StackValue) {
        match key {
            StackValue::Integer(index) => {
                if index > 0 && self.set_in_list(index as usize - 1, value) {
                    return;
                }
            }
            StackValue::Float(float) => {
                if let Some(i) = coerce_integer(float) {
                    if i > 0 && self.set_in_list(i as usize - 1, value) {
                        return;
                    }

                    self.set_in_map(StackValue::Integer(i), value);
                    return;
                }

                if !float.is_finite() {
                    // infinity and NaN keys are never stored
                    // todo: runtime error
                    return;
                }
            }
            _ => {}
        };

        self.set_in_map(key, value);
    }

    pub(crate) fn set_in_map(&mut self, key: StackValue, value: StackValue) {
        let key = MapKey::from(key);

        if value == StackValue::Nil {
            self.map.shift_remove(&key);
        } else {
            self.map.insert(key, value);
        }
    }

    fn set_in_list(&mut self, index: usize, value: StackValue) -> bool {
        match index.cmp(&self.list.len()) {
            std::cmp::Ordering::Less => {
                if value == StackValue::Nil && index + 1 == self.list.len() {
                    // shrink the list
                    let reverse_iter = self.list.iter().rev();
                    let nil_count = reverse_iter
                        .skip(1)
                        .filter(|v| **v == StackValue::Nil)
                        .count()
                        + 1;

                    let new_len = self.list.len() - nil_count;
                    self.list.truncate(new_len);
                } else {
                    self.list[index] = value;
                }
            }
            std::cmp::Ordering::Equal => {
                if value == StackValue::Nil {
                    return false;
                }

                self.list.push(value);

                // merge from map
                self.merge_from_map_into_list();
            }
            std::cmp::Ordering::Greater => return false,
        }

        true
    }

    fn merge_from_map_into_list(&mut self) {
        if self.map.is_empty() {
            // quick exit optimization
            // assumes we usually don't need to merge map data into the list
            return;
        }

        let map_index = self.list.len() as i64 + 1;
        let mut map_key = MapKey {
            variant: MapKey::VARIANT_INT,
            value: map_index as _,
        };

        while let Some(value) = self.map.swap_remove(&map_key) {
            self.list.push(value);
            map_key.value += 1;
        }
    }

    /// Clears all values from the table, preserves metatable
    pub(crate) fn clear(&mut self) {
        self.list.clear();
        self.map.clear();
    }

    pub(crate) fn next(&self, previous: StackValue) -> Option<(StackValue, StackValue)> {
        if previous == StackValue::Nil {
            return self.map.first().map(|(k, v)| (k.into(), *v));
        }

        let index = self.map.get_index_of(&MapKey::from(previous))?;
        self.map.get_index(index + 1).map(|(k, v)| (k.into(), *v))
    }
}
