use super::heap::HeapKey;
use super::value_stack::{Primitive, StackValue};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;
use std::ops::Range;

#[derive(Clone, Default)]
pub(crate) struct Table {
    metatable: Option<HeapKey>,
    map: IndexMap<StackValue, StackValue, FxBuildHasher>,
    list: Vec<StackValue>,
}

impl Table {
    pub(crate) fn metatable(&self) -> Option<HeapKey> {
        self.metatable
    }

    pub(crate) fn set_metatable(&mut self, table_key: Option<HeapKey>) {
        self.metatable = table_key;
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
            let map_key = StackValue::Primitive(Primitive::Integer(index as _));

            self.map.swap_remove(&map_key);
        }

        // shrink so we can properly extend
        self.list.truncate(index_start);

        self.list.extend(values);
        self.merge_from_map_into_list();
    }

    /// Uses zero-based index
    /// Panics if the starting point is greater than the end point or if the end point is greater than the length of the vector.
    pub(crate) fn splice(
        &mut self,
        range: Range<usize>,
        iter: impl IntoIterator<Item = StackValue>,
    ) {
        self.list.splice(range, iter);
    }

    fn remap_key(key: StackValue) -> StackValue {
        if let StackValue::Primitive(Primitive::Float(float)) = key {
            if float.fract() == 0.0 {
                return Primitive::Integer(float as _).into();
            }
        }

        key
    }

    pub(crate) fn get(&self, mut key: StackValue) -> StackValue {
        key = Self::remap_key(key);

        if let StackValue::Primitive(Primitive::Integer(index)) = key {
            if index > 0 {
                if let Some(value) = self.list.get(index as usize - 1) {
                    return *value;
                }
            }
        }

        self.map
            .get(&key)
            .cloned()
            .unwrap_or(StackValue::Primitive(Primitive::Nil))
    }

    pub(crate) fn set(&mut self, mut key: StackValue, value: StackValue) {
        key = Self::remap_key(key);

        if self.set_in_list(key, value) {
            return;
        }

        if let StackValue::Primitive(Primitive::Nil) = value {
            self.map.shift_remove(&key);
            return;
        }

        self.map.insert(key, value);
    }

    fn set_in_list(&mut self, remapped_key: StackValue, value: StackValue) -> bool {
        let StackValue::Primitive(Primitive::Integer(index)) = remapped_key else {
            return false;
        };

        if index <= 0 {
            return false;
        }

        let index = index as usize - 1;

        if self.list.get(index).is_some() {
            if value == Primitive::Nil.into() && index + 1 == self.list.len() {
                // shrink the list
                let reverse_iter = self.list.iter().rev();
                let nil_count = reverse_iter
                    .skip(1)
                    .filter(|v| **v == StackValue::Primitive(Primitive::Nil))
                    .count()
                    + 1;

                let new_len = self.list.len() - nil_count;
                self.list.truncate(new_len);

                return true;
            } else {
                self.list[index] = value;
            }
        } else if index == self.list.len() {
            if value == Primitive::Nil.into() {
                return false;
            }

            self.list.push(value);

            // merge from map
            self.merge_from_map_into_list();
        } else {
            return false;
        }

        true
    }

    fn merge_from_map_into_list(&mut self) {
        let mut map_index = self.list.len() as i64 + 1;

        while let Some(value) = self
            .map
            .swap_remove(&StackValue::Primitive(Primitive::Integer(map_index)))
        {
            self.list.push(value);
            map_index += 1;
        }
    }

    /// Clears all values from the table, preserves metatable
    pub(crate) fn clear(&mut self) {
        self.list.clear();
        self.map.clear();
    }

    /// Clears all values from the table, incliding metatable
    pub(crate) fn reset(&mut self) {
        self.list.clear();
        self.map.clear();
        self.metatable = None;
    }

    pub(crate) fn next(&mut self, previous: StackValue) -> Option<(StackValue, StackValue)> {
        if previous == Primitive::Nil.into() {
            self.map.first().map(|(k, v)| (*k, *v))
        } else {
            self.map.get_full(&previous).map(|(_, k, v)| (*k, *v))
        }
    }
}
