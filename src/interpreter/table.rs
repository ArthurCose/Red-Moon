use super::{heap::TableObjectKey, value_stack::StackValue};
use crate::languages::lua::coerce_integer;
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct Table {
    pub(crate) metatable: Option<TableObjectKey>,
    pub(crate) map: IndexMap<StackValue, StackValue, FxBuildHasher>,
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

    pub(crate) fn metatable(&self) -> Option<TableObjectKey> {
        self.metatable
    }

    pub(crate) fn set_metatable(&mut self, table_key: Option<TableObjectKey>) {
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
            let map_key = StackValue::Integer(index as _);

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
                if index > 0 {
                    if let Some(value) = self.list.get(index as usize - 1) {
                        return *value;
                    }
                }
            }
            StackValue::Float(float) => {
                if let Some(i) = coerce_integer(float) {
                    if let Some(value) = self.list.get(i as usize - 1) {
                        return *value;
                    }
                }
            }
            _ => {}
        }

        self.get_from_map(key)
    }

    pub(crate) fn get_from_map(&self, key: StackValue) -> StackValue {
        self.map.get(&key).cloned().unwrap_or(StackValue::Nil)
    }

    pub(crate) fn set(&mut self, key: StackValue, value: StackValue) {
        let used_list = match key {
            StackValue::Integer(index) if index > 0 => self.set_in_list(index as usize - 1, value),
            StackValue::Float(float) => {
                if let Some(i) = coerce_integer(float) {
                    self.set_in_list(i as usize - 1, value)
                } else {
                    false
                }
            }
            _ => false,
        };

        if used_list {
            return;
        }

        self.set_in_map(key, value);
    }

    pub(crate) fn set_in_map(&mut self, key: StackValue, value: StackValue) {
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
        let mut map_index = self.list.len() as i64 + 1;

        while let Some(value) = self.map.swap_remove(&StackValue::Integer(map_index)) {
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

    pub(crate) fn next(&self, previous: StackValue) -> Option<(StackValue, StackValue)> {
        if previous == StackValue::Nil {
            return self.map.first().map(|(k, v)| (*k, *v));
        }

        let index = self.map.get_index_of(&previous)?;
        self.map.get_index(index + 1).map(|(k, v)| (*k, *v))
    }
}
