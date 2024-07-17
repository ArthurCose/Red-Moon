use super::heap::HeapKey;
use super::value_stack::{Primitive, StackValue};
use indexmap::IndexMap;
use rustc_hash::FxBuildHasher;

#[derive(Clone, Default)]
pub(crate) struct Table {
    pub(crate) metatable: Option<HeapKey>,
    pub(crate) map: IndexMap<StackValue, StackValue, FxBuildHasher>,
    pub(crate) list: Vec<StackValue>,
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

        // remove existing overlapping values from the list by using splice to extend
        let index_end = (index_start + values.len()).min(self.list.len());
        self.list
            .splice(index_start..index_end, values.iter().cloned());

        self.merge_from_map_into_list();
    }

    pub(crate) fn get(&self, key: StackValue) -> StackValue {
        match key {
            StackValue::Primitive(Primitive::Integer(index)) => {
                if index > 0 {
                    if let Some(value) = self.list.get(index as usize - 1) {
                        return *value;
                    }
                }
            }
            StackValue::Primitive(Primitive::Float(float)) => {
                if float.fract() == 0.0 {
                    if let Some(value) = self.list.get(float as usize - 1) {
                        return *value;
                    }
                }
            }
            _ => {}
        }

        self.map
            .get(&key)
            .cloned()
            .unwrap_or(StackValue::Primitive(Primitive::Nil))
    }

    pub(crate) fn set(&mut self, key: StackValue, value: StackValue) {
        let used_list = match key {
            StackValue::Primitive(Primitive::Integer(index)) if index > 0 => {
                self.set_in_list(index as usize - 1, value)
            }
            StackValue::Primitive(Primitive::Float(float))
                if float.fract() == 0.0 && float as usize > 0 =>
            {
                self.set_in_list(float as usize - 1, value)
            }
            _ => false,
        };

        if used_list {
            return;
        }

        if let StackValue::Primitive(Primitive::Nil) = value {
            self.map.shift_remove(&key);
            return;
        }

        self.map.insert(key, value);
    }

    fn set_in_list(&mut self, index: usize, value: StackValue) -> bool {
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

    pub(crate) fn next(&self, previous: StackValue) -> Option<(StackValue, StackValue)> {
        if previous == Primitive::Nil.into() {
            return self.map.first().map(|(k, v)| (*k, *v));
        }

        let index = self.map.get_index_of(&previous)?;
        self.map.get_index(index + 1).map(|(k, v)| (*k, *v))
    }
}
