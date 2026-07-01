use super::{FromValue, IntoValue, Value};
use crate::errors::{RuntimeError, RuntimeErrorData};
use crate::interpreter::VmContext;
use crate::interpreter::garbage_collector::GarbageCollector;
use crate::interpreter::heap::{Heap, HeapRef, Storage, TableObjectKey};
use crate::interpreter::table::Table;
use crate::interpreter::value_stack::StackValue;
use crate::tag_native_type;
use slotmap::Key;

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TableRef(pub(crate) HeapRef<TableObjectKey>);

tag_native_type!(TableRef);

impl TableRef {
    #[inline]
    pub fn id(&self) -> u64 {
        Storage::key_to_id(self.0.key().data(), Storage::TABLES_TAG)
    }

    pub fn metatable(&self, ctx: &mut VmContext) -> Result<Option<TableRef>, RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let metatable_ref = heap
            .get_table_metatable(self.0.key())
            .map(|key| TableRef(heap.create_ref(key)));

        Ok(metatable_ref)
    }

    pub fn set_metatable(
        &self,
        metatable_ref: Option<&TableRef>,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;
        let metatable_key = metatable_ref
            .map(|metatable_ref| {
                let key = metatable_ref.0.key();

                if heap.get_table(key).is_some() {
                    Ok(key)
                } else {
                    Err(RuntimeErrorData::InvalidRef)
                }
            })
            .transpose()?;

        heap.set_table_metatable(gc, self.0.key(), metatable_key);

        Ok(())
    }

    /// Gets a value from the table without invoking the `__index` metamethod.
    pub fn raw_get<K: IntoValue, V: FromValue>(
        &self,
        key: K,
        ctx: &mut VmContext,
    ) -> Result<V, RuntimeError> {
        let key = key.into_value(ctx)?.to_stack_value();
        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table(heap)?;

        let value = table.get(key);
        let value = Value::from_stack_value(heap, value);

        V::from_value(value, ctx)
    }

    /// Sets a value on the table without invoking the `__newindex` metamethod.
    pub fn raw_set<K: IntoValue, V: IntoValue>(
        &self,
        key: K,
        value: V,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        let key = key.into_value(ctx)?;
        let value = value.into_value(ctx)?;

        // need to test validity to make sure invalid data doesn't get stored in the ctx
        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;

        key.test_validity(heap)?;
        value.test_validity(heap)?;

        let table = self.table_mut(gc, heap)?;

        let key = key.to_stack_value();
        let value = value.to_stack_value();

        let original_size = table.heap_size();

        table.set(key, value);

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        ctx.try_gc_step();

        Ok(())
    }

    /// Gets the length of the sequential part of the table without invoking the `__len` metamethod.
    pub fn raw_len(&self, ctx: &VmContext) -> Result<usize, RuntimeError> {
        let heap = &ctx.vm.execution_data.heap;
        let table = self.table(heap)?;

        Ok(table.list_len())
    }

    /// Gets a value from the table, using the `__index` metamethod if available, and falling back to direct access.
    pub fn get<K: IntoValue, V: FromValue>(
        &self,
        key: K,
        ctx: &mut VmContext,
    ) -> Result<V, RuntimeError> {
        let table_key = self.0.key();
        let method_key = ctx.metatable_keys().index.0.key();

        let heap = &mut ctx.vm.execution_data.heap;
        if let Some(function_key) = heap.get_table_metamethod(table_key, method_key) {
            return ctx.call_function_key(function_key, (self.clone(), key));
        };

        // fallback
        self.raw_get(key, ctx)
    }

    /// Sets a value on the table, using the `__newindex` metamethod if available, and falling back to direct access.
    pub fn set<K: IntoValue, V: IntoValue>(
        &self,
        key: K,
        value: V,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        let key = key.into_value(ctx)?;
        let value = value.into_value(ctx)?;

        let table_key = self.0.key();
        let method_key = ctx.metatable_keys().newindex.0.key();

        let heap = &mut ctx.vm.execution_data.heap;
        if let Some(function_key) = heap.get_table_metamethod(table_key, method_key) {
            return ctx.call_function_key(function_key, (self.clone(), key, value));
        };

        // fallback
        self.raw_set(key, value, ctx)
    }

    /// Gets the length of the sequential part of the table, using the `__len` metamethod if available, and falling back to direct access.
    pub fn len(&self, ctx: &mut VmContext) -> Result<usize, RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table(heap)?;

        let len = table.list_len();
        let len_key = ctx.metatable_keys().len.0.key();

        let heap = &mut ctx.vm.execution_data.heap;
        let Some(function_key) = heap.get_table_metamethod(self.0.key(), len_key) else {
            return Ok(len);
        };

        ctx.call_function_key(function_key, self.clone())
    }

    /// Clears all values from the table without invoking metamethods, preserves the metatable.
    pub fn clear(&self, ctx: &mut VmContext) -> Result<(), RuntimeError> {
        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table_mut_unmarked(heap)?;

        let original_size = table.heap_size();

        table.clear();

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        Ok(())
    }

    pub fn raw_insert<V: IntoValue>(
        &self,
        index: i64,
        value: V,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        if index == 0 {
            return Err(RuntimeError::from(RuntimeErrorData::OutOfBounds));
        }

        let value = value.into_value(ctx)?;

        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table_mut(gc, heap)?;

        let index = (index - 1) as usize;

        if index > table.list_len() {
            return Err(RuntimeError::from(RuntimeErrorData::OutOfBounds));
        }

        table.list.insert(index, value.to_stack_value());

        gc.modify_used_memory(Table::LIST_ELEMENT_SIZE as isize);

        ctx.try_gc_step();

        Ok(())
    }

    pub fn raw_remove<V: FromValue>(
        &self,
        index: i64,
        ctx: &mut VmContext,
    ) -> Result<V, RuntimeError> {
        if index == 0 {
            return Err(RuntimeError::from(RuntimeErrorData::OutOfBounds));
        }

        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table_mut_unmarked(heap)?;

        let index = (index - 1) as usize;

        if index >= table.list_len() {
            return Err(RuntimeError::from(RuntimeErrorData::OutOfBounds));
        }

        let value = table.get(StackValue::Integer((index + 1) as _));

        table.list.remove(index);

        gc.modify_used_memory(-(Table::LIST_ELEMENT_SIZE as isize));

        let value = Value::from_stack_value(heap, value);

        V::from_value(value, ctx)
    }

    pub fn raw_push<V: IntoValue>(
        &self,
        value: V,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        let value = value.into_value(ctx)?.to_stack_value();

        let heap = &mut ctx.vm.execution_data.heap;
        let gc = &mut ctx.vm.execution_data.gc;
        let table = self.table_mut(gc, heap)?;

        let original_size = table.heap_size();

        // push
        let len = table.list_len();
        table.set(StackValue::Integer((len + 1) as _), value);

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        ctx.try_gc_step();

        Ok(())
    }

    pub fn copy_within(
        &self,
        src_start: usize,
        dest_start: usize,
        len: usize,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let gc = &mut ctx.vm.execution_data.gc;

        let table = self.table_mut_unmarked(heap)?;

        let original_size = table.heap_size();

        if dest_start < src_start {
            // dest is less than src, we can move from left to right without overwriting the src
            for i in 0..len {
                let src = src_start + i;
                let dest = dest_start + i;
                let value = table.get(StackValue::Integer(src as _));
                table.set(StackValue::Integer(dest as _), value);
            }
        } else {
            // dest is greater than src, we should move reversed to avoid overwriting the src
            for i in (0..len).rev() {
                let src = src_start + i;
                let dest = dest_start + i;
                let value = table.get(StackValue::Integer(src as _));
                table.set(StackValue::Integer(dest as _), value);
            }
        }

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        ctx.try_gc_step();

        Ok(())
    }

    pub fn copy_from(
        &self,
        src_start: usize,
        dest_start: usize,
        len: usize,
        src_table: &TableRef,
        ctx: &mut VmContext,
    ) -> Result<(), RuntimeError> {
        if self == src_table {
            return self.copy_within(src_start, dest_start, len, ctx);
        }

        let heap = &mut ctx.vm.execution_data.heap;
        let gc = &mut ctx.vm.execution_data.gc;

        let table_keys = [self.0.key(), src_table.0.key()];
        let Some([table, other_table]) = heap.get_disjoint_mut(gc, table_keys) else {
            return Err(RuntimeErrorData::InvalidRef.into());
        };

        let original_size = table.heap_size();

        // copy logic
        for i in 0..len {
            let src = src_start + i;
            let dest = dest_start + i;
            let value = other_table.get(StackValue::Integer(src as _));
            table.set(StackValue::Integer(dest as _), value);
        }

        let new_size = table.heap_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        ctx.try_gc_step();

        Ok(())
    }

    /// Using zero based index
    fn get_from_list(&self, i: usize, heap: &mut Heap) -> Result<Value, RuntimeError> {
        let table = self.table(heap)?;
        let Some(&stack_value) = table.list.get(i) else {
            return Ok(Default::default());
        };

        let value = Value::from_stack_value(heap, stack_value);

        Ok(value)
    }

    pub fn sort_unstable_by(
        &self,
        ctx: &mut VmContext,
        mut less_than_fn: impl FnMut(&Value, &Value, &mut VmContext) -> Result<bool, RuntimeError>,
    ) -> Result<(), RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table(heap)?;

        if table.list.len() < 2 {
            return Ok(());
        }

        // quicksort
        let mut partitions = vec![0..table.list.len()];

        while let Some(range) = partitions.pop() {
            if range.len() < 2 {
                continue;
            }

            if range.len() == 2 {
                let left_index = range.start;
                let right_index = range.end - 1;

                let heap = &mut ctx.vm.execution_data.heap;
                let left = self.get_from_list(left_index, heap)?;
                let right = self.get_from_list(right_index, heap)?;

                if less_than_fn(&right, &left, ctx)? {
                    let heap = &mut ctx.vm.execution_data.heap;
                    let table = self.table_mut_unmarked(heap)?;

                    if right_index < table.list.len() {
                        table.list.swap(left_index, right_index);
                    }
                }

                continue;
            }

            let mut left_index = range.start.wrapping_sub(1);
            let mut right_index = range.end;

            let pivot_index = range.start + range.len() / 2;

            let heap = &mut ctx.vm.execution_data.heap;
            let pivot_value = self.get_from_list(pivot_index, heap)?;

            loop {
                // increment left
                loop {
                    left_index = left_index.wrapping_add(1);

                    let heap = &mut ctx.vm.execution_data.heap;
                    let value = self.get_from_list(left_index, heap)?;
                    if !less_than_fn(&value, &pivot_value, ctx)? {
                        break;
                    }

                    if left_index == right_index {
                        return Err(RuntimeError::new_static_string(
                            "invalid order function for sorting",
                        ));
                    }

                    if left_index > right_index {
                        break;
                    }
                }

                // decrement right
                loop {
                    right_index = right_index.wrapping_sub(1);

                    let heap = &mut ctx.vm.execution_data.heap;
                    let value = self.get_from_list(right_index, heap)?;
                    if !less_than_fn(&pivot_value, &value, ctx)? {
                        break;
                    }

                    if right_index <= left_index {
                        break;
                    }
                }

                // crossed
                if left_index >= right_index {
                    // use the left_index as the end of the range as it may have passed the right index
                    if range.start != left_index {
                        if left_index - range.start > 1 {
                            // avoid growing the partition stack for completed ranges
                            partitions.push(range.start..left_index);
                        }

                        partitions.push(left_index..range.end);
                    }

                    break;
                }

                // swap, but check len just in case the user deleted some values
                let heap = &mut ctx.vm.execution_data.heap;
                let table = self.table_mut_unmarked(heap)?;

                if right_index < table.list.len() {
                    table.list.swap(left_index, right_index);
                }
            }
        }

        Ok(())
    }

    pub fn next<P: IntoValue, K: FromValue, V: FromValue>(
        &self,
        previous_key: P,
        ctx: &mut VmContext,
    ) -> Result<Option<(K, V)>, RuntimeError> {
        let previous_key = previous_key.into_value(ctx)?.to_stack_value();

        let heap = &mut ctx.vm.execution_data.heap;
        let table = self.table(heap)?;
        let Some((k, v)) = table.next(previous_key) else {
            if previous_key != StackValue::Nil && !table.is_key_valid(previous_key) {
                return Err(RuntimeError::new_static_string("invalid key to 'next'"));
            }
            return Ok(None);
        };

        let k = Value::from_stack_value(heap, k);
        let v = Value::from_stack_value(heap, v);

        let k = K::from_value(k, ctx)?;
        let v = V::from_value(v, ctx)?;

        Ok(Some((k, v)))
    }

    pub fn is_map_empty(&self, ctx: &mut VmContext) -> Result<bool, RuntimeError> {
        let heap = &ctx.vm.execution_data.heap;
        let table = self.table(heap)?;

        let has_next = table.next(StackValue::Nil).is_some();

        Ok(!has_next)
    }

    fn table<'a>(&self, heap: &'a Heap) -> Result<&'a Table, RuntimeErrorData> {
        let table_key = self.0.key();
        heap.get_table(table_key)
            .ok_or(RuntimeErrorData::InvalidRef)
    }

    fn table_mut<'a>(
        &self,
        gc: &mut GarbageCollector,
        heap: &'a mut Heap,
    ) -> Result<&'a mut Table, RuntimeErrorData> {
        let table_key = self.0.key();
        heap.get_table_mut(gc, table_key)
            .ok_or(RuntimeErrorData::InvalidRef)
    }

    fn table_mut_unmarked<'a>(
        &self,
        heap: &'a mut Heap,
    ) -> Result<&'a mut Table, RuntimeErrorData> {
        let table_key = self.0.key();
        heap.get_table_mut_unmarked(table_key)
            .ok_or(RuntimeErrorData::InvalidRef)
    }
}
