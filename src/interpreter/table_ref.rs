use super::heap::{HeapRef, HeapValue};
use super::table::Table;
use super::value_stack::StackValue;
use super::{FromValue, IntoValue, Value, VmContext};
use crate::errors::{IllegalInstruction, RuntimeError, RuntimeErrorData};
use slotmap::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef(pub(crate) HeapRef);

impl TableRef {
    #[inline]
    pub fn id(&self) -> u64 {
        self.0.key().data().as_ffi()
    }

    pub fn metatable(&self, ctx: &mut VmContext) -> Result<Option<TableRef>, RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let Some(table_value) = heap.get(self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let HeapValue::Table(table) = table_value else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let metatable_ref = table.metatable().map(|key| TableRef(heap.create_ref(key)));

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

                if heap.get(key).is_some() {
                    Ok(key)
                } else {
                    Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey))
                }
            })
            .transpose()?;

        let Some(heap_value) = heap.get_mut(gc, self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let HeapValue::Table(table) = heap_value else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        table.set_metatable(metatable_key);

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

        let Some(HeapValue::Table(table)) = heap.get(self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

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

        let Some(HeapValue::Table(table)) = heap.get_mut(gc, self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let key = key.to_stack_value();
        let value = value.to_stack_value();

        let original_size = table.gc_size();

        table.set(key, value);

        let new_size = table.gc_size();
        gc.modify_used_memory(new_size as isize - original_size as isize);

        Ok(())
    }

    /// Gets the length of the sequential part of the table without invoking the `__len` metamethod.
    pub fn raw_len(&self, ctx: &VmContext) -> Result<usize, RuntimeError> {
        let heap = &ctx.vm.execution_data.heap;
        let Some(HeapValue::Table(table)) = heap.get(self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

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
        if let Some(function_key) = heap.get_metamethod(table_key, method_key.into()) {
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
        if let Some(function_key) = heap.get_metamethod(table_key, method_key.into()) {
            return ctx.call_function_key(function_key, (self.clone(), key, value));
        };

        // fallback
        self.raw_set(key, value, ctx)
    }

    /// Gets the length of the sequential part of the table, using the `__len` metamethod if available, and falling back to direct access.
    pub fn len(&self, ctx: &mut VmContext) -> Result<usize, RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let table_key = self.0.key();
        let Some(HeapValue::Table(table)) = heap.get(table_key) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let len = table.list_len();
        let len_key = ctx.metatable_keys().len.0.key();

        let heap = &mut ctx.vm.execution_data.heap;
        let Some(function_key) = heap.get_metamethod(table_key, len_key.into()) else {
            return Ok(len);
        };

        ctx.call_function_key(function_key, self.clone())
    }

    /// Clears all values from the table without invoking metamethods, preserves the metatable.
    pub fn clear(&self, ctx: &mut VmContext) -> Result<(), RuntimeError> {
        let gc = &mut ctx.vm.execution_data.gc;
        let heap = &mut ctx.vm.execution_data.heap;
        let Some(HeapValue::Table(table)) = heap.get_mut(gc, self.0.key()) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let original_size = table.gc_size();

        table.clear();

        let new_size = table.gc_size();
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
        let table_key = self.0.key();
        let Some(HeapValue::Table(table)) = heap.get_mut(gc, table_key) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let index = (index - 1) as usize;

        if index > table.list_len() {
            return Err(RuntimeError::from(RuntimeErrorData::OutOfBounds));
        }

        table.list.insert(index, value.to_stack_value());

        gc.modify_used_memory(Table::LIST_ELEMENT_SIZE as isize);

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
        let table_key = self.0.key();
        let Some(HeapValue::Table(table)) = heap.get_mut(gc, table_key) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

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

    pub fn next<P: IntoValue, K: FromValue, V: FromValue>(
        &self,
        previous_key: P,
        ctx: &mut VmContext,
    ) -> Result<Option<(K, V)>, RuntimeError> {
        let previous_key = previous_key.into_value(ctx)?.to_stack_value();

        let heap = &mut ctx.vm.execution_data.heap;
        let table_key = self.0.key();
        let Some(HeapValue::Table(table)) = heap.get(table_key) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let Some((k, v)) = table.next(previous_key) else {
            return Ok(None);
        };

        let k = Value::from_stack_value(heap, k);
        let v = Value::from_stack_value(heap, v);

        let k = K::from_value(k, ctx)?;
        let v = V::from_value(v, ctx)?;

        Ok(Some((k, v)))
    }

    pub fn is_map_empty(&self, ctx: &mut VmContext) -> Result<bool, RuntimeError> {
        let heap = &mut ctx.vm.execution_data.heap;
        let table_key = self.0.key();
        let Some(HeapValue::Table(table)) = heap.get(table_key) else {
            return Err(RuntimeErrorData::from(IllegalInstruction::InvalidHeapKey).into());
        };

        let has_next = table.next(StackValue::Nil).is_some();

        Ok(!has_next)
    }
}
