use super::Heap;
use super::NativeFnObjectKey;
use super::StorageKey;
use super::TableObjectKey;
use crate::interpreter::cache_pools::CachePools;
use crate::interpreter::cache_pools::RECYCLE_LIMIT;
use crate::interpreter::execution::ExecutionContext;
use crate::interpreter::metatable_keys::MetatableKeys;
use crate::interpreter::up_values::UpValues;
use crate::interpreter::value_stack::{StackValue, ValueStack};
use crate::interpreter::vm::CoroutineData;
use crate::interpreter::Continuation;
use crate::{FastHashMap, FastHashSet};
use std::rc::Rc;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Configuration for the incremental garbage collector
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct GarbageCollectorConfig {
    /// used_memory_at_last_collection * pause / 100 = threshold for starting GcPhase::Mark
    ///
    /// Default is 200
    pub pause: usize,
    /// used_memory * step_multiplier / 1024 = how many marks and sweeps per step
    ///
    /// Default is 100
    pub step_multiplier: usize,
    /// step_size = threshold for a sweep / mark step
    ///
    /// Default is 2^13
    pub step_size: usize,
}

impl Default for GarbageCollectorConfig {
    fn default() -> Self {
        Self {
            pause: 200,
            step_multiplier: 100,
            step_size: 2usize.pow(13),
        }
    }
}

#[derive(Default, Clone, Copy)]
enum Phase {
    #[default]
    Idle,
    Mark,
    Sweep,
    Stopped,
}

#[derive(Clone)]
enum Mark {
    Black,
    Gray,
}

#[derive(Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct GarbageCollector {
    #[cfg_attr(feature = "serde", serde(skip))]
    phase: Phase,
    used_memory: usize,
    /// how many bytes we've accumulated since the last step
    accumulated: usize,
    pub(crate) config: GarbageCollectorConfig,
    #[cfg_attr(feature = "serde", serde(skip))]
    traversed_definitions: FastHashSet<usize>,
    /// gray + black, excluded keys are white
    #[cfg_attr(feature = "serde", serde(skip))]
    marked: FastHashMap<StorageKey, Mark>,
    /// gray or pending sweep
    #[cfg_attr(feature = "serde", serde(skip))]
    phase_queue: Vec<StorageKey>,
    /// element_key/value -> Vec<(table_key, element_key)>
    #[cfg_attr(feature = "serde", serde(skip))]
    weak_associations: FastHashMap<StorageKey, Vec<(TableObjectKey, StackValue)>>,
}

impl Clone for GarbageCollector {
    fn clone(&self) -> Self {
        Self {
            phase: self.phase,
            used_memory: self.used_memory,
            accumulated: self.accumulated,
            config: self.config.clone(),
            traversed_definitions: self.traversed_definitions.clone(),
            marked: self.marked.clone(),
            phase_queue: self.phase_queue.clone(),
            weak_associations: self.weak_associations.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.phase = source.phase;
        self.used_memory = source.used_memory;
        self.accumulated = source.accumulated;
        self.config.clone_from(&source.config);
        self.traversed_definitions
            .clone_from(&source.traversed_definitions);
        self.marked.clone_from(&source.marked);
        self.phase_queue.clone_from(&source.phase_queue);
        self.weak_associations.clone_from(&source.weak_associations);
    }
}

impl GarbageCollector {
    pub(crate) fn used_memory(&self) -> usize {
        self.used_memory
    }

    pub(crate) fn modify_used_memory(&mut self, change: isize) {
        self.used_memory = (self.used_memory as isize + change) as usize;
        self.accumulated = (self.accumulated as isize + change).max(0) as usize;
    }

    pub(crate) fn acknowledge_write(&mut self, key: StorageKey) {
        let Some(mark) = self.marked.get_mut(&key) else {
            // not marked, we'll handle this through normal traversal
            return;
        };

        if !matches!(self.phase, Phase::Mark) || matches!(mark, Mark::Gray) {
            // not marking or already in the gray queue
            return;
        }

        // mark as gray again and place in the queue
        *mark = Mark::Gray;
        self.phase_queue.push(key);
    }

    pub(crate) fn is_running(&self) -> bool {
        !matches!(self.phase, Phase::Stopped)
    }

    pub(crate) fn stop(&mut self) {
        self.phase = Phase::Stopped;
    }

    pub(crate) fn restart(&mut self) {
        self.phase = Phase::Idle;
        self.marked.clear();
        self.traversed_definitions.clear();
        self.weak_associations.clear();
        self.phase_queue.clear();
    }

    pub(crate) fn should_step(&self) -> bool {
        match self.phase {
            Phase::Idle => {
                let threshold = (self.used_memory - self.accumulated) * self.config.pause / 100;

                self.accumulated >= threshold
            }
            Phase::Mark | Phase::Sweep => self.accumulated >= self.config.step_size,
            Phase::Stopped => false,
        }
    }

    pub(crate) fn step(
        &mut self,
        metatable_keys: &MetatableKeys,
        cache_pools: &CachePools,
        heap: &mut Heap,
        execution_stack: &[ExecutionContext],
        coroutine_data: &CoroutineData,
    ) {
        let mark = match self.phase {
            Phase::Idle => {
                self.phase = Phase::Mark;
                true
            }
            Phase::Mark => true,
            Phase::Sweep => false,
            Phase::Stopped => return,
        };

        self.accumulated = 0;

        let limit = self.used_memory * self.config.step_multiplier / 1024;

        if mark {
            self.mark_roots(heap, execution_stack, coroutine_data);
            self.traverse_gray(metatable_keys, cache_pools, heap, Some(limit));

            if self.phase_queue.is_empty() {
                self.init_sweep(heap);
            }
        } else {
            self.sweep(cache_pools, heap, Some(limit));
        }
    }

    pub(crate) fn full_cycle(
        &mut self,
        metatable_keys: &MetatableKeys,
        cache_pools: &CachePools,
        heap: &mut Heap,
        execution_stack: &[ExecutionContext],
        coroutine_data: &CoroutineData,
    ) {
        self.mark_roots(heap, execution_stack, coroutine_data);

        while !self.phase_queue.is_empty() {
            self.traverse_gray(metatable_keys, cache_pools, heap, None);
        }

        self.init_sweep(heap);
        self.sweep(cache_pools, heap, None);
        self.accumulated = 0;
    }

    fn mark_roots(
        &mut self,
        heap: &mut Heap,
        execution_stack: &[ExecutionContext],
        coroutine_data: &CoroutineData,
    ) {
        // identify ref roots and mark them
        heap.ref_roots.retain(|&key, counter| {
            let keep = counter.count() > 0;

            if keep {
                self.mark_heap_key_root(key);
            }

            keep
        });

        // mark tags, we don't want to delete a string required for rehydration
        #[cfg(feature = "serde")]
        for tag_value in heap.tags.keys() {
            self.mark_stack_value(tag_value);
        }

        // mark the stack
        for execution in execution_stack {
            self.mark_execution_context(execution);
        }

        // mark coroutine related data

        for value_stack in &coroutine_data.continuation_states {
            self.mark_value_stack(value_stack);
        }

        for (continuation, _) in &coroutine_data.in_progress_yield {
            match continuation {
                Continuation::Entry(key) => self.mark_storage_key(*key),
                Continuation::Callback(key, state) => {
                    self.mark_storage_key(key.into());
                    self.mark_value_stack(state);
                }
                Continuation::Execution { execution, .. } => self.mark_execution_context(execution),
            }
        }
    }

    fn mark_execution_context(&mut self, execution: &ExecutionContext) {
        self.mark_value_stack(&execution.value_stack);

        for call in &execution.call_stack {
            self.mark_up_values(&call.function.up_values)
        }
    }

    fn traverse_gray(
        &mut self,
        metatable_keys: &MetatableKeys,
        cache_pools: &CachePools,
        heap: &mut Heap,
        limit: Option<usize>,
    ) {
        let mut mark_count = 0;

        while let Some(key) = self.phase_queue.pop() {
            self.marked.insert(key, Mark::Black);
            self.traverse_heap_value(metatable_keys, cache_pools, heap, key);

            mark_count += 1;

            if limit.is_some_and(|limit| mark_count >= limit) {
                break;
            }
        }
    }

    fn init_sweep(&mut self, heap: &mut Heap) {
        debug_assert!(self.phase_queue.is_empty());

        for key in heap.storage.keys() {
            if !self.marked.contains_key(&key) {
                self.phase_queue.push(key);
            }
        }

        self.phase = Phase::Sweep;
    }

    fn sweep(&mut self, cache_pools: &CachePools, heap: &mut Heap, limit: Option<usize>) {
        // todo: handle finalizers and resurrection

        let mut delete_count = 0;

        while let Some(key) = self.phase_queue.pop() {
            // clear weak associations
            if let Some(list) = self.weak_associations.remove(&key) {
                let key_stack_value: StackValue = key.into();

                for &(table_key, element_key) in &list {
                    let Some(table) = heap.storage.tables.get_mut(table_key) else {
                        continue;
                    };

                    // we need to test to see if the association is still true

                    // test key
                    if element_key == key_stack_value {
                        table.set(element_key, StackValue::default());
                        continue;
                    }

                    // test value
                    if table.get(element_key) == key_stack_value {
                        table.set(element_key, StackValue::default());
                    }
                }

                cache_pools.store_weak_associations_list(list);
            };

            // delete
            match key {
                StorageKey::StackValue(key) => {
                    heap.storage.stack_values.remove(key);
                    self.used_memory -= std::mem::size_of::<StackValue>();
                }
                StorageKey::Bytes(key) => {
                    let Some(bytes) = heap.storage.byte_strings.remove(key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        continue;
                    };
                    heap.byte_strings.remove(&bytes);
                    self.used_memory -= std::mem::size_of_val(&bytes) + bytes.heap_size();
                }
                StorageKey::Table(key) => {
                    let Some(mut table) = heap.storage.tables.remove(key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        continue;
                    };
                    self.used_memory -= std::mem::size_of_val(&table) + table.heap_size();

                    if heap.recycled_tables.len() < RECYCLE_LIMIT {
                        table.reset();
                        heap.recycled_tables.push(table);
                    }
                }
                StorageKey::NativeFunction(key) => {
                    let Some(native_fn) = heap.storage.native_functions.remove(key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        continue;
                    };
                    self.used_memory -= std::mem::size_of_val(&native_fn);

                    if let Some(callback) = heap.resume_callbacks.remove(&key) {
                        self.used_memory -= std::mem::size_of::<NativeFnObjectKey>()
                            + std::mem::size_of_val(&callback);
                    }
                }
                StorageKey::Function(key) => {
                    let Some(function) = heap.storage.functions.remove(key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        continue;
                    };
                    self.used_memory -= std::mem::size_of_val(&function) + function.heap_size();
                }
                StorageKey::Coroutine(key) => {
                    let Some(co) = heap.storage.coroutines.remove(key) else {
                        crate::debug_unreachable!();
                        #[cfg(not(debug_assertions))]
                        continue;
                    };
                    self.used_memory -= std::mem::size_of_val(&co) + co.heap_size();
                }
            }

            // test against limit
            delete_count += 1;

            if limit.is_some_and(|limit| delete_count >= limit) {
                break;
            }
        }

        if self.phase_queue.is_empty() {
            // completed sweep, restart cycle
            self.restart();
        }
    }

    fn mark_table_value(&mut self, value: &StackValue) {
        let Some(key) = value.as_storage_key() else {
            return;
        };

        self.mark_storage_key(key);
    }

    fn mark_stack_value(&mut self, value: &StackValue) {
        let Some(key) = value.as_storage_key() else {
            return;
        };

        self.mark_storage_key(key);
    }

    fn mark_up_values(&mut self, up_values: &UpValues) {
        for key in up_values.iter() {
            self.mark_storage_key(key.into());
        }
    }

    fn mark_value_stack(&mut self, value_stack: &ValueStack) {
        for value in value_stack.iter() {
            self.mark_stack_value(value);
        }
    }

    fn mark_storage_key(&mut self, key: StorageKey) {
        if self.marked.contains_key(&key) {
            return;
        }

        self.marked.insert(key, Mark::Gray);
        self.phase_queue.push(key);
    }

    fn traverse_heap_value(
        &mut self,
        metatable_keys: &MetatableKeys,
        cache_pools: &CachePools,
        heap: &mut Heap,
        key: StorageKey,
    ) {
        match key {
            StorageKey::Function(key) => {
                let Some(function) = heap.get_interpreted_fn(key) else {
                    crate::debug_unreachable!();
                    #[cfg(not(debug_assertions))]
                    return;
                };

                self.mark_up_values(&function.up_values);

                let definition_key = Rc::as_ptr(&function.definition) as usize;

                if self.traversed_definitions.insert(definition_key) {
                    for key in &function.definition.byte_strings {
                        self.mark_storage_key(key.into());
                    }

                    for key in &function.definition.functions {
                        self.mark_storage_key(key.into());
                    }
                }
            }
            StorageKey::Table(table_key) => {
                let Some(table) = heap.get_table(table_key) else {
                    crate::debug_unreachable!();
                    #[cfg(not(debug_assertions))]
                    return;
                };

                let mut weak_keys = false;
                let mut weak_values = false;

                if let Some(key) = table.metatable {
                    self.mark_storage_key(key.into());

                    if let Some(metatable) = heap.get_table(key) {
                        let mode_key = metatable_keys.mode.0.key.into();
                        let mode_value = metatable.get(mode_key);

                        if let StackValue::Bytes(mode_key) = mode_value {
                            if let Some(bytes) = heap.get_bytes(mode_key) {
                                weak_keys = bytes.as_bytes().contains(&b'k');
                                weak_values = bytes.as_bytes().contains(&b'v');
                            } else {
                                crate::debug_unreachable!();
                            }
                        }
                    } else {
                        crate::debug_unreachable!();
                    }
                }

                if weak_keys {
                    for &element_key in table.map.keys() {
                        if let Some(storage_key) = element_key.as_storage_key() {
                            self.acknowledge_weak_association(
                                cache_pools,
                                storage_key,
                                table_key,
                                element_key,
                            );
                        }
                    }
                } else {
                    for key in table.map.keys() {
                        self.mark_table_value(key);
                    }
                }

                if weak_values {
                    for (i, value) in table.list.iter().enumerate() {
                        let Some(storage_key) = value.as_storage_key() else {
                            continue;
                        };

                        self.acknowledge_weak_association(
                            cache_pools,
                            storage_key,
                            table_key,
                            StackValue::Integer((i + 1) as _),
                        );
                    }

                    for (element_key, value) in &table.map {
                        let Some(storage_key) = value.as_storage_key() else {
                            continue;
                        };

                        self.acknowledge_weak_association(
                            cache_pools,
                            storage_key,
                            table_key,
                            *element_key,
                        );
                    }
                } else {
                    for value in &table.list {
                        self.mark_table_value(value);
                    }

                    for value in table.map.values() {
                        self.mark_table_value(value);
                    }
                }
            }
            StorageKey::Coroutine(key) => {
                let Some(co) = heap.get_coroutine(key) else {
                    crate::debug_unreachable!();
                    #[cfg(not(debug_assertions))]
                    return;
                };

                for (continuation, _) in &co.continuation_stack {
                    match continuation {
                        Continuation::Entry(key) => self.mark_storage_key(*key),
                        Continuation::Callback(key, state) => {
                            self.mark_storage_key(key.into());
                            self.mark_value_stack(state);
                        }
                        Continuation::Execution { execution, .. } => {
                            self.mark_execution_context(execution)
                        }
                    }
                }
            }
            StorageKey::StackValue(key) => {
                if let Some(value) = heap.get_stack_value(key) {
                    self.mark_stack_value(value);
                } else {
                    crate::debug_unreachable!();
                }
            }
            _ => {}
        }
    }

    fn mark_heap_key_root(&mut self, key: StorageKey) {
        self.marked.insert(key, Mark::Gray);
        self.phase_queue.push(key);
    }

    fn acknowledge_weak_association(
        &mut self,
        cache_pools: &CachePools,
        key: StorageKey,
        table_key: TableObjectKey,
        element_key: StackValue,
    ) {
        let list = self
            .weak_associations
            .entry(key)
            .or_insert_with(|| cache_pools.create_weak_associations_list());

        list.push((table_key, element_key));
    }
}
