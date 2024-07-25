use std::rc::Rc;

use super::HeapValue;
use super::{Heap, HeapKey};
use crate::interpreter::cache_pools::CachePools;
use crate::interpreter::cache_pools::RECYCLE_LIMIT;
use crate::interpreter::execution::ExecutionContext;
use crate::interpreter::metatable_keys::MetatableKeys;
use crate::interpreter::value_stack::StackValue;
use crate::interpreter::Continuation;
use crate::{FastHashMap, FastHashSet};

/// Configuration for the incremental garbage collector
#[derive(Clone)]
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
pub(crate) struct GarbageCollector {
    phase: Phase,
    used_memory: usize,
    /// how many bytes we've accumulated since the last step
    accumulated: usize,
    pub(crate) config: GarbageCollectorConfig,
    traversed_definitions: FastHashSet<usize>,
    /// gray + black, excluded keys are white
    marked: slotmap::SecondaryMap<HeapKey, Mark>,
    /// gray or pending sweep
    phase_queue: Vec<HeapKey>,
    /// element_key/value -> Vec<(table_key, element_key)>
    weak_associations: FastHashMap<HeapKey, Vec<(HeapKey, StackValue)>>,
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

    pub(crate) fn acknowledge_write(&mut self, heap_key: HeapKey) {
        let Some(mark) = self.marked.get_mut(heap_key) else {
            // not marked, we'll handle this through normal traversal
            return;
        };

        if !matches!(self.phase, Phase::Mark) || matches!(mark, Mark::Gray) {
            // not marking or already in the gray queue
            return;
        }

        // mark as gray again and place in the queue
        *mark = Mark::Gray;
        self.phase_queue.push(heap_key);
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
        execution_stack: &[ExecutionContext],
        heap: &mut Heap,
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
            self.mark_roots(heap, execution_stack);
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
        execution_stack: &[ExecutionContext],
        heap: &mut Heap,
    ) {
        self.mark_roots(heap, execution_stack);

        while !self.phase_queue.is_empty() {
            self.traverse_gray(metatable_keys, cache_pools, heap, None);
        }

        self.init_sweep(heap);
        self.sweep(cache_pools, heap, None);
        self.accumulated = 0;
    }

    fn mark_roots(&mut self, heap: &mut Heap, execution_stack: &[ExecutionContext]) {
        // identify ref roots and mark them
        heap.ref_roots.retain(|&key, counter| {
            let keep = counter.count() > 0;

            if keep {
                self.mark_heap_key_root(key);
            }

            keep
        });

        // mark the stack
        for execution in execution_stack {
            self.mark_execution_context(execution);
        }
    }

    fn mark_execution_context(&mut self, execution: &ExecutionContext) {
        for value in execution.value_stack.iter() {
            self.mark_stack_value(value);
        }

        for call in &execution.call_stack {
            for value in call.up_values.iter() {
                self.mark_stack_value(value);
            }
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
            if !self.marked.contains_key(key) {
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
                for &(table_key, element_key) in &list {
                    let Some(table_heap_value) = heap.storage.get_mut(table_key) else {
                        continue;
                    };

                    let HeapValue::Table(table) = table_heap_value else {
                        unreachable!();
                    };

                    // we need to test to see if the association is still true

                    // test key
                    if element_key == StackValue::HeapValue(key) {
                        table.set(element_key, StackValue::default());
                        continue;
                    }

                    // test value
                    if table.get(element_key) == StackValue::HeapValue(key) {
                        table.set(element_key, StackValue::default());
                    }
                }

                cache_pools.store_weak_associations_list(list);
            };

            // delete
            let value = heap.storage.remove(key).unwrap();

            self.used_memory -= value.gc_size();

            match value {
                HeapValue::Bytes(bytes) => {
                    heap.byte_strings.remove(&bytes);
                }
                HeapValue::Table(mut table) => {
                    if heap.recycled_tables.len() < RECYCLE_LIMIT {
                        table.reset();
                        heap.recycled_tables.push(table);
                    }
                }
                _ => {}
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
        let StackValue::HeapValue(key) = value else {
            return;
        };

        self.mark_heap_key(*key);
    }

    fn mark_stack_value(&mut self, value: &StackValue) {
        let (StackValue::HeapValue(key) | StackValue::Pointer(key)) = value else {
            return;
        };

        self.mark_heap_key(*key);
    }

    fn mark_heap_key(&mut self, key: HeapKey) {
        if self.marked.contains_key(key) {
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
        key: HeapKey,
    ) {
        let value = heap.storage.get(key).unwrap();

        match value {
            HeapValue::Function(function) => {
                for value in function.up_values.iter() {
                    self.mark_stack_value(value);
                }

                let definition_key = Rc::as_ptr(&function.definition) as usize;

                if self.traversed_definitions.insert(definition_key) {
                    for key in &function.definition.byte_strings {
                        self.mark_heap_key(*key);
                    }

                    for key in &function.definition.functions {
                        self.mark_heap_key(*key);
                    }
                }
            }
            HeapValue::Table(table) => {
                let mut weak_keys = false;
                let mut weak_values = false;

                if let Some(key) = table.metatable {
                    self.mark_heap_key(key);

                    let HeapValue::Table(metatable) = heap.storage.get(key).unwrap() else {
                        unreachable!();
                    };

                    let mode_key = metatable_keys.mode.0.key.into();
                    let mode_value = metatable.get(mode_key);

                    if let StackValue::HeapValue(mode_heap_key) = mode_value {
                        if let HeapValue::Bytes(bytes) = heap.storage.get(mode_heap_key).unwrap() {
                            weak_keys = bytes.as_bytes().contains(&b'k');
                            weak_values = bytes.as_bytes().contains(&b'v');
                        }
                    }
                }

                if weak_keys {
                    let table_heap_key = key;

                    for &element_key in table.map.keys() {
                        if let StackValue::HeapValue(heap_key) = element_key {
                            self.acknowledge_weak_association(
                                cache_pools,
                                heap_key,
                                table_heap_key,
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
                    let table_heap_key = key;

                    for (i, value) in table.list.iter().enumerate() {
                        let StackValue::HeapValue(heap_key) = value else {
                            continue;
                        };

                        self.acknowledge_weak_association(
                            cache_pools,
                            *heap_key,
                            table_heap_key,
                            StackValue::Integer((i + 1) as _),
                        );
                    }

                    for (element_key, value) in &table.map {
                        let StackValue::HeapValue(heap_key) = value else {
                            continue;
                        };

                        self.acknowledge_weak_association(
                            cache_pools,
                            *heap_key,
                            table_heap_key,
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
            HeapValue::Coroutine(co) => {
                for (continuation, _) in &co.continuation_stack {
                    match continuation {
                        Continuation::Entry(key) => self.mark_heap_key(*key),
                        Continuation::Callback(_) => {}
                        Continuation::Execution { execution, .. } => {
                            self.mark_execution_context(execution)
                        }
                    }
                }
            }
            HeapValue::StackValue(StackValue::HeapValue(key)) => self.mark_heap_key(*key),
            _ => {}
        }
    }

    fn mark_heap_key_root(&mut self, key: HeapKey) {
        self.marked.insert(key, Mark::Gray);
        self.phase_queue.push(key);
    }

    fn acknowledge_weak_association(
        &mut self,
        cache_pools: &CachePools,
        heap_key: HeapKey,
        table_heap_key: HeapKey,
        element_key: StackValue,
    ) {
        let list = self
            .weak_associations
            .entry(heap_key)
            .or_insert_with(|| cache_pools.create_weak_associations_list());

        list.push((table_heap_key, element_key));
    }
}
