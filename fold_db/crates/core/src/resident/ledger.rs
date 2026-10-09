//! Residency ledger — approximate byte accounting + LRU order for the graph.
//!
//! Mirrors the warm set's tick-based recency shape (`ShardWarmSet` in
//! laststore): a touch is two map operations and the LRU end is `pop_first`,
//! so recency never degrades to a linear scan as the graph grows.
//!
//! The ledger tracks *charges*, not the objects themselves — the graph's maps
//! stay the single owner of resident values. Bytes are estimates (see
//! `types::approx_*`); the budget is a pressure valve, not an allocator.

use std::collections::{BTreeMap, HashMap};

use super::types::DirtyKey;

#[derive(Debug, Default)]
pub(crate) struct ResidentLedger {
    /// Recency order, least recently used first (tick → key).
    order: BTreeMap<u64, DirtyKey>,
    tick_by_key: HashMap<DirtyKey, u64>,
    next_tick: u64,
    bytes_by_key: HashMap<DirtyKey, u64>,
    total_bytes: u64,
}

impl ResidentLedger {
    /// Charge (or re-charge) `key` at `bytes` and mark it most recently used.
    pub(crate) fn charge(&mut self, key: DirtyKey, bytes: u64) {
        if let Some(previous) = self.bytes_by_key.insert(key.clone(), bytes) {
            self.total_bytes = self.total_bytes.saturating_sub(previous);
        }
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.touch(key);
    }

    /// Mark `key` most recently used without changing its charge. A key that
    /// was never charged is ignored (nothing to protect from eviction).
    pub(crate) fn touch_existing(&mut self, key: &DirtyKey) {
        if self.bytes_by_key.contains_key(key) {
            self.touch(key.clone());
        }
    }

    fn touch(&mut self, key: DirtyKey) {
        if let Some(previous) = self.tick_by_key.get(&key) {
            self.order.remove(previous);
        }
        let tick = self.next_tick;
        self.next_tick = self.next_tick.saturating_add(1);
        self.order.insert(tick, key.clone());
        self.tick_by_key.insert(key, tick);
    }

    /// Drop `key`'s charge (evicted or removed from the graph).
    pub(crate) fn discharge(&mut self, key: &DirtyKey) {
        if let Some(bytes) = self.bytes_by_key.remove(key) {
            self.total_bytes = self.total_bytes.saturating_sub(bytes);
        }
        if let Some(tick) = self.tick_by_key.remove(key) {
            self.order.remove(&tick);
        }
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes_by_key.len()
    }

    /// Least-recently-used keys, oldest first. The caller decides dirty-skip
    /// and stops iterating once under budget.
    pub(crate) fn lru_keys(&self) -> impl Iterator<Item = &DirtyKey> {
        self.order.values()
    }

    /// Copy only enough eligible victims to cover the current overage.
    /// Copying the whole ledger for each one-entry overflow amplifies both
    /// allocation and CPU cost precisely when memory is scarce.
    pub(crate) fn eviction_candidates(
        &self,
        mut bytes: u64,
        eligible: impl Fn(&DirtyKey) -> bool,
    ) -> Vec<DirtyKey> {
        let mut victims = Vec::new();
        for key in self.lru_keys() {
            if bytes == 0 {
                break;
            }
            if eligible(key) {
                victims.push(key.clone());
                bytes = bytes.saturating_sub(self.bytes_by_key.get(key).copied().unwrap_or(0));
            }
        }
        victims
    }
}
