//! Current typed definitions share the core owner and the query's held lifetime.

use super::*;
use crate::resident::core_keys::{CoreKeyBudget, CoreKeyReservation};
use std::any::Any;
use std::ops::Deref;

const MAX_ENTRIES: usize = 10_000;
const MEMORY_BYTES: usize = 16 * 1024 * 1024;
pub(super) type MemoryKey = (usize, String);

/// An immutable charged definition, prepared before its publication boundary.
#[derive(Clone)]
pub(crate) struct PreparedCoreMemory {
    pub(crate) key: MemoryKey,
    pub(super) value: Arc<dyn Any + Send + Sync>,
    sources: Arc<Vec<CoreMemorySource>>,
}

impl PreparedCoreMemory {
    pub(crate) fn typed<T: Any + Send + Sync>(&self) -> Option<Arc<CoreMemoryValue<T>>> {
        Arc::clone(&self.value).downcast().ok()
    }

    pub(crate) fn sources(&self) -> &[CoreMemorySource] {
        self.sources.as_slice()
    }
}

/// An immutable core object. Its charge survives purge while a call holds it.
pub struct CoreMemoryValue<T> {
    value: T,
    _charge: Option<CoreKeyReservation>,
}

impl<T> Deref for CoreMemoryValue<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

/// Depend on every readable spelling, including currently absent spellings.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CoreMemorySource {
    pub(crate) collection: String,
    pub(crate) key: Option<String>,
}

impl CoreMemorySource {
    pub(crate) fn collection(name: &str) -> Self {
        Self {
            collection: name.to_owned(),
            key: None,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct CoreMemoryStats {
    pub entries: usize,
    pub limit_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub invalidated: u64,
    pub purged: u64,
    pub refused: u64,
}

struct Entry {
    value: Arc<dyn Any + Send + Sync>,
    sources: Vec<CoreMemorySource>,
    tick: u64,
}

pub(super) struct CoreMemory {
    entries: HashMap<MemoryKey, Entry>,
    sources: HashMap<CoreMemorySource, HashSet<MemoryKey>>,
    order: BTreeMap<u64, MemoryKey>,
    budget: Arc<CoreKeyBudget>,
    clock: u64,
    hits: u64,
    misses: u64,
    invalidated: u64,
    purged: u64,
    refused: u64,
}

impl Default for CoreMemory {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            sources: HashMap::new(),
            order: BTreeMap::new(),
            budget: Arc::new(CoreKeyBudget::new(MEMORY_BYTES)),
            clock: 0,
            hits: 0,
            misses: 0,
            invalidated: 0,
            purged: 0,
            refused: 0,
        }
    }
}

impl CoreMemory {
    pub(super) fn prepare<T: Any + Send + Sync>(
        &mut self,
        key: MemoryKey,
        value: T,
        sources: Vec<CoreMemorySource>,
        bytes: usize,
    ) -> Result<PreparedCoreMemory, CoreCapacity> {
        let bytes = allocation_bound(&key, &sources, bytes);
        if sources.is_empty() || bytes > MEMORY_BYTES {
            self.refused = self.refused.saturating_add(1);
            return Err(memory_capacity());
        }
        let mut charge = None;
        for _ in 0..=32 {
            charge = self.budget.reserve_temporary(bytes).ok();
            if charge.is_some() {
                break;
            }
            let Some((_, oldest)) = self.order.first_key_value() else {
                break;
            };
            let oldest = oldest.clone();
            self.remove(&oldest);
            self.purged = self.purged.saturating_add(1);
        }
        let Some(charge) = charge else {
            self.refused = self.refused.saturating_add(1);
            return Err(memory_capacity());
        };
        self.reserve_install(1)?;
        Ok(PreparedCoreMemory {
            key,
            value: Arc::new(CoreMemoryValue {
                value,
                _charge: Some(charge),
            }),
            sources: Arc::new(sources),
        })
    }

    pub(super) fn reserve_install(&mut self, count: usize) -> Result<(), CoreCapacity> {
        if count > MAX_ENTRIES {
            return Err(memory_capacity());
        }
        while self.entries.len().saturating_add(count) > MAX_ENTRIES {
            let Some((_, key)) = self.order.first_key_value() else {
                return Err(memory_capacity());
            };
            let key = key.clone();
            self.remove(&key);
            self.purged = self.purged.saturating_add(1);
        }
        self.entries
            .try_reserve(count)
            .map_err(|_| memory_capacity())?;
        self.sources
            .try_reserve(count.saturating_mul(64))
            .map_err(|_| memory_capacity())?;
        Ok(())
    }

    pub(super) fn install_prepared(&mut self, definition: PreparedCoreMemory) {
        let PreparedCoreMemory {
            key,
            value,
            sources,
        } = definition;
        self.install(key, value, sources.as_ref().clone());
    }

    fn install(
        &mut self,
        key: MemoryKey,
        value: Arc<dyn Any + Send + Sync>,
        sources: Vec<CoreMemorySource>,
    ) {
        self.remove(&key);
        for source in &sources {
            self.sources
                .entry(source.clone())
                .or_default()
                .insert(key.clone());
        }
        self.clock = self.clock.wrapping_add(1);
        self.order.insert(self.clock, key.clone());
        self.entries.insert(
            key,
            Entry {
                value,
                sources,
                tick: self.clock,
            },
        );
    }

    pub(super) fn get<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: &str,
    ) -> Option<Arc<CoreMemoryValue<T>>> {
        let key = (owner, name.to_owned());
        let Some(entry) = self.entries.get_mut(&key) else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        let value = Arc::clone(&entry.value).downcast().ok()?;
        self.order.remove(&entry.tick);
        self.clock = self.clock.wrapping_add(1);
        entry.tick = self.clock;
        self.order.insert(entry.tick, key);
        self.hits = self.hits.saturating_add(1);
        Some(value)
    }

    pub(super) fn insert<T: Any + Send + Sync>(
        &mut self,
        key: MemoryKey,
        value: T,
        sources: Vec<CoreMemorySource>,
        bytes: usize,
    ) -> Arc<CoreMemoryValue<T>> {
        let bytes = allocation_bound(&key, &sources, bytes);
        // These objects can never enter shared memory. Refuse them before any
        // reservation or purge, so repeated misses cannot discard useful state.
        if sources.is_empty() || bytes > MEMORY_BYTES {
            self.refused = self.refused.saturating_add(1);
            return CoreWarmSet::memory_uncached(value);
        }
        let mut charge = None;
        // Bounded purge: long calls can retain charges after their entry leaves.
        for _ in 0..=32 {
            if self.entries.len() < MAX_ENTRIES {
                charge = self.budget.reserve_temporary(bytes).ok();
                if charge.is_some() {
                    break;
                }
            }
            let Some((_, oldest)) = self.order.first_key_value() else {
                break;
            };
            let oldest = oldest.clone();
            self.remove(&oldest);
            self.purged = self.purged.saturating_add(1);
        }
        let retained = charge.is_some();
        let value = Arc::new(CoreMemoryValue {
            value,
            _charge: charge,
        });
        if retained {
            self.install(key, value.clone(), sources);
        } else {
            self.refused = self.refused.saturating_add(1);
        }
        value
    }

    pub(super) fn invalidate(&mut self, changes: &[(String, String)]) {
        for (collection, key) in changes {
            for source in [
                CoreMemorySource {
                    collection: collection.clone(),
                    key: Some(key.clone()),
                },
                CoreMemorySource::collection(collection),
            ] {
                let keys: Vec<_> = self
                    .sources
                    .get(&source)
                    .into_iter()
                    .flatten()
                    .cloned()
                    .collect();
                for key in keys {
                    self.remove(&key);
                    self.invalidated = self.invalidated.saturating_add(1);
                }
            }
        }
    }

    fn remove(&mut self, key: &MemoryKey) {
        let Some(entry) = self.entries.remove(key) else {
            return;
        };
        self.order.remove(&entry.tick);
        for source in entry.sources {
            if let Some(keys) = self.sources.get_mut(&source) {
                keys.remove(key);
                if keys.is_empty() {
                    self.sources.remove(&source);
                }
            }
        }
    }

    pub(super) fn stats(&self) -> CoreMemoryStats {
        let budget = self.budget.stats();
        CoreMemoryStats {
            entries: self.entries.len(),
            limit_bytes: budget.limit_bytes,
            used_bytes: budget.used_bytes,
            peak_bytes: budget.peak_bytes,
            hits: self.hits,
            misses: self.misses,
            invalidated: self.invalidated,
            purged: self.purged,
            refused: self.refused,
        }
    }
}

fn allocation_bound(key: &MemoryKey, sources: &[CoreMemorySource], bytes: usize) -> usize {
    // Keys also occur in the recency map and reverse-source sets. Sources occur
    // in both the entry and reverse-source map.
    sources.iter().fold(
        bytes
            .saturating_add(key.1.capacity().saturating_mul(3))
            .saturating_add(1024),
        |total, source| {
            total
                .saturating_add(source.collection.capacity().saturating_mul(3))
                .saturating_add(
                    source
                        .key
                        .as_ref()
                        .map_or(0, String::capacity)
                        .saturating_mul(3),
                )
                .saturating_add(key.1.capacity())
                .saturating_add(512)
        },
    )
}

impl CoreWarmSet {
    pub(crate) fn prepare_memory<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: String,
        value: T,
        sources: Vec<CoreMemorySource>,
        bytes: usize,
    ) -> Result<PreparedCoreMemory, CoreCapacity> {
        self.memory.prepare((owner, name), value, sources, bytes)
    }

    pub(crate) fn memory_get<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: &str,
    ) -> Option<Arc<CoreMemoryValue<T>>> {
        self.memory.get(owner, name)
    }

    pub(crate) fn memory_insert<T: Any + Send + Sync>(
        &mut self,
        owner: usize,
        name: String,
        value: T,
        sources: Vec<CoreMemorySource>,
        bytes: usize,
    ) -> Arc<CoreMemoryValue<T>> {
        self.memory.insert((owner, name), value, sources, bytes)
    }

    pub(crate) fn memory_uncached<T>(value: T) -> Arc<CoreMemoryValue<T>> {
        Arc::new(CoreMemoryValue {
            value,
            _charge: None,
        })
    }

    pub(crate) fn memory_stats(&self) -> CoreMemoryStats {
        self.memory.stats()
    }
}

fn memory_capacity() -> CoreCapacity {
    CoreCapacity {
        detail: "typed core definition allowance refused the publication",
    }
}
