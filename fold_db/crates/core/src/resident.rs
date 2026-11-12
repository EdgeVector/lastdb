//! Warm set byte limit, byte accounting, and LRU eviction over decoded records.

use lastdb_model::record_value::{FileMarker, RecordMeta, StoredRecord};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

pub(crate) type RecordCoordinate = (String, String);

pub const DEFAULT_CORE_WARM_BYTES: usize = 256 * 1024 * 1024;
pub const CORE_WARM_BYTES_ENV: &str = "LASTDB_CORE_WARM_BYTES";

static CORE_WARM_BYTES_IN_FORCE: OnceLock<usize> = OnceLock::new();

pub fn init_core_warm_bytes_from_env() -> Result<usize, String> {
    let bytes = match std::env::var(CORE_WARM_BYTES_ENV) {
        Err(_) => DEFAULT_CORE_WARM_BYTES,
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(0) => {
                return Err(format!(
                    "{CORE_WARM_BYTES_ENV}=0 is refused; the limit must be positive"
                ));
            }
            Ok(b) => b,
            Err(e) => return Err(format!("invalid {CORE_WARM_BYTES_ENV}={raw:?}: {e}")),
        },
    };
    Ok(*CORE_WARM_BYTES_IN_FORCE.get_or_init(|| bytes))
}

#[must_use]
pub fn core_warm_bytes() -> usize {
    CORE_WARM_BYTES_IN_FORCE
        .get()
        .copied()
        .unwrap_or(DEFAULT_CORE_WARM_BYTES)
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub(crate) struct CoreWarmStats {
    pub(crate) records: usize,
    pub(crate) held: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) purged: u64,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct CoreWarmByteStats {
    pub limit_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
    pub record_bytes: usize,
    pub purged_bytes: u64,
    pub over_limit_admissions: u64,
}

struct WarmRecord {
    record: Arc<StoredRecord>,
    bytes: usize,
    tick: u64,
    charge: usize,
}

#[derive(Default)]
pub(crate) struct CoreWarmSet {
    clock: u64,
    entries: HashMap<Arc<RecordCoordinate>, WarmRecord>,
    order: BTreeMap<u64, Arc<RecordCoordinate>>,
    hits: u64,
    misses: u64,
    purged: u64,
    record_bytes: usize,
    peak_bytes: usize,
    purged_bytes: u64,
    over_limit_admissions: u64,
}

const JSON_MAP_ENTRY: usize = 96;
const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
const RECORD_BASE: usize = 128;
const SAFETY_PERCENT: usize = 110;
const ROUND: usize = 16;
const VALUE_SIZE: usize = std::mem::size_of::<Value>();

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        CoreWarmStats {
            records: self.entries.len(),
            held: self
                .entries
                .values()
                .filter(|e| Arc::strong_count(&e.record) > 1)
                .count(),
            hits: self.hits,
            misses: self.misses,
            purged: self.purged,
        }
    }

    pub(crate) fn byte_stats(&mut self) -> CoreWarmByteStats {
        self.peak_bytes = self.peak_bytes.max(self.record_bytes);
        CoreWarmByteStats {
            limit_bytes: core_warm_bytes(),
            used_bytes: self.record_bytes,
            peak_bytes: self.peak_bytes,
            record_bytes: self.record_bytes,
            purged_bytes: self.purged_bytes,
            over_limit_admissions: self.over_limit_admissions,
        }
    }

    pub(crate) fn purge_clean_to_bytes(&mut self, target: usize) -> (usize, usize, usize) {
        let before = self.byte_stats().used_bytes;
        let purged = self.purge_clean_until(target);
        (purged, before, self.byte_stats().used_bytes)
    }

    pub(crate) fn record_lookup(
        &mut self,
        coordinates: &[RecordCoordinate],
    ) -> Vec<Option<(Arc<StoredRecord>, usize)>> {
        coordinates
            .iter()
            .map(|c| {
                let found = self.touch_record(c);
                if found.is_some() {
                    self.hits = self.hits.saturating_add(1);
                } else {
                    self.misses = self.misses.saturating_add(1);
                }
                found
            })
            .collect()
    }

    pub(crate) fn admit_records(
        &mut self,
        records: Vec<(RecordCoordinate, Arc<StoredRecord>, usize)>,
    ) {
        if records.is_empty() {
            return;
        }
        for (coord, record, bytes) in records {
            self.remove_record(&coord);
            let charge = record_charge(&coord, &record);
            let tick = self.next_tick();
            let key = Arc::new(coord);
            self.order.insert(tick, Arc::clone(&key));
            self.entries.insert(
                key,
                WarmRecord {
                    record,
                    bytes,
                    tick,
                    charge,
                },
            );
            self.record_bytes += charge;
        }
        let limit = core_warm_bytes();
        if self.record_bytes > limit {
            self.purge_clean_until(limit);
            if self.record_bytes > limit {
                self.over_limit_admissions = self.over_limit_admissions.saturating_add(1);
            }
        }
    }

    pub(crate) fn invalidate_records(&mut self, changes: &[RecordCoordinate]) {
        if !self.entries.is_empty() {
            for c in changes {
                self.remove_record(c);
            }
        }
    }

    fn purge_clean_until(&mut self, target: usize) -> usize {
        if self.record_bytes <= target {
            return 0;
        }
        let (before, needed) = (self.record_bytes, self.record_bytes - target);
        let (mut released, mut victims) = (0usize, Vec::new());
        for key in self.order.values() {
            if released >= needed {
                break;
            }
            if let Some(e) = self
                .entries
                .get::<RecordCoordinate>(key)
                .filter(|e| Arc::strong_count(&e.record) == 1)
            {
                released = released.saturating_add(e.charge);
                victims.push(Arc::clone(key));
            }
        }
        let purged = victims.iter().filter(|k| self.remove_record(k)).count();
        self.purged = self.purged.saturating_add(purged as u64);
        self.purged_bytes = self
            .purged_bytes
            .saturating_add(before.saturating_sub(self.record_bytes) as u64);
        purged
    }

    fn next_tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            let all = std::mem::take(&mut self.order);
            self.clock = 0;
            for (_, key) in all {
                self.clock += 1;
                self.entries
                    .get_mut::<RecordCoordinate>(&key)
                    .expect("ordered warm record")
                    .tick = self.clock;
                self.order.insert(self.clock, key);
            }
        }
        self.clock += 1;
        self.clock
    }

    fn touch_record(&mut self, coord: &RecordCoordinate) -> Option<(Arc<StoredRecord>, usize)> {
        if !self.entries.contains_key(coord) {
            return None;
        }
        let tick = self.next_tick();
        let entry = self.entries.get_mut(coord)?;
        let key = self
            .order
            .remove(&std::mem::replace(&mut entry.tick, tick))?;
        self.order.insert(tick, key);
        Some((Arc::clone(&entry.record), entry.bytes))
    }

    fn remove_record(&mut self, coord: &RecordCoordinate) -> bool {
        let Some(entry) = self.entries.remove(coord) else {
            return false;
        };
        self.order.remove(&entry.tick);
        self.record_bytes -= entry.charge;
        true
    }
}

fn record_charge(coord: &RecordCoordinate, record: &StoredRecord) -> usize {
    let key_cap = |k: &lastdb_model::schema::types::KeyValue| {
        k.hash.as_ref().map_or(0, String::capacity) + k.range.as_ref().map_or(0, String::capacity)
    };
    let fields: usize = record
        .fields
        .iter()
        .map(|(n, v)| JSON_MAP_ENTRY + n.capacity() + json_heap_bytes(v))
        .sum();
    let addresses = record
        .alternates
        .iter()
        .map(|(n, k)| JSON_MAP_ENTRY + n.capacity() + k.as_ref().map_or(0, key_cap))
        .sum::<usize>()
        + key_cap(&record.key);
    let meta = record.meta.as_ref().map_or(0, |m| {
        std::mem::size_of::<RecordMeta>()
            + m.source_file_name.as_ref().map_or(0, String::capacity)
            + m.metadata
                .iter()
                .map(|(k, v)| JSON_MAP_ENTRY + k.capacity() + v.capacity())
                .sum::<usize>()
    });
    let marker = |f: &FileMarker| {
        std::mem::size_of::<FileMarker>()
            + f.name.as_ref().map_or(0, String::capacity)
            + f.media_type.as_ref().map_or(0, String::capacity)
            + f.sha256.capacity()
    };
    let raw = RECORD_BASE
        + ARC_HEADER
        + std::mem::size_of::<StoredRecord>()
        + coord.0.capacity()
        + coord.1.capacity()
        + fields
        + addresses
        + meta
        + record.file.as_ref().map_or(0, marker)
        + record.inline_file.as_ref().map_or(0, marker)
        + record.stamp.position.source_stream.as_str().len()
        + record
            .file_version
            .as_ref()
            .map_or(0, |v| v.source_stream.as_str().len());
    raw.saturating_mul(SAFETY_PERCENT)
        .div_ceil(100)
        .div_ceil(ROUND)
        .saturating_mul(ROUND)
}

fn json_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(s) => s.capacity(),
        Value::Array(a) => a.capacity() * VALUE_SIZE + a.iter().map(json_heap_bytes).sum::<usize>(),
        Value::Object(m) => m
            .iter()
            .map(|(k, v)| JSON_MAP_ENTRY + k.capacity() + json_heap_bytes(v))
            .sum(),
    }
}
