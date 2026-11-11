//! Warm set byte limit, byte accounting, and LRU eviction over decoded records.

use lastdb_model::record_value::{FileMarker, RecordMeta, StoredRecord};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

pub(crate) type RecordCoordinate = (String, String);

pub const DEFAULT_CORE_WARM_BYTES: usize = 256 * 1024 * 1024;
pub const CORE_WARM_BYTES_ENV: &str = "LASTDB_CORE_WARM_BYTES";

static CORE_WARM_BYTES_IN_FORCE: OnceLock<usize> = OnceLock::new();

pub fn parse_core_warm_bytes(raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_CORE_WARM_BYTES);
    };
    match raw.trim().parse::<usize>() {
        Ok(0) => Err(format!(
            "{CORE_WARM_BYTES_ENV}=0 is refused; the limit must be positive"
        )),
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(format!("invalid {CORE_WARM_BYTES_ENV}={raw:?}: {error}")),
    }
}

pub fn init_core_warm_bytes_from_env() -> Result<usize, String> {
    let bytes = parse_core_warm_bytes(std::env::var(CORE_WARM_BYTES_ENV).ok().as_deref())?;
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
                .filter(|entry| Arc::strong_count(&entry.record) > 1)
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
            .map(|coordinate| {
                let found = self.touch_record(coordinate);
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
        for (coordinate, record, bytes) in records {
            self.remove_record(&coordinate);
            let charge = record_charge(&coordinate, &record);
            let tick = self.next_tick();
            let key = Arc::new(coordinate);
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
        if self.entries.is_empty() {
            return;
        }
        for coordinate in changes {
            self.remove_record(coordinate);
        }
    }

    fn purge_clean_until(&mut self, target: usize) -> usize {
        if self.record_bytes <= target {
            return 0;
        }
        let before = self.record_bytes;
        let needed = before - target;
        let mut released_plan = 0usize;
        let mut victims = Vec::new();
        for key in self.order.values() {
            if released_plan >= needed {
                break;
            }
            if let Some(entry) = self.entries.get::<RecordCoordinate>(key) {
                if Arc::strong_count(&entry.record) == 1 {
                    released_plan = released_plan.saturating_add(entry.charge);
                    victims.push(Arc::clone(key));
                }
            }
        }
        let mut purged = 0;
        for key in victims {
            if self.record_bytes <= target {
                break;
            }
            if self
                .entries
                .get::<RecordCoordinate>(&key)
                .is_some_and(|entry| Arc::strong_count(&entry.record) == 1)
                && self.remove_record(&key)
            {
                purged += 1;
            }
        }
        self.purged = self.purged.saturating_add(purged as u64);
        let released = before.saturating_sub(self.record_bytes) as u64;
        self.purged_bytes = self.purged_bytes.saturating_add(released);
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

    fn touch_record(
        &mut self,
        coordinate: &RecordCoordinate,
    ) -> Option<(Arc<StoredRecord>, usize)> {
        if !self.entries.contains_key(coordinate) {
            return None;
        }
        let tick = self.next_tick();
        let entry = self.entries.get_mut(coordinate).expect("warm record");
        let old = std::mem::replace(&mut entry.tick, tick);
        let found = (Arc::clone(&entry.record), entry.bytes);
        let key = self.order.remove(&old).expect("ordered warm record");
        self.order.insert(tick, key);
        Some(found)
    }

    fn remove_record(&mut self, coordinate: &RecordCoordinate) -> bool {
        let Some(entry) = self.entries.remove(coordinate) else {
            return false;
        };
        self.order.remove(&entry.tick);
        self.record_bytes -= entry.charge;
        true
    }
}

fn record_charge(coordinate: &RecordCoordinate, record: &StoredRecord) -> usize {
    let fields = record
        .fields
        .iter()
        .map(|(name, value)| JSON_MAP_ENTRY + name.capacity() + json_heap_bytes(value))
        .sum::<usize>();
    let addresses = record
        .alternates
        .iter()
        .map(|(name, key)| {
            JSON_MAP_ENTRY + name.capacity() + key.as_ref().map_or(0, key_heap_bytes)
        })
        .sum::<usize>()
        + key_heap_bytes(&record.key);
    let meta = record.meta.as_ref().map_or(0, |meta| {
        std::mem::size_of::<RecordMeta>()
            + meta.source_file_name.as_ref().map_or(0, String::capacity)
            + meta
                .metadata
                .iter()
                .map(|(key, value)| JSON_MAP_ENTRY + key.capacity() + value.capacity())
                .sum::<usize>()
    });
    let marker_bytes = |file: &FileMarker| {
        std::mem::size_of::<FileMarker>()
            + file.name.as_ref().map_or(0, String::capacity)
            + file.media_type.as_ref().map_or(0, String::capacity)
            + file.sha256.capacity()
    };
    let raw = RECORD_BASE
        + ARC_HEADER
        + std::mem::size_of::<StoredRecord>()
        + coordinate.0.capacity()
        + coordinate.1.capacity()
        + fields
        + addresses
        + meta
        + record.file.as_ref().map_or(0, marker_bytes)
        + record.inline_file.as_ref().map_or(0, marker_bytes)
        + record.stamp.position.source_stream.as_str().len()
        + record
            .file_version
            .as_ref()
            .map_or(0, |version| version.source_stream.as_str().len());
    raw.saturating_mul(SAFETY_PERCENT)
        .div_ceil(100)
        .div_ceil(ROUND)
        .saturating_mul(ROUND)
}

fn key_heap_bytes(key: &lastdb_model::schema::types::KeyValue) -> usize {
    key.hash.as_ref().map_or(0, String::capacity) + key.range.as_ref().map_or(0, String::capacity)
}

fn json_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(text) => text.capacity(),
        Value::Array(items) => {
            items.capacity() * VALUE_SIZE + items.iter().map(json_heap_bytes).sum::<usize>()
        }
        Value::Object(map) => map
            .iter()
            .map(|(key, child)| JSON_MAP_ENTRY + key.capacity() + json_heap_bytes(child))
            .sum(),
    }
}
