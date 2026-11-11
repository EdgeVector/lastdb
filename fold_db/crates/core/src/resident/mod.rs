//! Warm set byte limit, byte accounting, and LRU eviction over decoded records.

mod records;

use lastdb_model::record_value::{FileMarker, RecordMeta, StoredRecord};
use serde_json::Value;
use std::sync::OnceLock;

pub(crate) use records::RecordCoordinate;

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

#[derive(Default)]
struct WarmBytes {
    record_bytes: usize,
    peak_bytes: usize,
    purged_bytes: u64,
    over_limit_admissions: u64,
}

#[derive(Default)]
pub(crate) struct CoreWarmSet {
    clock: u64,
    records: records::RecordTable,
    hits: u64,
    misses: u64,
    purged: u64,
    bytes: WarmBytes,
}

const PURGE_BATCH: usize = 64;
const JSON_MAP_ENTRY: usize = 96;
const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
const RECORD_BASE: usize = 128;
const SAFETY_PERCENT: usize = 110;
const ROUND: usize = 16;
const VALUE_SIZE: usize = std::mem::size_of::<Value>();

impl CoreWarmSet {
    pub(crate) fn stats(&self) -> CoreWarmStats {
        CoreWarmStats {
            records: self.records.len(),
            held: self.records.held(),
            hits: self.hits,
            misses: self.misses,
            purged: self.purged,
        }
    }

    pub(crate) fn byte_stats(&mut self) -> CoreWarmByteStats {
        self.bytes.peak_bytes = self.bytes.peak_bytes.max(self.bytes.record_bytes);
        CoreWarmByteStats {
            limit_bytes: core_warm_bytes(),
            used_bytes: self.bytes.record_bytes,
            peak_bytes: self.bytes.peak_bytes,
            record_bytes: self.bytes.record_bytes,
            purged_bytes: self.bytes.purged_bytes,
            over_limit_admissions: self.bytes.over_limit_admissions,
        }
    }

    pub(super) fn charge_record(&mut self, charge: usize) {
        self.bytes.record_bytes += charge;
    }

    pub(super) fn release_record(&mut self, charge: usize) {
        self.bytes.record_bytes -= charge;
    }

    pub(crate) fn purge_clean_to_bytes(&mut self, target: usize) -> (usize, usize, usize) {
        let before = self.byte_stats().used_bytes;
        let purged = self.purge_clean_until(target);
        (purged, before, self.byte_stats().used_bytes)
    }

    fn fits(&self, incoming: usize) -> bool {
        self.bytes.record_bytes.saturating_add(incoming) <= core_warm_bytes()
    }

    pub(super) fn trim_to_limit(&mut self, incoming: usize) {
        let limit = core_warm_bytes();
        if self.fits(incoming) {
            return;
        }
        self.purge_clean_until(limit.saturating_sub(incoming));
        if !self.fits(incoming) {
            self.bytes.over_limit_admissions = self.bytes.over_limit_admissions.saturating_add(1);
        }
    }

    fn purge_clean_until(&mut self, target: usize) -> usize {
        let before = self.bytes.record_bytes;
        let mut purged = 0;
        'scan: loop {
            let victims = self.record_victims(PURGE_BATCH);
            if victims.is_empty() {
                break;
            }
            for (_, key) in victims {
                if self.bytes.record_bytes <= target {
                    break 'scan;
                }
                if self.purge_record(&key) {
                    purged += 1;
                }
            }
        }
        self.purged = self.purged.saturating_add(purged as u64);
        let released = before.saturating_sub(self.bytes.record_bytes) as u64;
        self.bytes.purged_bytes = self.bytes.purged_bytes.saturating_add(released);
        purged
    }
}

pub(super) fn record_charge(coordinate: &(String, String), record: &StoredRecord) -> usize {
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
