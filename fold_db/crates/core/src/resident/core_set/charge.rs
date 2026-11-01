//! Byte charges for the warm set gauge (design: docs/lastdb-warm-set-byte-limit-design.md).
//!
//! A charge is a calculated estimate of retained heap bytes, rounded up. The
//! variable parts (strings, vectors, maps, the decoded JSON tree) are measured
//! from capacities; only the per-object constants are estimates. Admission
//! purges clean records against this byte charge.

use lastdb_model::record_value::{FieldStamp, RecordMeta, StoredRecord};
use serde_json::Value;

/// One `BTreeMap` entry of a JSON object: inline key and value plus node slack (guess).
const JSON_MAP_ENTRY: usize = 96;
/// The `Arc` strong and weak counters in front of the record.
const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
/// One warm record: its map slot, the `Arc` of its key pair, and its recency
/// entry. The key strings are charged by their capacity (estimate).
const RECORD_BASE: usize = 128;
/// Allocator size classes and tree slack, as a ratio of 100 (guess).
const SAFETY_PERCENT: usize = 110;
const ROUND: usize = 16;

const VALUE_SIZE: usize = std::mem::size_of::<Value>();

/// One decoded record of a format 7 home and its key coordinate.
pub(super) fn record_charge(coordinate: &(String, String), record: &StoredRecord) -> usize {
    let fields = record
        .fields
        .iter()
        .map(|(name, value)| JSON_MAP_ENTRY + name.capacity() + json_heap_bytes(value))
        .sum::<usize>();
    let stamps = record
        .stamps
        .iter()
        .map(|(name, stamp)| JSON_MAP_ENTRY + name.capacity() + stamp_heap_bytes(stamp))
        .sum::<usize>();
    let meta = record.meta.as_ref().map_or(0, |meta| {
        std::mem::size_of::<RecordMeta>()
            + meta.source_file_name.as_ref().map_or(0, String::capacity)
            + meta
                .metadata
                .iter()
                .map(|(key, value)| JSON_MAP_ENTRY + key.capacity() + value.capacity())
                .sum::<usize>()
            + stamp_heap_bytes(&meta.stamp)
    });
    finish(
        RECORD_BASE
            + ARC_HEADER
            + std::mem::size_of::<StoredRecord>()
            + coordinate.0.capacity()
            + coordinate.1.capacity()
            + fields
            + stamps
            + meta
            + record.deleted.as_ref().map_or(0, stamp_heap_bytes),
    )
}

fn stamp_heap_bytes(stamp: &FieldStamp) -> usize {
    stamp.device_id.capacity() + stamp.mutation_id.capacity()
}

/// Heap bytes that a JSON value owns outside its own inline slot.
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

fn finish(bytes: usize) -> usize {
    let scaled = bytes.saturating_mul(SAFETY_PERCENT).div_ceil(100);
    scaled.div_ceil(ROUND).saturating_mul(ROUND)
}
