//! Byte charges for the warm set gauge (design: docs/lastdb-warm-set-byte-limit-design.md).
//!
//! A charge is a calculated estimate of retained heap bytes, rounded up. The
//! variable parts (strings, vectors, maps, the decoded JSON tree) are measured
//! from capacities; only the per-object constants are estimates. Stage 1 only
//! reports these charges. The record count still controls admission.

use super::{CorePath, SlotKey, SourceKey};
use crate::atom::Atom;
use serde_json::Value;

/// Map entries, `PathState`, and the recency entry for one path (guess; calibrate in stage 2).
const PATH_BASE: usize = 512;
/// One `BTreeMap` entry of a JSON object: inline key and value plus node slack (guess).
const JSON_MAP_ENTRY: usize = 96;
/// One source link: its entries in `slot_sources` and `source_slots`, and the
/// share of the small hash table that each side allocates (guess; calibrate).
const LINK_BASE: usize = 384;
/// One slot: its entries in `slot_owners`, `owned_slots`, `slot_atoms`,
/// `slot_sources`, the molecule tip and key-metadata maps, the copies of the
/// tip entry in `owned_slots`, and the recency map share. Calibrated on
/// 2026-10-19 from a heap profile (about 1.76 KB of tip and molecule entries
/// per slot) plus the map entries; recalibrate in stage 2.
const SLOT_BASE: usize = 2816;
/// Copies of the slot key strings kept by those maps.
const SLOT_KEY_COPIES: usize = 6;
/// One `HashMap` slot of atom metadata: key, value, and control byte (guess).
const HASH_MAP_SLOT: usize = 56;
/// The `Arc` strong and weak counters in front of the atom.
const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
/// Allocator size classes and tree slack, as a ratio of 100 (guess).
const SAFETY_PERCENT: usize = 110;
const ROUND: usize = 16;

const VALUE_SIZE: usize = std::mem::size_of::<Value>();

/// One path: the base cost plus its key strings, which the core copies into
/// `paths`, `slot_owners`, and `order`.
pub(super) fn path_charge(path: &CorePath) -> usize {
    let strings = path.scope.as_ref().map_or(0, String::capacity)
        + path.schema.capacity()
        + path.field.capacity()
        + path.molecule.capacity()
        + path.hash.capacity()
        + path.range.capacity();
    finish(PATH_BASE + 3 * strings)
}

/// One slot: the per-slot map entries and their copies of the slot key.
pub(super) fn slot_charge(slot: &SlotKey) -> usize {
    finish(SLOT_BASE + SLOT_KEY_COPIES * slot_strings(slot))
}

/// One source link. The source key is stored on both sides of the link and the
/// slot key once more in the reverse set. Lengths, not capacities, so the
/// release computes the same charge from the stored copy.
pub(super) fn link_charge(slot: &SlotKey, source: &SourceKey) -> usize {
    let source_strings = source.collection.len() + source.key.len();
    finish(LINK_BASE + 2 * source_strings + slot_strings(slot))
}

fn slot_strings(slot: &SlotKey) -> usize {
    slot.molecule.scope.as_ref().map_or(0, String::len)
        + slot.molecule.molecule.len()
        + slot.hash.len()
        + slot.range.len()
}

/// One atom allocation: the struct, its strings, metadata, and decoded content.
pub(super) fn atom_charge(atom: &Atom) -> usize {
    // `uuid` and the schema name are only reachable as `&str`; their length
    // stands in for capacity. The safety factor covers the difference.
    let strings = atom.uuid().len()
        + atom.source_schema_name().len()
        + atom.source_file_name().map_or(0, String::capacity);
    let metadata = atom.metadata().map_or(0, |map| {
        map.capacity() * HASH_MAP_SLOT
            + map
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity())
                .sum::<usize>()
    });
    finish(
        ARC_HEADER
            + std::mem::size_of::<Atom>()
            + strings
            + metadata
            + json_heap_bytes(atom.content()),
    )
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
