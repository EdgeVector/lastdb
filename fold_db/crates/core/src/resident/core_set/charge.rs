//! Byte charges for the warm set gauge (design: docs/lastdb-warm-set-byte-limit-design.md).
//!
//! A charge is a calculated estimate of retained heap bytes, rounded up. The
//! variable parts (strings, vectors, maps, the decoded JSON tree) are measured
//! from capacities; only the per-object constants are estimates. Admission
//! purges clean records against this byte charge.

use super::SlotKey;
use crate::atom::{Atom, AtomEntry, KeyMetadata};
use serde_json::Value;

/// One path: its path table entry, index bucket, and recency entry. The
/// schema and field names are interned and shared by every path that uses
/// them, so they are not charged. Calibrated on 2026-10-20 from a heap
/// profile of the chunked path table and recency map (about 93 bytes per
/// path).
const PATH_BASE: usize = 80;
/// One `BTreeMap` entry of a JSON object: inline key and value plus node slack (guess).
const JSON_MAP_ENTRY: usize = 96;
/// One source link: one id in the slot's list and one in the source's set.
/// Both lists are usually inline, so this covers spills and table growth.
const LINK_BASE: usize = 8;
/// One interned source key: its id-only index bucket, its table entry, and
/// the header of its packed allocation. A delete barrier shares the packed
/// `mk:` key and is charged this base only. Calibrated on 2026-10-19 (heap
/// profile: about 450 bytes of sources per path).
const SOURCE_BASE: usize = 72;
/// One slot: its chunked table entry with the inline tip, its index bucket,
/// and its interned key allocations. The tip's strings and boxed metadata are
/// charged by `tip_heap_charge`. A clean read keeps no owned copy of the tip.
/// Calibrated on 2026-10-20 from a heap profile after the molecule copy left
/// (about 620 bytes per slot with a small tip, 1.34 KB before).
const SLOT_BASE: usize = 350;
/// The exact source marker adds one optional interned source id to each slot.
const TIP_SOURCE_BYTES: usize = std::mem::size_of::<Option<u32>>();
/// Copies of the slot hash and range strings: the interned slot key only.
const SLOT_KEY_COPIES: usize = 1;
/// One `HashMap` slot of atom metadata: key, value, and control byte (guess).
const HASH_MAP_SLOT: usize = 56;
/// The `Arc` strong and weak counters in front of the atom.
const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
/// Allocator size classes and tree slack, as a ratio of 100 (guess).
const SAFETY_PERCENT: usize = 110;
const ROUND: usize = 16;

const VALUE_SIZE: usize = std::mem::size_of::<Value>();

/// One path. Its strings live in the slot key and the name table.
pub(super) fn path_charge() -> usize {
    finish(PATH_BASE)
}

/// One slot: the per-slot entries and the copies of its hash and range.
/// Molecule names are shared by every slot of the molecule and not charged.
pub(super) fn slot_charge(slot: &SlotKey) -> usize {
    finish(SLOT_BASE + TIP_SOURCE_BYTES + SLOT_KEY_COPIES * (slot.hash.len() + slot.range.len()))
}

/// Heap owned by the slot's tip, apart from the fixed slot cost: the tip's
/// strings and boxed signer part, and the boxed key metadata.
pub(super) fn tip_heap_charge(entry: &AtomEntry, metadata: Option<&KeyMetadata>) -> usize {
    let strings = [
        &entry.atom_uuid,
        &entry.mutation_uuid,
        &entry.device_id,
        &entry.origin_device,
    ]
    .into_iter()
    .map(String::capacity)
    .sum::<usize>();
    // The boxed signer part: its box, strings and provenance.
    let signer = entry.extra_heap_bytes();
    let metadata = metadata.map_or(0, |metadata| {
        std::mem::size_of::<KeyMetadata>()
            + metadata
                .source_file_name
                .as_ref()
                .map_or(0, String::capacity)
            + metadata.metadata.as_ref().map_or(0, metadata_heap_bytes)
    });
    finish(strings + signer + metadata)
}

/// One source link. Both sides store only ids.
pub(super) fn link_charge() -> usize {
    finish(LINK_BASE)
}

/// One interned source key, from the length of its packed form.
pub(super) fn source_charge(packed_len: usize) -> usize {
    finish(SOURCE_BASE + packed_len)
}

/// One atom allocation: the struct, its strings, metadata, and decoded content.
pub(super) fn atom_charge(atom: &Atom) -> usize {
    // `uuid` and the schema name are only reachable as `&str`; their length
    // stands in for capacity. The safety factor covers the difference.
    let strings = atom.uuid().len()
        + atom.source_schema_name().len()
        + atom.source_file_name().map_or(0, String::capacity);
    let metadata = atom.metadata().map_or(0, metadata_heap_bytes);
    finish(
        ARC_HEADER
            + std::mem::size_of::<Atom>()
            + strings
            + metadata
            + json_heap_bytes(atom.content()),
    )
}

fn metadata_heap_bytes(map: &std::collections::HashMap<String, String>) -> usize {
    map.capacity() * HASH_MAP_SLOT
        + map
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>()
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
