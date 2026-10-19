//! Byte charges for the warm set gauge (design: docs/lastdb-warm-set-byte-limit-design.md).
//!
//! A charge is a calculated estimate of retained heap bytes, rounded up. The
//! variable parts (strings, vectors, maps, the decoded JSON tree) are measured
//! from capacities; only the per-object constants are estimates. Stage 1 only
//! reports these charges. The record count still controls admission.

use super::{CorePath, SlotKey};
use crate::atom::Atom;
use serde_json::Value;

/// One path: its `paths` entry and `PathState`, the shared `Arc<CorePath>`
/// header and struct, its recency entry, and its pointer in the slot owner
/// list. Calibrated on 2026-10-19 from a heap profile of the interned tables
/// (about 0.64 KB per path with its strings).
const PATH_BASE: usize = 368;
/// One `BTreeMap` entry of a JSON object: inline key and value plus node slack (guess).
const JSON_MAP_ENTRY: usize = 96;
/// One source link: one id in the slot's list and one in the source's set.
/// Both lists are usually inline, so this covers spills and table growth.
const LINK_BASE: usize = 8;
/// One interned source key: its index entry, its table entry, and the header
/// of its single packed allocation. Calibrated on 2026-10-19 (heap profile:
/// about 350 bytes per source with a packed key of about 190 bytes).
const SOURCE_BASE: usize = 96;
/// One slot: its table and index entries, the molecule tip and key-metadata
/// map entries, and the copy of the tip entry in its owned value. Calibrated
/// on 2026-10-19 from a heap profile of the interned tables (about 1.87 KB
/// per slot).
const SLOT_BASE: usize = 1656;
/// Copies of the slot hash and range strings: the interned key and the
/// molecule tip map.
const SLOT_KEY_COPIES: usize = 2;
/// One `HashMap` slot of atom metadata: key, value, and control byte (guess).
const HASH_MAP_SLOT: usize = 56;
/// The `Arc` strong and weak counters in front of the atom.
const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();
/// Allocator size classes and tree slack, as a ratio of 100 (guess).
const SAFETY_PERCENT: usize = 110;
const ROUND: usize = 16;

const VALUE_SIZE: usize = std::mem::size_of::<Value>();

/// One path: the base cost plus its key strings, held once in the shared
/// `Arc<CorePath>`.
pub(super) fn path_charge(path: &CorePath) -> usize {
    let strings = path.scope.as_ref().map_or(0, String::capacity)
        + path.schema.capacity()
        + path.field.capacity()
        + path.molecule.capacity()
        + path.hash.capacity()
        + path.range.capacity();
    finish(PATH_BASE + strings)
}

/// One slot: the per-slot entries and the copies of its hash and range.
/// Molecule names are shared by every slot of the molecule and not charged.
pub(super) fn slot_charge(slot: &SlotKey) -> usize {
    finish(SLOT_BASE + SLOT_KEY_COPIES * (slot.hash.len() + slot.range.len()))
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
