//! Heap-size estimates for deferred batches.

use super::*;

/// Estimated in-memory footprint of a JSON value, without allocating.
///
/// Deliberately not `Value::to_string().len()`: that is the *serialized*
/// length, it under-reports the in-memory shape (every `String` carries a
/// header and capacity; every map entry carries node overhead), and it
/// allocates a full copy of the content on the write path to measure it.
///
/// `depth_budget` bounds recursion so hostile nesting cannot blow the stack;
/// past it the subtree is charged its node cost only.
pub(super) fn json_heap_bytes(value: &serde_json::Value, depth_budget: u32) -> u64 {
    /// Per-node charge for the `Value` enum plus allocator overhead.
    const NODE: u64 = 32;
    if depth_budget == 0 {
        return NODE;
    }
    NODE + match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(s) => s.len() as u64,
        serde_json::Value::Array(items) => items
            .iter()
            .map(|v| json_heap_bytes(v, depth_budget - 1))
            .sum(),
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(k, v)| NODE + k.len() as u64 + json_heap_bytes(v, depth_budget - 1))
            .sum(),
    }
}

/// Recursion budget for [`json_heap_bytes`]. Comfortably above any real atom
/// shape and below any stack that matters.
pub const JSON_DEPTH_BUDGET: u32 = 32;

/// Byte term the logical resident set adds to [`ProcessMemoryBudget::compute`].
///
/// Always 0. The set is capped by [`crate::resident::RESIDENT_KEY_CAP`] used
/// records, not by a byte ledger. Do not fold this into `warm_bytes`,
/// `key_cache_bytes`, `resident_graph_bytes`, or a new `resident_bytes` field.
#[must_use]
pub const fn logical_resident_set_charged_bytes() -> u64 {
    0
}

/// Bytes to charge for deferring one write batch: every atom body it holds,
/// plus [`PER_DEFERRED_TASK_BYTES`] for the task state that is not measured.
///
/// Takes an iterator of borrowed atoms so measuring a batch never copies one —
/// measuring memory by allocating a second copy of it would be its own bug.
#[must_use]
pub fn estimate_deferred_batch_bytes<'a, I>(atoms: I) -> u64
where
    I: IntoIterator<Item = &'a crate::atom::Atom>,
{
    atoms
        .into_iter()
        .map(|atom| json_heap_bytes(atom.content(), JSON_DEPTH_BUDGET))
        .fold(PER_DEFERRED_TASK_BYTES, u64::saturating_add)
}
