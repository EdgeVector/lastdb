//! Which physical planes count toward the budget, and capture-neutral compaction.

/// Planes whose physical bytes count toward the budget (file blobs exempt).
pub const BUDGET_PHYSICAL_PLANES: &[&str] = &[
    "atoms",
    "tips",
    "indexes",
    "field_update_order_log",
    "atom_locators",
    "proteins",
    "schema_index",
];

/// Capture-neutral contract: a LastStore segment compact rewrites live keys
/// in the collection directory. It does not go through `KvStore::put`, so the
/// mutation-log capture wrapper cannot emit pin-log records for the rewrite.
///
/// Returning 0 is the whole safety argument for self-compacting captured
/// planes (tips) without reopening the 2026-08-08 amplifier.
#[must_use]
pub fn physical_compact_rewrite_capture_records(_collection: &str) -> u64 {
    0
}
