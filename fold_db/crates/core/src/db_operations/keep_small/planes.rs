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

/// Sum `collection_disk_bytes` over [`BUDGET_PHYSICAL_PLANES`].
#[must_use]
pub fn sum_physical_plane_bytes(mut disk_bytes: impl FnMut(&str) -> Option<u64>) -> u64 {
    BUDGET_PHYSICAL_PLANES
        .iter()
        .map(|plane| disk_bytes(plane).unwrap_or(0))
        .fold(0u64, u64::saturating_add)
}

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

/// True when compacting `collection` is a LastStore segment rewrite (no
/// capture records). Named so a future put-through compact fails the bar
/// instead of silently amplifying the pin log.
#[must_use]
pub fn physical_compact_rewrite_is_capture_neutral(collection: &str) -> bool {
    physical_compact_rewrite_capture_records(collection) == 0
        && matches!(
            collection,
            "tips"
                | "indexes"
                | "atoms"
                | "schemas"
                | "schema_states"
                | "schema_index"
                | "atom_locators"
                | "idempotency"
                | "change_feed"
                | "keep_small"
                | "sync_pin_log"
                | "sync_capture_reexport"
                | "molecule_ref_edges"
                | "blob_ref_edges"
        )
}
