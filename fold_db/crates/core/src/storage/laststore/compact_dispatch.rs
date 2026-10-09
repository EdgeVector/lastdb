//! How the owner compact verb treats the planes that hold the only copy of
//! their rows.
//!
//! `cas_blobs` is the first such plane. A blob row has no second copy until a
//! cloud copy exists, so the verb must not rewrite more of it than it has to,
//! and must not reach it by accident.

use super::{LastStoreKvStore, StorageResult};
use laststore::LastStore;

/// Planes that a bare `compact` and `compact --all` skip. An operator names
/// them with `--collection`.
///
/// `cas_blobs`: a first run reads the whole plane (568.8 MiB on the primary at
/// audit) and rewrites every group that holds a deleted blob. That is too much
/// work to start from an empty request body, and the manual path has no
/// host-pressure gate.
pub const COMPACT_NAMED_ONLY: &[&str] = &["cas_blobs"];

/// Planes whose owner compact rewrites only the hash groups that hold dead
/// bytes. A clean group stays byte-identical, so it never enters the crash
/// window of a segment swap.
const COMPACT_DEAD_GROUPS_ONLY: &[&str] = &["cas_blobs"];

/// Compact one collection in the store, by the plane's rule.
pub(super) fn compact_collection_in_store(
    store: &LastStore,
    collection: &str,
) -> StorageResult<()> {
    if COMPACT_DEAD_GROUPS_ONLY.contains(&collection) {
        store
            .compact_collection_dead_groups(collection)
            .map(|_rewritten| ())
            .map_err(LastStoreKvStore::map_error)
    } else {
        store
            .compact_collection(collection)
            .map_err(LastStoreKvStore::map_error)
    }
}
