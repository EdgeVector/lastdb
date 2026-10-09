//! Plain ids whose ABSENCE the resident set may remember.
//!
//! A plain read opens a hash group, returns the bytes, and keeps nothing ("the id
//! does not stay"). That keeps the warm set small, but an id that is read on every
//! query and is almost always absent costs one hash group open per read, forever.
//! Measured 2026-10-08 (brain `design-lastdb-batch-reads-and-composable-queries-20261007`):
//! with a key's data already in memory, a repeated query still made 2 (Hash schema)
//! to 5 (HashRange schema) loader loads, and four of the five were reads of ids that
//! did not exist.
//!
//! The ids named here may have their absence kept in the bounded hint cache of the
//! set (`resident/logical_set/negative_barrier_cache.rs`). A hint is not a logical
//! record: it holds no bytes, does not count against the 10,000-record budget, and
//! is capped. A PRESENT id is read from disk on every read, as before.
//!
//! A hint stays valid for the same reasons an absent Delete marker hint does:
//! - a put, delete, compare-and-swap, or batch write of the id calls
//!   `forget_record(collection, id)`, which drops the hint and bumps the id's epoch;
//! - the epoch taken before a load refuses an admit that raced a write;
//! - a direct write or a restore calls `forget_all_records`, which clears every hint.

use std::sync::Mutex;

use super::logical_path::{note_point_hit, timed_load};
use super::{is_draft_v2_delete_barrier, strip_org_storage_prefix, LastStoreKvStore};
use crate::resident::LogicalResidentSet;
use crate::storage::error::StorageResult;
use laststore::LastStore;

/// The home conflict index (`db_operations::conflict_operations::HOME_CONFLICT_INDEX_KEY`).
/// One read per query for the conflict annotation; absent until a conflict is recorded.
/// The `hcu:evt\0{seq}` event keys are NOT hinted: they are written on every conflict.
const HOME_CONFLICT_INDEX_KEY: &str = "hcu:mols";

/// The molecule generation pointer (`molecule_key_codec::MOLECULE_GENERATION_POINTER_PREFIX`).
/// One read per HashRange query; absent for a molecule that was never re-generated.
const GENERATION_POINTER_PREFIX: &str = "mgp:v1:";

/// True when the absence of `id` may be remembered. `id` is a storage key, with an
/// optional 64-hex org scope in front.
pub(super) fn is_absence_hint_id(id: &str) -> bool {
    let bare = strip_org_storage_prefix(id);
    is_draft_v2_delete_barrier(bare)
        || bare == HOME_CONFLICT_INDEX_KEY
        || bare.starts_with(GENERATION_POINTER_PREFIX)
}

/// Point read of a plain id: from the hint when its absence is known, else from
/// disk. The bytes never stay; only an absence may, and only for a hinted id.
pub(super) fn get_plain(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    id: &str,
) -> StorageResult<Option<Vec<u8>>> {
    let hinted = is_absence_hint_id(id);
    let epoch = {
        let mut guard = set.lock().expect("poison");
        if hinted && guard.absent_hint(collection, id) {
            note_point_hit(&guard);
            return Ok(None);
        }
        guard.record_epoch(id)
    };
    let loaded = timed_load(set, 1, || store.load_existing_point(collection, id))
        .map_err(LastStoreKvStore::map_error)?;
    let body = loaded.and_then(|point| point.body);
    if hinted && body.is_none() {
        set.lock()
            .expect("poison")
            .admit_absent_hint(collection, id, epoch);
    }
    Ok(body)
}
