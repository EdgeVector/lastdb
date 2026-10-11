//! Product point, range, and write through [`LogicalResidentSet`] and the loader.
//!
//! A read opens an unpublished hash group and drops it after the call.
//! A plain write copies the previous body and appends on the same pin.
//! A fetched molecule key, atom, or header stays warm; a plain id does not.
//! An absent v2 Delete marker, or an absent id named in `absence_hints`, can stay in a bounded cache.
//! There is no `LASTDB_LOGICAL_RESIDENT_SET` read. The cap is read once at boot
//! (`LASTDB_RESIDENT_KEY_CAP` can only lower it, for a test node).

use super::LastStoreKvStore;
use crate::atom::atom_key_codec;
use crate::atom::molecule_key_codec::{self, MH_PREFIX, MK_PREFIX};
use crate::atom::molecule_uuid::parse_molecule_uuid_bytes;
use crate::atom::AtomEntry;
use crate::crypto::is_sealed_at_rest;
use crate::resident::{
    AtomId, DurabilityToken, HashCompleteness, LogicalResidentSet, MoleculeId, Tip,
};
use crate::storage::error::{StorageError, StorageResult};
use laststore::{LastStore, LoadedTip, ShardKey};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Warm-set lookup for the encrypting layer. Tombstone is not a miss.
#[derive(Debug)]
pub(crate) enum ResidentLookup {
    Miss,
    Absent,
    Hit(Vec<u8>),
}

/// True when `prefix` is a molecule hash-range scan (`mk:{M}:{esc(hash)}\0…`).
///
/// A NUL in a catalog twin (`mord\0…`) is not a hash-range. FullKey product
/// range of a real molecule prefix still calls `load_hash` and errors.
pub(super) fn is_hash_range_scan_prefix(prefix: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(prefix) else {
        return false;
    };
    parse_hash_scan_prefix(text).is_some()
}

pub(super) fn get(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
) -> StorageResult<Option<Vec<u8>>> {
    let id = LastStoreKvStore::encode_key(key);
    if let Some(coords) = parse_tip_coords(&id) {
        let epoch = {
            let mut guard = set.lock().expect("poison");
            if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
                return Ok(None);
            }
            if let Some(body) = guard
                .tip_body(coords.molecule, &coords.hash, &coords.range)
                .map(ToOwned::to_owned)
            {
                guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
                guard.release_tip(coords.molecule, &coords.hash, &coords.range);
                note_point_hit(&guard);
                return Ok(Some(body));
            }
            if let Some(known) = record_hit(&mut guard, collection, &id) {
                return Ok(known);
            }
            guard.record_epoch(&id)
        };
        let loaded = timed_load(set, 1, || store.load_point(collection, &id))
            .map_err(LastStoreKvStore::map_error)?;
        let Some(body) = loaded.and_then(|point| point.body) else {
            admit_absent(set, collection, &id, epoch);
            return Ok(None);
        };
        admit_loaded_tip(set, &coords, &body);
        return Ok(Some(body));
    }

    if let Some(atom) = parse_atom_id(&id) {
        let epoch = {
            let mut guard = set.lock().expect("poison");
            if let Some(body) = guard.atom_body(&atom).map(ToOwned::to_owned) {
                guard.touch(crate::resident::ResidentKey::Atom(atom));
                note_point_hit(&guard);
                return Ok(Some(body));
            }
            if let Some(known) = record_hit(&mut guard, collection, &id) {
                return Ok(known);
            }
            guard.record_epoch(&id)
        };
        let loaded = timed_load(set, 1, || store.load_point(collection, &id))
            .map_err(LastStoreKvStore::map_error)?;
        let Some(body) = loaded.and_then(|point| point.body) else {
            admit_absent(set, collection, &id, epoch);
            return Ok(None);
        };
        if admit_raw_body(&body) {
            set.lock()
                .expect("poison")
                .admit_atom_body(atom, body.clone());
        }
        return Ok(Some(body));
    }

    if is_molecule_header_key(&id) {
        let epoch = {
            let mut guard = set.lock().expect("poison");
            if let Some(known) = record_hit(&mut guard, collection, &id) {
                return Ok(known);
            }
            guard.record_epoch(&id)
        };
        let loaded = timed_load(set, 1, || store.load_point(collection, &id))
            .map_err(LastStoreKvStore::map_error)?;
        let Some(body) = loaded.and_then(|point| point.body) else {
            admit_absent(set, collection, &id, epoch);
            return Ok(None);
        };
        if admit_raw_body(&body) {
            set.lock()
                .expect("poison")
                .admit_record(collection, &id, Some(body.clone()), epoch);
        }
        return Ok(Some(body));
    }

    // A plain id is not a schema, a field, a molecule key, a molecule header,
    // or an atom. The bytes come back. The id does not stay (an absent one may:
    // `absence_hints`). An absent plain id must not create an empty group.
    super::absence_hints::get_plain(store, set, collection, &id)
}

/// Serve a fetched record (present bytes or a known absence) from the set.
fn record_hit(
    guard: &mut LogicalResidentSet,
    collection: &str,
    id: &str,
) -> Option<Option<Vec<u8>>> {
    let known = guard.fetched_record(collection, id)?;
    note_point_hit(guard);
    Some(known)
}

/// Keep an absent molecule key, atom, or molecule header, so the next read of
/// that id does not open the group again. A plain id does not use this. A
/// write of the header replaces this entry with the new body.
fn admit_absent(set: &Mutex<LogicalResidentSet>, collection: &str, id: &str, epoch: u64) {
    set.lock()
        .expect("poison")
        .admit_record(collection, id, None, epoch);
}

pub(super) fn get_many(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    keys: &[Vec<u8>],
) -> StorageResult<Vec<Option<Vec<u8>>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = vec![None; keys.len()];
    let mut miss = Vec::new();
    let mut epochs = Vec::new();
    {
        let mut guard = set.lock().expect("poison");
        for (slot, key) in keys.iter().enumerate() {
            match warm_lookup(&mut guard, collection, key) {
                WarmLookup::Hit(body) => out[slot] = Some(body),
                WarmLookup::Absent => {}
                WarmLookup::Miss => {
                    epochs.push(guard.record_epoch(&LastStoreKvStore::encode_key(key)));
                    miss.push(slot);
                }
            }
        }
    }
    if miss.is_empty() {
        return Ok(out);
    }
    let ids: Vec<String> = miss
        .iter()
        .map(|&slot| LastStoreKvStore::encode_key(&keys[slot]))
        .collect();
    let loaded = timed_load(set, ids.len() as u64, || {
        if ids.iter().all(|id| !read_keeps_id(id)) {
            store.load_existing_points(collection, &ids)
        } else {
            store.load_points(collection, &ids)
        }
    })
    .map_err(LastStoreKvStore::map_error)?;
    for ((slot, point), epoch) in miss.into_iter().zip(loaded).zip(epochs) {
        let id = LastStoreKvStore::encode_key(&keys[slot]);
        let Some(body) = point.and_then(|point| point.body) else {
            if read_keeps_id(&id) {
                admit_absent(set, collection, &id, epoch);
            } else if super::absence_hints::is_absence_hint_id(&id) {
                set.lock()
                    .expect("poison")
                    .admit_absent_hint(collection, &id, epoch);
            }
            continue;
        };
        if is_molecule_header_key(&id) {
            if admit_raw_body(&body) {
                set.lock().expect("poison").admit_record(
                    collection,
                    &id,
                    Some(body.clone()),
                    epoch,
                );
            }
        } else if is_logical_storage_key(&id) {
            admit_loaded_body(set, &id, &body);
        }
        out[slot] = Some(body);
    }
    Ok(out)
}

pub(super) fn exists(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
) -> StorageResult<bool> {
    let id = LastStoreKvStore::encode_key(key);
    if let Some(coords) = parse_tip_coords(&id) {
        let guard = set.lock().expect("poison");
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return Ok(false);
        }
        if guard
            .tip(coords.molecule, &coords.hash, &coords.range)
            .is_some()
        {
            return Ok(true);
        }
    }
    if let Some(atom) = parse_atom_id(&id) {
        let guard = set.lock().expect("poison");
        if guard.atom_body(&atom).is_some() {
            return Ok(true);
        }
    }
    if read_keeps_id(&id) {
        if let Some(present) = set
            .lock()
            .expect("poison")
            .has_fetched_record(collection, &id)
        {
            return Ok(present);
        }
    }
    let loaded = store
        .load_point(collection, &id)
        .map_err(LastStoreKvStore::map_error)?;
    Ok(loaded.and_then(|point| point.body).is_some())
}

pub(super) fn exists_many(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    keys: &[Vec<u8>],
) -> StorageResult<Vec<bool>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = vec![false; keys.len()];
    let mut miss = Vec::new();
    {
        let guard = set.lock().expect("poison");
        for (slot, key) in keys.iter().enumerate() {
            match warm_exists(&guard, collection, key) {
                WarmExists::Yes => out[slot] = true,
                WarmExists::No => {}
                WarmExists::Miss => miss.push(slot),
            }
        }
    }
    if miss.is_empty() {
        return Ok(out);
    }
    let ids: Vec<String> = miss
        .iter()
        .map(|&slot| LastStoreKvStore::encode_key(&keys[slot]))
        .collect();
    let loaded = store
        .exists_points(collection, &ids)
        .map_err(LastStoreKvStore::map_error)?;
    for (slot, present) in miss.into_iter().zip(loaded) {
        out[slot] = present;
    }
    Ok(out)
}

pub(super) fn scan_prefix(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    prefix: &[u8],
    limit: usize,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    if !is_hash_range_scan_prefix(prefix) {
        return LastStoreKvStore::prefix_rows_sync(store, collection, prefix, limit);
    }
    hash_range_rows(store, set, collection, prefix, limit)
}

pub(super) fn scan_prefix_keys(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    prefix: &[u8],
    limit: usize,
) -> StorageResult<Vec<Vec<u8>>> {
    if !is_hash_range_scan_prefix(prefix) {
        return LastStoreKvStore::prefix_keys_sync(store, collection, prefix, limit);
    }
    Ok(hash_range_rows(store, set, collection, prefix, limit)?
        .into_iter()
        .map(|(key, _)| key)
        .collect())
}

mod batch;
mod coords;
mod hash_rows;
mod resident_io;
mod warm;
mod write;

pub(super) use batch::*;
pub(super) use coords::*;
pub(super) use hash_rows::*;
pub(crate) use resident_io::*;
pub(super) use warm::*;
pub(super) use write::*;
