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

pub(super) fn put(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> StorageResult<()> {
    let _ = write_put(store, set, collection, key, value)?;
    Ok(())
}

fn write_put(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> StorageResult<Option<Vec<u8>>> {
    let id = LastStoreKvStore::encode_key(key);
    if split_molecule_storage_key(&id).is_err() {
        return Err(StorageError::InvalidOperation(
            "ambiguous scoped molecule key".to_string(),
        ));
    }
    if split_atom_id(&id).is_err() {
        return Err(StorageError::InvalidOperation(
            "ambiguous scoped atom key".to_string(),
        ));
    }
    if is_molecule_header_key(&id) {
        return write_molecule_header(store, set, collection, &id, value);
    }
    if !is_logical_storage_key(&id) {
        // A plain id is not a molecule key, a molecule header, or an atom.
        // The append copies the previous body under the same write stripe.
        // A separate read would parse and drop an unpinned group before the
        // append opens it again. No write token is recorded, so the id does not stay.
        let ack = store
            .append_for_resident(collection, &id, Some(value))
            .map_err(LastStoreKvStore::map_error);
        set.lock().expect("poison").forget_record(collection, &id);
        let ack = ack?;
        return Ok(ack.previous);
    }
    let ack = store
        .append_for_resident(collection, &id, Some(value))
        .map_err(LastStoreKvStore::map_error)?;
    let previous = ack.previous;
    let token = DurabilityToken::new(ack.token.as_u64());
    let mut guard = set.lock().expect("poison");
    guard.forget_record(collection, &id);
    let digest = body_digest(value);
    let newest = guard.record_write_token(key.to_vec(), token, digest);
    if newest && !superseded_tip(&guard, &id, token) {
        // The put supersedes a delete overlay even when the stored bytes are
        // ciphertext and no body is admitted here. A later read then loads
        // the stored body instead of returning Absent.
        if let Some(coords) = parse_tip_coords(&id) {
            guard.supersede_tombstone(coords.molecule, &coords.hash, &coords.range);
        }
        if admit_raw_body(value) {
            store_warm_body(&mut guard, &id, value, Some(token));
            // The body is resident, so no admit follows. Drop the entry; the
            // resident tip token stops an older put that records late.
            guard.take_write_token_for(key, digest);
        }
    }
    drop(guard);
    publish_loader_measurements(store, set);
    Ok(previous)
}

/// True when the set holds a tip or delete overlay newer than `token`.
/// An append and the set lock are separate steps, so an older write can
/// take the lock after a newer write or delete for the same tip.
fn superseded_tip(guard: &LogicalResidentSet, id: &str, token: DurabilityToken) -> bool {
    let Some(coords) = parse_tip_coords(id) else {
        return false;
    };
    let newer = |other: Option<DurabilityToken>| other.is_some_and(|other| other > token);
    newer(guard.tip_token(coords.molecule, &coords.hash, &coords.range))
        || newer(guard.tombstone_token(coords.molecule, &coords.hash, &coords.range))
}

pub(super) fn delete(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
) -> StorageResult<bool> {
    Ok(write_delete(store, set, collection, key)?.0)
}

fn write_delete(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
) -> StorageResult<(bool, Option<Vec<u8>>)> {
    let id = LastStoreKvStore::encode_key(key);
    if split_molecule_storage_key(&id).is_err() {
        return Err(StorageError::InvalidOperation(
            "ambiguous scoped molecule key".to_string(),
        ));
    }
    if split_atom_id(&id).is_err() {
        return Err(StorageError::InvalidOperation(
            "ambiguous scoped atom key".to_string(),
        ));
    }
    if is_molecule_header_key(&id) {
        return delete_molecule_header(store, set, collection, &id);
    }
    if !is_logical_storage_key(&id) {
        // Same unpublished path as a plain put. The id does not stay, and
        // the hash group count is 0 after the delete. An absent id matches
        // LastStore::delete: no tombstone and no group directory.
        let previous = store
            .load_point(collection, &id)
            .map_err(LastStoreKvStore::map_error)?
            .and_then(|point| point.body);
        if previous.is_none() {
            return Ok((false, None));
        }
        let write = store
            .append_for_resident(collection, &id, None)
            .map_err(LastStoreKvStore::map_error);
        set.lock().expect("poison").forget_record(collection, &id);
        write?;
        return Ok((previous.is_some(), previous));
    }
    let ack = store
        .append_for_resident(collection, &id, None)
        .map_err(LastStoreKvStore::map_error)?;
    let previous = ack.previous;
    let token = DurabilityToken::new(ack.token.as_u64());
    let mut guard = set.lock().expect("poison");
    guard.forget_record(collection, &id);
    guard.clear_write_tokens(key);
    if let Some(coords) = parse_tip_coords(&id) {
        guard.delete_resident(
            coords.molecule,
            coords.hash.clone(),
            coords.range.clone(),
            token,
        );
        guard.release_tombstone(coords.molecule, &coords.hash, &coords.range);
    }
    if let Some(atom) = parse_atom_id(&id) {
        guard.drop_atom_body(&atom);
    }
    drop(guard);
    publish_loader_measurements(store, set);
    Ok((previous.is_some(), previous))
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

pub(crate) fn apply_durable_through(store: &LastStore, set: &Mutex<LogicalResidentSet>) {
    let seal = DurabilityToken::new(store.durable_through().as_u64());
    set.lock().expect("poison").apply_durable_through(seal);
    publish_loader_measurements(store, set);
}

/// Count one point read the set served from memory.
pub(super) fn note_point_hit(guard: &LogicalResidentSet) {
    if let Some(metrics) = guard.metrics() {
        metrics.record_point_hit();
    }
}

/// Run one loader call and record it: `misses` point misses, one load, and
/// its wall-clock cost. The set lock is not held while `load` runs.
pub(super) fn timed_load<T>(
    set: &Mutex<LogicalResidentSet>,
    misses: u64,
    load: impl FnOnce() -> T,
) -> T {
    let metrics = set.lock().expect("poison").metrics().cloned();
    let started = std::time::Instant::now();
    let out = load();
    if let Some(metrics) = metrics {
        for _ in 0..misses {
            metrics.record_point_miss();
        }
        metrics.record_loader_load(started.elapsed());
    }
    out
}

/// Publish pin occupancy. Call after a pin opens, a pin is reaped, and flush.
fn publish_loader_measurements(store: &LastStore, set: &Mutex<LogicalResidentSet>) {
    let metrics = set.lock().expect("poison").metrics().cloned();
    let Some(metrics) = metrics else {
        return;
    };
    metrics.set_loader_measurements(store.loader_pin_bytes(), store.loader_groups_open_now());
}

/// Hit the warm set with plaintext already admitted (encrypting layer).
pub(crate) fn lookup_plaintext(set: &Mutex<LogicalResidentSet>, key: &[u8]) -> ResidentLookup {
    let id = LastStoreKvStore::encode_key(key);
    let mut guard = set.lock().expect("poison");
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return ResidentLookup::Absent;
        }
        if let Some(body) = guard
            .tip_body(coords.molecule, &coords.hash, &coords.range)
            .map(ToOwned::to_owned)
        {
            guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
            guard.release_tip(coords.molecule, &coords.hash, &coords.range);
            note_point_hit(&guard);
            return ResidentLookup::Hit(body);
        }
        return ResidentLookup::Miss;
    }
    if let Some(atom) = parse_atom_id(&id) {
        if let Some(body) = guard.atom_body(&atom).map(ToOwned::to_owned) {
            guard.touch(crate::resident::ResidentKey::Atom(atom));
            note_point_hit(&guard);
            return ResidentLookup::Hit(body);
        }
    }
    ResidentLookup::Miss
}

/// Digest of the bytes one append stored. The encrypting layer passes the
/// digest of its sealed bytes so that only the put that wrote them admits
/// its plaintext.
pub(crate) fn body_digest(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Admit a body that a read decrypted. A read never replaces a resident
/// body: a write admitted that body, and it is at least as new as the read.
pub(crate) fn admit_plaintext(set: &Mutex<LogicalResidentSet>, key: &[u8], value: &[u8]) {
    let id = LastStoreKvStore::encode_key(key);
    let mut guard = set.lock().expect("poison");
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range)
            || guard
                .tip_body(coords.molecule, &coords.hash, &coords.range)
                .is_some()
        {
            return;
        }
    }
    if let Some(atom) = parse_atom_id(&id) {
        if guard.atom_body(&atom).is_some() {
            return;
        }
    }
    store_warm_body(&mut guard, &id, value, None);
}

/// Admit the plaintext of a put whose stored bytes have digest
/// `stored_digest`. Only the newest append for the key admits; an older put
/// that finishes last stores nothing.
pub(crate) fn admit_written_plaintext(
    set: &Mutex<LogicalResidentSet>,
    key: &[u8],
    stored_digest: u64,
    value: &[u8],
) {
    let id = LastStoreKvStore::encode_key(key);
    let mut guard = set.lock().expect("poison");
    let Some(token) = guard.take_write_token_for(key, stored_digest) else {
        return;
    };
    if superseded_tip(&guard, &id, token) {
        return;
    }
    store_warm_body(&mut guard, &id, value, Some(token));
}

enum WarmLookup {
    Miss,
    Absent,
    Hit(Vec<u8>),
}

enum WarmExists {
    Miss,
    No,
    Yes,
}

fn warm_lookup(guard: &mut LogicalResidentSet, collection: &str, key: &[u8]) -> WarmLookup {
    let id = LastStoreKvStore::encode_key(key);
    let known = |guard: &mut LogicalResidentSet| match record_hit(guard, collection, &id) {
        Some(Some(body)) => WarmLookup::Hit(body),
        Some(None) => WarmLookup::Absent,
        None => WarmLookup::Miss,
    };
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return WarmLookup::Absent;
        }
        if let Some(body) = guard
            .tip_body(coords.molecule, &coords.hash, &coords.range)
            .map(ToOwned::to_owned)
        {
            guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
            guard.release_tip(coords.molecule, &coords.hash, &coords.range);
            note_point_hit(guard);
            return WarmLookup::Hit(body);
        }
        return known(guard);
    }
    if let Some(atom) = parse_atom_id(&id) {
        if let Some(body) = guard.atom_body(&atom).map(ToOwned::to_owned) {
            guard.touch(crate::resident::ResidentKey::Atom(atom));
            note_point_hit(guard);
            return WarmLookup::Hit(body);
        }
        return known(guard);
    }
    if is_molecule_header_key(&id) {
        return known(guard);
    }
    if super::absence_hints::is_absence_hint_id(&id) && guard.absent_hint(collection, &id) {
        note_point_hit(guard);
        return WarmLookup::Absent;
    }
    WarmLookup::Miss
}

fn warm_exists(guard: &LogicalResidentSet, collection: &str, key: &[u8]) -> WarmExists {
    let id = LastStoreKvStore::encode_key(key);
    let known = || match guard.has_fetched_record(collection, &id) {
        Some(true) => WarmExists::Yes,
        Some(false) => WarmExists::No,
        None => WarmExists::Miss,
    };
    if let Some(coords) = parse_tip_coords(&id) {
        if guard.has_tombstone(coords.molecule, &coords.hash, &coords.range) {
            return WarmExists::No;
        }
        if guard
            .tip(coords.molecule, &coords.hash, &coords.range)
            .is_some()
        {
            return WarmExists::Yes;
        }
        return known();
    }
    if let Some(atom) = parse_atom_id(&id) {
        if guard.atom_body(&atom).is_some() {
            return WarmExists::Yes;
        }
        return known();
    }
    if is_molecule_header_key(&id) {
        return known();
    }
    WarmExists::Miss
}

fn store_warm_body(
    guard: &mut LogicalResidentSet,
    id: &str,
    value: &[u8],
    token: Option<DurabilityToken>,
) {
    if let Some(coords) = parse_tip_coords(id) {
        if let Ok(tip) = tip_from_body(value, coords.scope.as_deref()) {
            guard.store_tip_body(coords.molecule, &coords.hash, &coords.range, value.to_vec());
            if guard
                .tip(coords.molecule, &coords.hash, &coords.range)
                .is_some()
            {
                guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
            } else {
                guard.admit_tip(
                    coords.molecule,
                    coords.hash.clone(),
                    coords.range.clone(),
                    tip,
                );
            }
            if let Some(token) = token {
                guard.mark_dirty(coords.molecule, &coords.hash, &coords.range, token);
            }
            guard.release_tip(coords.molecule, &coords.hash, &coords.range);
        }
    }
    if let Some(atom) = parse_atom_id(id) {
        guard.admit_atom_body(atom, value.to_vec());
    }
}

fn admit_loaded_body(set: &Mutex<LogicalResidentSet>, id: &str, body: &[u8]) {
    if let Some(coords) = parse_tip_coords(id) {
        admit_loaded_tip(set, &coords, body);
        return;
    }
    if let Some(atom) = parse_atom_id(id) {
        if admit_raw_body(body) {
            let mut guard = set.lock().expect("poison");
            if guard.atom_body(&atom).is_some() {
                return;
            }
            guard.admit_atom_body(atom, body.to_vec());
        }
    }
}

fn hash_range_rows(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    prefix: &[u8],
    limit: usize,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    if let Ok(text) = std::str::from_utf8(prefix) {
        if let Some(coords) = parse_hash_scan_prefix(text) {
            let guard = set.lock().expect("poison");
            if matches!(
                guard.hash_completeness(coords.molecule, &coords.hash),
                HashCompleteness::Complete { .. }
            ) {
                return Ok(complete_rows(&guard, &coords, limit));
            }
        }
    }

    let loaded = match store.load_hash(collection, prefix) {
        Ok(rows) => rows,
        Err(err) => return Err(LastStoreKvStore::map_error(err)),
    };

    let mut guard = set.lock().expect("poison");
    if let Ok(text) = std::str::from_utf8(prefix) {
        if let Some(coords) = parse_hash_scan_prefix(text) {
            return Ok(admit_hash_rows(&mut guard, &coords, loaded, limit));
        }
    }
    let mut rows = Vec::new();
    for row in loaded {
        let Some(body) = row.body else {
            continue;
        };
        let key = LastStoreKvStore::decode_key(&row.storage_key)?;
        rows.push((key, body));
        if rows.len() == limit {
            break;
        }
    }
    Ok(rows)
}

fn complete_rows(
    set: &LogicalResidentSet,
    coords: &HashCoords,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    for range in set.hash_order(coords.molecule, &coords.hash) {
        if set.has_tombstone(coords.molecule, &coords.hash, &range) {
            continue;
        }
        let Some(body) = set.tip_body(coords.molecule, &coords.hash, &range) else {
            continue;
        };
        let storage_key = scoped_storage_key(
            coords.scope.as_deref(),
            &molecule_key_codec::hash_range_record_key(
                &coords.storage_molecule.storage_spelling(),
                &coords.hash,
                &range,
            ),
        );
        rows.push((storage_key.into_bytes(), body.to_vec()));
        if rows.len() == limit {
            break;
        }
    }
    rows
}

fn admit_hash_rows(
    set: &mut LogicalResidentSet,
    coords: &HashCoords,
    loaded: Vec<LoadedTip>,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let spelling = coords.storage_molecule.storage_spelling();
    let record_prefix = scoped_storage_key(
        coords.scope.as_deref(),
        &molecule_key_codec::molecule_record_prefix(&spelling),
    );
    let mut merged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for row in loaded {
        let Some((row_hash, range)) = row
            .storage_key
            .strip_prefix(&record_prefix)
            .and_then(molecule_key_codec::decode_hash_range_suffix)
        else {
            continue;
        };
        if row_hash != coords.hash {
            continue;
        }
        let Some(body) = row.body else {
            continue;
        };
        merged.insert(range, body);
    }
    // Tombstones overlay the loader list. Warm-set tip bodies do not: the
    // encrypting seam stores plaintext in the set and ciphertext on the pin,
    // and inner scan must return the stored form so that seam can open it.
    for range in set.tombstone_ranges_for_hash(coords.molecule, &coords.hash) {
        merged.remove(&range);
    }

    // Keep only the keys this call returns. A full read returns every live
    // key. A short page returns that page, and the other keys stay absent.
    let mut rows = Vec::new();
    let mut returned = Vec::new();
    for (range, body) in &merged {
        if rows.len() == limit {
            break;
        }
        let storage_key = scoped_storage_key(
            coords.scope.as_deref(),
            &molecule_key_codec::hash_range_record_key(&spelling, &coords.hash, range),
        );
        rows.push((storage_key.into_bytes(), body.clone()));
        returned.push(range.clone());
        admit_returned_tip(set, coords, range, body);
    }

    // Complete means a later read can skip the hash group. That is true only
    // when this call returned every live key and the warm body is still the
    // stored body this call loaded. A short page, a purge, or a plaintext
    // overlay on ciphertext must not certify the hash. The next read opens
    // the group and returns the stored bytes.
    let returned_every_live_key = !merged.is_empty() && returned.len() == merged.len();
    let every_returned_key_stays = returned.iter().all(|range| {
        set.tip(coords.molecule, &coords.hash, range).is_some()
            && set
                .tip_body(coords.molecule, &coords.hash, range)
                .is_some_and(|warm| warm == merged[range].as_slice())
    });
    if returned_every_live_key && every_returned_key_stays {
        set.mark_hash_complete(coords.molecule, &coords.hash, returned);
    }
    rows
}

/// Copy one returned key into the warm set. Ciphertext and a body that is
/// not a tip stay out. The caller still receives those stored bytes.
fn admit_returned_tip(set: &mut LogicalResidentSet, coords: &HashCoords, range: &str, body: &[u8]) {
    if !admit_raw_body(body) {
        return;
    }
    let Ok(tip) = tip_from_body(body, coords.scope.as_deref()) else {
        return;
    };
    set.store_tip_body(coords.molecule, &coords.hash, range, body.to_vec());
    if set.tip(coords.molecule, &coords.hash, range).is_some() {
        set.hold_tip(coords.molecule, &coords.hash, range);
    } else {
        set.admit_tip(coords.molecule, coords.hash.clone(), range.to_string(), tip);
    }
    set.release_tip(coords.molecule, &coords.hash, range);
}

fn admit_loaded_tip(set: &Mutex<LogicalResidentSet>, coords: &TipCoords, body: &[u8]) {
    if !admit_raw_body(body) {
        return;
    }
    let mut guard = set.lock().expect("poison");
    if guard
        .tip_body(coords.molecule, &coords.hash, &coords.range)
        .is_some()
    {
        return;
    }
    if let Ok(tip) = tip_from_body(body, coords.scope.as_deref()) {
        guard.store_tip_body(coords.molecule, &coords.hash, &coords.range, body.to_vec());
        if guard
            .tip(coords.molecule, &coords.hash, &coords.range)
            .is_some()
        {
            guard.hold_tip(coords.molecule, &coords.hash, &coords.range);
        } else {
            guard.admit_tip(
                coords.molecule,
                coords.hash.clone(),
                coords.range.clone(),
                tip,
            );
        }
        guard.release_tip(coords.molecule, &coords.hash, &coords.range);
    }
}

fn admit_raw_body(body: &[u8]) -> bool {
    !is_sealed_at_rest(body)
}

mod batch;
mod coords;

pub(super) use batch::*;
pub(super) use coords::*;
