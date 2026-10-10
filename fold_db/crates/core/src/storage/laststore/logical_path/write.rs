//! Plain put and delete through the logical path.

use super::*;

pub(in crate::storage::laststore) fn put(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> StorageResult<()> {
    let _ = write_put(store, set, collection, key, value)?;
    Ok(())
}

pub(super) fn write_put(
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
pub(super) fn superseded_tip(guard: &LogicalResidentSet, id: &str, token: DurabilityToken) -> bool {
    let Some(coords) = parse_tip_coords(id) else {
        return false;
    };
    let newer = |other: Option<DurabilityToken>| other.is_some_and(|other| other > token);
    newer(guard.tip_token(coords.molecule, &coords.hash, &coords.range))
        || newer(guard.tombstone_token(coords.molecule, &coords.hash, &coords.range))
}

pub(in crate::storage::laststore) fn delete(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    key: &[u8],
) -> StorageResult<bool> {
    Ok(write_delete(store, set, collection, key)?.0)
}

pub(super) fn write_delete(
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
