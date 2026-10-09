//! Storage-key parsing, scoped molecule ids and molecule header writes.

use super::*;

#[derive(Clone)]
pub(super) struct TipCoords {
    pub(super) molecule: MoleculeId,
    pub(super) scope: Option<String>,
    pub(super) hash: String,
    pub(super) range: String,
}

#[derive(Clone)]
pub(super) struct HashCoords {
    pub(super) molecule: MoleculeId,
    pub(super) storage_molecule: MoleculeId,
    pub(super) scope: Option<String>,
    pub(super) hash: String,
}

pub(super) fn parse_tip_coords(storage_key: &str) -> Option<TipCoords> {
    let (scope, bare, storage_molecule) = split_molecule_storage_key(storage_key).ok()??;
    let (hash, range) = molecule_key_codec::decode_hash_range_any(bare)?;
    Some(TipCoords {
        molecule: scoped_molecule(storage_molecule, scope),
        scope: scope.map(str::to_string),
        hash,
        range,
    })
}

pub(super) fn parse_hash_scan_prefix(prefix: &str) -> Option<HashCoords> {
    if !prefix.contains('\0') {
        return None;
    }
    let (scope, bare, storage_molecule) = split_molecule_storage_key(prefix).ok()??;
    let spelling = storage_molecule.storage_spelling();
    let record_prefix = molecule_key_codec::molecule_record_prefix(&spelling);
    let suffix = bare.strip_prefix(&record_prefix)?;
    let (hash, _) = molecule_key_codec::decode_hash_range_suffix(suffix)?;
    Some(HashCoords {
        molecule: scoped_molecule(storage_molecule, scope),
        storage_molecule,
        scope: scope.map(str::to_string),
        hash,
    })
}

type ScopedMoleculeKey<'a> = (Option<&'a str>, &'a str, MoleculeId);

pub(super) fn split_molecule_storage_key(
    storage_key: &str,
) -> Result<Option<ScopedMoleculeKey<'_>>, ()> {
    let mut found = None;
    for (start, _) in storage_key.match_indices(MK_PREFIX) {
        let (prefix, bare) = storage_key.split_at(start);
        let Some(scope) = scope_from_prefix(prefix) else {
            continue;
        };
        let Some(spelling) = molecule_key_codec::molecule_uuid_from_storage_key(bare) else {
            continue;
        };
        let Some(bytes) = parse_molecule_uuid_bytes(spelling) else {
            continue;
        };
        if molecule_key_codec::decode_hash_range_any(bare).is_none() {
            continue;
        }
        // A scope or hash can contain `mk:`. Never choose an ambiguous
        // boundary and let one scope claim another scope's memory key.
        if found.is_some() {
            return Err(());
        }
        found = Some((scope, bare, MoleculeId::from_bytes(bytes)));
    }
    Ok(found)
}

pub(super) fn scope_from_prefix(prefix: &str) -> Option<Option<&str>> {
    if prefix.is_empty() {
        Some(None)
    } else {
        Some(Some(prefix.strip_suffix(':')?))
    }
}

pub(in crate::storage::laststore) fn scoped_molecule(
    molecule: MoleculeId,
    scope: Option<&str>,
) -> MoleculeId {
    let Some(scope) = scope else {
        return molecule;
    };
    let mut digest = Sha256::new();
    digest.update(b"lastdb:resident-scope:v1\0");
    digest.update((scope.len() as u64).to_le_bytes());
    digest.update(scope.as_bytes());
    digest.update(molecule.as_bytes());
    MoleculeId::from_bytes(digest.finalize().into())
}

pub(super) fn scoped_storage_key(scope: Option<&str>, bare: &str) -> String {
    match scope {
        Some(scope) => format!("{scope}:{bare}"),
        None => bare.to_string(),
    }
}

pub(super) fn parse_atom_id(storage_key: &str) -> Option<AtomId> {
    split_atom_id(storage_key).ok().flatten()
}

pub(super) fn split_atom_id(storage_key: &str) -> Result<Option<AtomId>, ()> {
    let mut found = None;
    for kind in ["atom:", "atom\0"] {
        for (start, _) in storage_key.match_indices(kind) {
            let (prefix, bare) = storage_key.split_at(start);
            let Some(scope) = scope_from_prefix(prefix) else {
                continue;
            };
            let Some(uuid) = atom_key_codec::uuid_of(bare).filter(|uuid| !uuid.is_empty()) else {
                continue;
            };
            // The exact storage key still works if its scope contains a
            // second atom marker. Do not admit an ambiguous warm identity.
            if found.is_some() {
                return Err(());
            }
            found = Some(AtomId::new(uuid).with_scope(scope));
        }
    }
    Ok(found)
}

pub(super) fn is_logical_storage_key(storage_key: &str) -> bool {
    parse_tip_coords(storage_key).is_some() || parse_atom_id(storage_key).is_some()
}

/// `mh:{M}` with `M` a molecule uuid (43-char base64url or 64-char hex).
/// A storage scope may sit in front (`{scope}:mh:{M}`). A longer `mh:` key
/// is a different record and does not stay.
pub(super) fn is_molecule_header_key(storage_key: &str) -> bool {
    let Some(at) = storage_key.rfind(MH_PREFIX) else {
        return false;
    };
    if at > 0 && storage_key.as_bytes()[at - 1] != b':' {
        return false;
    }
    parse_molecule_uuid_bytes(&storage_key[at + MH_PREFIX.len()..]).is_some()
}

/// A point read keeps a molecule key, an atom, and a molecule header.
/// A plain id does not stay.
pub(super) fn read_keeps_id(storage_key: &str) -> bool {
    is_logical_storage_key(storage_key) || is_molecule_header_key(storage_key)
}

pub(super) fn write_molecule_header(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    id: &str,
    value: &[u8],
) -> StorageResult<Option<Vec<u8>>> {
    // The append copies the previous body from the pin. Do not `load_point`
    // first. The hash group does not stay. The header body does.
    let ack = store
        .append_for_resident(collection, id, Some(value))
        .map_err(LastStoreKvStore::map_error)?;
    let mut guard = set.lock().expect("poison");
    guard.forget_record(collection, id);
    if admit_raw_body(value) {
        let epoch = guard.record_epoch(id);
        guard.admit_record(collection, id, Some(value.to_vec()), epoch);
    }
    drop(guard);
    publish_loader_measurements(store, set);
    Ok(ack.previous)
}

pub(super) fn delete_molecule_header(
    store: &LastStore,
    set: &Mutex<LogicalResidentSet>,
    collection: &str,
    id: &str,
) -> StorageResult<(bool, Option<Vec<u8>>)> {
    let known = set
        .lock()
        .expect("poison")
        .has_fetched_record(collection, id);
    if known == Some(false) {
        return Ok((false, None));
    }
    let previous = if known == Some(true) {
        set.lock()
            .expect("poison")
            .fetched_record(collection, id)
            .flatten()
    } else {
        let epoch = set.lock().expect("poison").record_epoch(id);
        let previous = store
            .load_point(collection, id)
            .map_err(LastStoreKvStore::map_error)?
            .and_then(|point| point.body);
        if previous.is_none() {
            admit_absent(set, collection, id, epoch);
            return Ok((false, None));
        }
        previous
    };
    if previous.is_none() {
        return Ok((false, None));
    }
    store
        .append_for_resident(collection, id, None)
        .map_err(LastStoreKvStore::map_error)?;
    let mut guard = set.lock().expect("poison");
    guard.forget_record(collection, id);
    let epoch = guard.record_epoch(id);
    guard.admit_record(collection, id, None, epoch);
    drop(guard);
    publish_loader_measurements(store, set);
    Ok((true, previous))
}

pub(super) fn tip_from_body(body: &[u8], scope: Option<&str>) -> Result<Tip, StorageError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|err| StorageError::BackendError(format!("invalid tip body: {err}")))?;
    let entry_value = value.get("entry").cloned().unwrap_or(value);
    let entry: AtomEntry = serde_json::from_value(entry_value)
        .map_err(|err| StorageError::BackendError(format!("invalid tip body: {err}")))?;
    Ok(Tip {
        atom: AtomId::new(entry.atom_uuid).with_scope(scope),
        written_at: entry.written_at,
        logical_counter: entry.logical_counter,
        device_id: entry.device_id,
        mutation_uuid: entry.mutation_uuid,
    })
}
