use super::*;

pub(super) fn hash_range_rows(
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

pub(super) fn complete_rows(
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

pub(super) fn admit_hash_rows(
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
