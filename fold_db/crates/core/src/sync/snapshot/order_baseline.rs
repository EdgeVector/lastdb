//! HashRange order-baseline normalization and validation for snapshot photographs.

use super::*;

/// Put the complete HashRange order baseline in its canonical photograph
/// planes. Older stores can still hold `mord:` / `moc:` rows in `tips`; a
/// photograph must not preserve that physical accident because a new Mini
/// routes those keys to the dedicated order collections.
pub(super) fn prepare_logical_photograph(
    namespaces: Vec<NamespaceData>,
    normalize_source_residue: bool,
) -> SyncResult<Vec<NamespaceData>> {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

    let mut normalized: BTreeMap<String, BTreeMap<String, (String, bool)>> = BTreeMap::new();
    let mut source_namespaces = HashSet::new();
    for namespace in namespaces {
        if !source_namespaces.insert(namespace.name.clone()) {
            return Err(SyncError::Serialization(format!(
                "snapshot contains duplicate namespace '{}'",
                namespace.name
            )));
        }
        // Keep empty source planes. Restore uses their presence to clear stale
        // rows even when every row moved to a canonical destination plane.
        normalized.entry(namespace.name.clone()).or_default();
        for entry in namespace.entries {
            let key = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            let target = photograph_namespace_for_key(&namespace.name, &key);
            let source_is_canonical = namespace.name == target;
            let rows = normalized.entry(target.to_string()).or_default();
            match rows.get(&entry.key) {
                Some((_, true)) if !source_is_canonical => {}
                Some((_, false)) if source_is_canonical => {
                    rows.insert(entry.key, (entry.value, true));
                }
                Some((value, _)) if value != &entry.value => {
                    return Err(SyncError::Serialization(format!(
                        "photograph contains conflicting copies of an order row in '{target}'"
                    )));
                }
                Some(_) => {}
                None => {
                    rows.insert(entry.key, (entry.value, source_is_canonical));
                }
            }
        }
    }

    let mut namespaces = normalized
        .into_iter()
        .map(|(name, entries)| NamespaceData {
            name,
            entries: entries
                .into_iter()
                .map(|(key, (value, _))| SnapshotEntry { key, value })
                .collect(),
        })
        .collect::<Vec<_>>();
    if normalize_source_residue {
        normalize_authoritative_dense_order_rows(&mut namespaces)?;
    }
    validate_order_baseline(&namespaces)?;
    Ok(namespaces)
}

pub(super) fn photograph_namespace_for_key<'a>(source: &'a str, key: &[u8]) -> &'a str {
    let Ok(key) = std::str::from_utf8(key) else {
        return source;
    };
    let base_key = strip_storage_prefix(key).map_or(key, |(_, base)| base);
    if colon_prefix_matches(base_key, MORD_PREFIX) {
        "field_update_order_log"
    } else if colon_prefix_matches(base_key, MOC_PREFIX) {
        "field_update_order_count"
    } else {
        source
    }
}

/// Normalize the dense order baseline to a contiguous sequence the read path
/// can address. Rows below `moc:` are authoritative even when a copied source
/// has a hole, so S compacts the present rows in sequence order and stamps the
/// resulting count. Rows at or above `moc:` are stale residue. Dense rows with
/// no count are also residue. Sparse `mord:{M}\0...` rows have no count and are
/// always retained.
pub(super) fn normalize_authoritative_dense_order_rows(
    namespaces: &mut [NamespaceData],
) -> SyncResult<()> {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

    let mut counts = BTreeMap::<String, usize>::new();
    for namespace in namespaces.iter() {
        if namespace.name != "field_update_order_count" {
            continue;
        }
        for entry in &namespace.entries {
            let key_bytes = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            let key = std::str::from_utf8(&key_bytes).map_err(|error| {
                SyncError::Serialization(format!("order key is not UTF-8: {error}"))
            })?;
            let value = BASE64.decode(&entry.value).map_err(|error| {
                SyncError::Serialization(format!("invalid value base64: {error}"))
            })?;
            let count: usize = serde_json::from_slice(&value).map_err(|error| {
                SyncError::Serialization(format!(
                    "photograph order count '{key}' is invalid: {error}"
                ))
            })?;
            counts.insert(key.to_string(), count);
        }
    }

    let mut dense_rows = BTreeMap::<String, BTreeMap<usize, SnapshotEntry>>::new();
    let mut sparse_rows = Vec::<SnapshotEntry>::new();
    for namespace in namespaces.iter() {
        if namespace.name != "field_update_order_log" {
            continue;
        }
        for entry in &namespace.entries {
            let key_bytes = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            let key = std::str::from_utf8(&key_bytes).map_err(|error| {
                SyncError::Serialization(format!("order key is not UTF-8: {error}"))
            })?;
            if let Some((count_key, seq)) = dense_count_key_and_seq(key) {
                let stored = stored_count_key(&counts, &count_key);
                if counts.get(&stored).is_some_and(|count| seq < *count) {
                    dense_rows
                        .entry(stored)
                        .or_default()
                        .insert(seq, entry.clone());
                }
            } else {
                sparse_rows.push(entry.clone());
            }
        }
    }
    let effective_counts = counts
        .keys()
        .map(|key| (key.clone(), dense_rows.get(key).map_or(0, BTreeMap::len)))
        .collect::<BTreeMap<_, _>>();

    for namespace in namespaces.iter_mut() {
        if namespace.name == "field_update_order_count" {
            for entry in &mut namespace.entries {
                let key_bytes = BASE64.decode(&entry.key).map_err(|error| {
                    SyncError::Serialization(format!("invalid key base64: {error}"))
                })?;
                let key = std::str::from_utf8(&key_bytes).map_err(|error| {
                    SyncError::Serialization(format!("order key is not UTF-8: {error}"))
                })?;
                if let Some(effective) = effective_counts.get(key) {
                    entry.value =
                        BASE64.encode(serde_json::to_vec(effective).map_err(|error| {
                            SyncError::Serialization(format!(
                                "serialize normalized order count '{key}': {error}"
                            ))
                        })?);
                }
            }
            continue;
        }
        if namespace.name != "field_update_order_log" {
            continue;
        }
        let mut retained = sparse_rows.clone();
        for (count_key, rows) in &dense_rows {
            for (new_seq, entry) in rows.values().enumerate() {
                let mut entry = entry.clone();
                entry.key = BASE64.encode(dense_key_for_count(count_key, new_seq)?);
                retained.push(entry);
            }
        }
        retained.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        namespace.entries = retained;
    }
    Ok(())
}

pub(super) fn stored_count_key(counts: &BTreeMap<String, usize>, count_key: &str) -> String {
    if counts.contains_key(count_key) {
        return count_key.to_string();
    }
    if let Some(twin) = form_twin(count_key) {
        if counts.contains_key(&twin) {
            return twin;
        }
    }
    count_key.to_string()
}

pub(super) fn dense_key_for_count(count_key: &str, seq: usize) -> SyncResult<String> {
    let (scope, base_key) = strip_storage_prefix(count_key)
        .map_or((None, count_key), |(scope, base)| (Some(scope), base));
    let molecule = rest_of(base_key, "moc").ok_or_else(|| {
        SyncError::Serialization(format!("invalid order count key '{count_key}'"))
    })?;
    let mut key = order_entry_key(molecule, seq);
    if !base_key.contains("moc\0") {
        key = rewrite_key_like(&key, "mord:");
    }
    Ok(scope.map_or(key.clone(), |scope| format!("{scope}:{key}")))
}

/// Reject a logical photograph whose dense order count names a row that the
/// photograph does not contain. MutationIntent does not carry raw order rows,
/// so a receiver cannot repair a hole after the fold cut.
pub(super) fn validate_order_baseline(namespaces: &[NamespaceData]) -> SyncResult<()> {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

    let mut counts = BTreeMap::<String, usize>::new();
    let mut dense_rows = BTreeMap::<String, HashSet<usize>>::new();
    for namespace in namespaces {
        if namespace.name != "field_update_order_log"
            && namespace.name != "field_update_order_count"
        {
            continue;
        }
        for entry in &namespace.entries {
            let key_bytes = BASE64.decode(&entry.key).map_err(|error| {
                SyncError::Serialization(format!("invalid key base64: {error}"))
            })?;
            let key = std::str::from_utf8(&key_bytes).map_err(|error| {
                SyncError::Serialization(format!("order key is not UTF-8: {error}"))
            })?;
            let base_key = strip_storage_prefix(key).map_or(key, |(_, base)| base);
            if colon_prefix_matches(base_key, MOC_PREFIX) {
                let value = BASE64.decode(&entry.value).map_err(|error| {
                    SyncError::Serialization(format!("invalid value base64: {error}"))
                })?;
                let count: usize = serde_json::from_slice(&value).map_err(|error| {
                    SyncError::Serialization(format!(
                        "photograph order count '{key}' is invalid: {error}"
                    ))
                })?;
                counts.insert(key.to_string(), count);
            } else if let Some((count_key, seq)) = dense_count_key_and_seq(key) {
                dense_rows
                    .entry(stored_count_key(&counts, &count_key))
                    .or_default()
                    .insert(seq);
            }
        }
    }

    for (count_key, rows) in &dense_rows {
        if !counts.contains_key(count_key) {
            return Err(SyncError::Serialization(format!(
                "photograph order baseline has rows but no count '{count_key}'"
            )));
        }
        let count = counts[count_key];
        if let Some(missing) = (0..count).find(|seq| !rows.contains(seq)) {
            return Err(SyncError::Serialization(format!(
                "photograph order baseline '{count_key}' is missing row {missing} of {count}"
            )));
        }
    }
    for (count_key, count) in counts {
        let rows = dense_rows.get(&count_key);
        if let Some(missing) = (0..count).find(|seq| match rows {
            Some(rows) => !rows.contains(seq),
            None => true,
        }) {
            return Err(SyncError::Serialization(format!(
                "photograph order baseline '{count_key}' is missing row {missing} of {count}"
            )));
        }
    }
    Ok(())
}

pub(super) fn dense_count_key_and_seq(key: &str) -> Option<(String, usize)> {
    let (scope, base_key) =
        strip_storage_prefix(key).map_or((None, key), |(scope, base)| (Some(scope), base));
    let rest = rest_of(base_key, "mord")?;
    let (molecule, seq) = rest.rsplit_once(':')?;
    if molecule.is_empty() || molecule.contains('\0') {
        return None;
    }
    let count = if base_key.contains("mord\0") {
        crate::kind_partition::anchored("moc", molecule)
    } else {
        crate::kind_partition::flat("moc", molecule)
    };
    let count_key = match scope {
        Some(scope) => format!("{scope}:{count}"),
        None => count,
    };
    Some((count_key, seq.parse().ok()?))
}
