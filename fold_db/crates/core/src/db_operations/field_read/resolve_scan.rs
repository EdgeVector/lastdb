use super::*;
use crate::schema::types::KeyValuePageOrder;

pub(super) async fn scan_hash_range_matches(
    db_ops: &Arc<DbOperations>,
    molecule_uuid: &str,
    filter: &HashRangeFilter,
    storage_prefix: Option<&str>,
    window: &Option<KeyWindow>,
    include_tombstones: bool,
) -> Result<(Vec<TipMatch>, bool), SchemaError> {
    let (HashRangeFilter::HashKey(hash)
    | HashRangeFilter::HashRangePrefix { hash, .. }
    | HashRangeFilter::HashRangeRange { hash, .. }) = filter
    else {
        return Err(SchemaError::InvalidData(
            "tip page requires one hash".into(),
        ));
    };
    let mut window_applied = false;
    let paged = if let Some(window) = &window {
        db_ops
            .atoms()
            .scan_hash_tip_window(
                molecule_uuid,
                filter,
                storage_prefix,
                window,
                include_tombstones,
            )
            .await?
    } else {
        None
    };
    let scanned = if let Some(paged) = paged {
        window_applied = true;
        paged
    } else {
        // Complete hash reads retain their resource fence. A range must
        // never fall back to a broader, unfiltered hash read here.
        if !matches!(filter, HashRangeFilter::HashKey(_)) {
            return Err(SchemaError::InvalidData(
                "hash range selection requires a bounded page".into(),
            ));
        }
        let prefix = db_ops
            .atoms()
            .key_codec_for_molecule(molecule_uuid)?
            .api_hash_range_scan_prefix_for_hash(molecule_uuid, hash)
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
        db_ops
            .atoms()
            .scan_per_key_prefix(&prefix, storage_prefix)
            .await?
    };
    let rows = api_rows_from_scan(db_ops, molecule_uuid, hash, scanned)?;
    Ok((rows, window_applied))
}

/// Convert scanned storage keys into API-keyed tip matches.
pub(super) fn api_rows_from_scan(
    db_ops: &Arc<DbOperations>,
    molecule: &str,
    hash: &str,
    scanned: Vec<(String, crate::db_operations::atom_store::PerKeyRecord)>,
) -> Result<Vec<TipMatch>, SchemaError> {
    let mut rows = Vec::with_capacity(scanned.len());
    let codec = db_ops.atoms().key_codec_for_molecule(molecule)?;
    for (storage_key, rec) in scanned {
        let (_decoded_hash, decoded_range) =
            crate::atom::molecule_key_codec::decode_hash_range(&storage_key, molecule).ok_or_else(
                || SchemaError::InvalidData("malformed hash-range record key".into()),
            )?;
        let range = codec
            .api_range(&decoded_range)
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        rows.push((
            KeyValue::new(Some(hash.to_string()), Some(range)),
            rec,
            storage_key,
        ));
    }
    Ok(rows)
}

/// Drop hidden tombstones and apply the key window when storage did not.
pub(super) fn page_visible_matches(
    matches: Vec<TipMatch>,
    window: Option<KeyWindow>,
    window_applied: bool,
    include_tombstones: bool,
) -> Vec<TipMatch> {
    let mut visible_matches: Vec<_> = matches
        .into_iter()
        .filter(|(_, rec, _)| {
            include_tombstones || !rec.meta.as_ref().is_some_and(|m| m.tombstoned)
        })
        .collect();

    // Compatibility and single-key paths still apply the window here.
    // Ordered partition pages already selected their tips in storage.
    if let Some(window) = window.filter(|_| !window_applied) {
        visible_matches.sort_by(|(a, ..), (b, ..)| a.cmp_page_order(b));
        visible_matches = match window {
            KeyWindow::Offset { offset, limit } => visible_matches
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect(),
            KeyWindow::After { after, limit } => visible_matches
                .into_iter()
                .filter(|(key, ..)| key.cmp_page_order(&after).is_gt())
                .take(limit)
                .collect(),
        };
    }
    visible_matches
}

/// Hydrate atoms for the selected tips into field values.
pub(super) async fn materialize_tip_matches(
    db_ops: &Arc<DbOperations>,
    molecule_uuid: &str,
    storage_prefix: Option<&str>,
    include_tombstones: bool,
    visible_matches: Vec<TipMatch>,
) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
    let runtime = db_ops.namespaced_store().disk_pack_runtime();
    let matches = visible_matches
        .into_iter()
        .map(|(key, rec, tip_key)| {
            let source = crate::schema::types::field::build_storage_key(storage_prefix, &tip_key);
            crate::storage::disk_pack::logical_paths::core_read::note_winning_tip(
                runtime.clone(),
                storage_prefix,
                molecule_uuid,
                &key,
                &rec.entry,
                rec.meta.as_ref(),
                &source,
            )?;
            note_query_tip_candidates(
                db_ops,
                storage_prefix,
                molecule_uuid,
                &key,
                &rec.entry.atom_uuid,
            )?;
            let writer = (!rec.entry.writer_pubkey().is_empty())
                .then(|| rec.entry.writer_pubkey().to_string());
            Ok((key, rec.entry.atom_uuid, rec.meta, writer))
        })
        .collect::<Result<Vec<_>, SchemaError>>()?;
    let mut resolved = fetch_atoms_with_key_metadata_async_with_prefix(
        db_ops,
        matches,
        storage_prefix,
        Some(molecule_uuid),
    )
    .await?;
    for value in resolved.values_mut() {
        value.molecule_uuid = Some(molecule_uuid.to_string());
    }
    retain_visible_values(&mut resolved, include_tombstones);
    Ok(resolved)
}
