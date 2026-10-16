use super::resolve::*;
use super::*;

impl FieldVariant {
    pub(super) async fn scan_hash_range_matches(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        hash: &str,
        storage_prefix: Option<&str>,
        window: &Option<KeyWindow>,
        include_tombstones: bool,
        use_resident: bool,
    ) -> Result<(Vec<TipMatch>, bool), SchemaError> {
        let mut window_applied = false;
        let partition_read = (use_resident && window.is_none()).then(|| {
            db_ops
                .resident()
                .begin_partition_interval_read(molecule_uuid, hash, "", None)
        });
        let page_read = window.as_ref().filter(|_| use_resident).and_then(|window| {
            db_ops
                .resident()
                .begin_page_read(molecule_uuid, hash, window, include_tombstones)
        });
        let paged = if let Some(window) = window.as_ref().filter(|_| use_resident) {
            partition_window::merged_tip_window(
                db_ops,
                molecule_uuid,
                hash,
                storage_prefix,
                window,
                include_tombstones,
            )
            .await?
        } else if let Some(window) = &window {
            db_ops
                .atoms()
                .scan_hash_tip_window(
                    molecule_uuid,
                    hash,
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
        } else if let Some(unique) = db_ops
            .atoms()
            .get_unique_hash_key_record(molecule_uuid, hash, storage_prefix)
            .await?
        {
            vec![unique]
        } else {
            // Storage-form hash (single path).
            let prefixes = db_ops
                .atoms()
                .key_codec_for_molecule(molecule_uuid)
                .api_hash_range_scan_prefixes_for_read(molecule_uuid, hash)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            db_ops
                .atoms()
                .scan_per_key_prefixes(&prefixes, storage_prefix)
                .await?
        };
        let rows = Self::api_rows_from_scan(db_ops, hash, scanned)?;
        if partition_read.is_some() || (window_applied && page_read.is_some()) {
            let tips = rows
                .iter()
                .map(|(key, rec, _)| crate::resident::ResidentTip {
                    molecule_uuid: molecule_uuid.to_string(),
                    hash: hash.to_string(),
                    range: key.range.clone().unwrap_or_default(),
                    atom_uuid: rec.entry.atom_uuid.clone(),
                    written_at: rec.entry.written_at,
                    logical_counter: rec.entry.logical_counter,
                    device_id: rec.entry.lww_device().to_string(),
                    mutation_uuid: rec.entry.mutation_uuid.clone(),
                    writer_pubkey: rec.entry.writer_pubkey.clone(),
                    key_metadata: rec.meta.clone(),
                })
                .collect();
            if let Some(read) = partition_read {
                db_ops.resident().finish_partition_read(&read, tips);
            } else if let Some(read) = page_read {
                db_ops.resident().finish_page_read(&read, tips);
            }
        }
        Ok((rows, window_applied))
    }

    /// Convert scanned storage keys into API-keyed tip matches.
    pub(super) fn api_rows_from_scan(
        db_ops: &Arc<DbOperations>,
        hash: &str,
        scanned: Vec<(String, crate::db_operations::atom_store::PerKeyRecord)>,
    ) -> Result<Vec<TipMatch>, SchemaError> {
        let mut rows = Vec::with_capacity(scanned.len());
        for (storage_key, rec) in scanned {
            let (_decoded_hash, decoded_range) =
                crate::atom::molecule_key_codec::decode_hash_range_any(&storage_key).ok_or_else(
                    || {
                        SchemaError::InvalidData(format!(
                            "malformed hash-range record key: {storage_key}"
                        ))
                    },
                )?;
            // Option I: never surface storage-form hash (blind token) as API key.
            // Under OPE, also leak-decode range so clients see API plaintext
            // (e.g. BoardCards sk `todo#pos#slug`).
            let range = if db_ops.atoms().key_codec().range_encoding().writes_ope() {
                crate::crypto::E2eKeys::ope_decode_range_plaintext(&decoded_range)
                    .unwrap_or(decoded_range)
            } else {
                decoded_range
            };
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

    /// Count and log a tip whose atom body could not be resolved.
    pub(super) fn record_unresolved_tip_atom(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        atom_uuid: &str,
        key: &KeyValue,
        tip_key: &str,
    ) {
        db_ops.record_unresolved_atom_skip(
            atom_uuid,
            key,
            crate::db_operations::core::UnresolvedAtomContext {
                molecule_uuid: Some(molecule_uuid),
                atom_partition: crate::atom::AtomPartition::from_record_key(tip_key).as_ref(),
                tip_storage_key: Some(tip_key),
                ..Default::default()
            },
        );
        tracing::warn!(
            atom_uuid = %atom_uuid,
            key = %key,
            "Skipping unresolved atom ref from hash-key query result"
        );
    }

    /// Hydrate atoms for the selected tips into field values.
    pub(super) async fn materialize_tip_matches(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        include_tombstones: bool,
        visible_matches: Vec<TipMatch>,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        // Tip base key → partition (cannot disagree with the tip walk). Fall
        // back to for_slot only if the tip key is unparseable.
        let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = visible_matches
            .iter()
            .map(|(key, rec, tip_key)| {
                let partition =
                    crate::atom::AtomPartition::from_record_key(tip_key).or_else(|| {
                        Some(crate::atom::AtomPartition::for_slot(
                            molecule_uuid,
                            key.hash.as_deref().unwrap_or(""),
                        ))
                    });
                (rec.entry.atom_uuid.as_str(), partition)
            })
            .collect();
        let atoms = crate::db_operations::resident_read::get_atoms_located_resident_first(
            db_ops,
            &slots,
            storage_prefix,
        )
        .await?;

        let mut resolved = HashMap::with_capacity(visible_matches.len());
        for ((key, rec, tip_key), atom) in visible_matches.into_iter().zip(atoms) {
            let atom_uuid = rec.entry.atom_uuid.clone();
            let Some(atom) = atom else {
                if storage_prefix.is_some() {
                    // Expected org-scope filtering, not an integrity fault —
                    // deliberately not counted as an unresolved skip.
                    tracing::warn!("Filtering orphan atom ref from org-scoped hash-key read");
                    continue;
                }
                Self::record_unresolved_tip_atom(db_ops, molecule_uuid, &atom_uuid, &key, &tip_key);
                continue;
            };

            if !include_tombstones && crate::atom::is_tombstone_value(atom.content()) {
                // Same accounting as the windowed path above: the window (line
                // ~483) already spent a slot on this row, so the drop has to be
                // reported or the page silently comes up short.
                crate::db_operations::note_tombstoned_row();
                continue;
            }

            let (source_file_name, metadata) = match rec.meta {
                Some(km) => (
                    km.source_file_name
                        .or_else(|| atom.source_file_name().cloned()),
                    km.metadata.or_else(|| atom.metadata().cloned()),
                ),
                None => (atom.source_file_name().cloned(), atom.metadata().cloned()),
            };
            let written_at = atom
                .created_at()
                .timestamp_nanos_opt()
                .and_then(|ns| u64::try_from(ns).ok());
            let writer_pubkey = if rec.entry.writer_pubkey.is_empty() {
                None
            } else {
                Some(rec.entry.writer_pubkey)
            };

            resolved.insert(
                key,
                FieldValue {
                    value: atom.content().clone(),
                    atom_uuid,
                    source_file_name,
                    metadata,
                    molecule_uuid: Some(molecule_uuid.to_string()),
                    molecule_version: None,
                    writer_pubkey,
                    written_at,
                },
            );
        }
        Ok(resolved)
    }
}
