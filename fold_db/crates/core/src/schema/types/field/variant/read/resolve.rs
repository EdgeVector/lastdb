// lint:file-size-ok verbatim move from the original module; one long function remains, splitting it is separate work
use super::*;

impl FieldVariant {
    /// Resolves field values by refreshing the field, applying filters, and
    /// fetching atom content. See prior docs for tombstone visibility.
    pub async fn resolve_value(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        self.resolve_value_windowed(db_ops, filter, None, as_of, include_tombstones)
            .await
    }

    /// [`Self::resolve_value`], but hydrating only the requested `window` of the
    /// matched keys.
    ///
    /// `window` is `(offset, limit)` over the [`KeyValue::cmp_page_order`]
    /// enumeration of the keys this filter matched — applied **after** the
    /// molecule tombstone-flag gate and **before** any atom body is loaded.
    /// That placement is the whole point: key enumeration walks the molecule
    /// tip index, which is cheap and already bounded by the filter, while body
    /// hydration reads, decrypts and materializes an atom per row. Slicing
    /// after hydration (what a key-restricted read did before) pays
    /// `O(partition)` to return `O(page)`, on every page request.
    ///
    /// A window is only meaningful for a filter that selects a set of keys.
    /// `Page` / `PageAfter` already carry their own bounds and must not be
    /// windowed twice, so callers pass `None` for those; this is enforced one
    /// level up, where the filter's shape is known.
    ///
    /// Rows can still drop out after the window is taken — a content tombstone
    /// or an unresolvable atom ref — so a full page in gives at most a full
    /// page out. That is not new: every push-down path can come up short, and
    /// the node reconciles `has_more` against the counted total rather than
    /// against the page length.
    pub async fn resolve_value_windowed(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        use crate::schema::types::field::fetch_atoms_with_key_metadata_async_with_prefix;

        if let Some(resolved) = self
            .resolve_hash_key_fast(
                db_ops,
                filter.as_ref(),
                window.clone(),
                as_of,
                include_tombstones,
            )
            .await?
        {
            self.record_resolved_tip_keys(&resolved);
            return Ok(resolved);
        }

        let storage_prefix_owned: Option<String> = self.common().storage_prefix.clone();
        let mut matches_with_meta = self
            .collect_matches(db_ops, filter, as_of, include_tombstones)
            .await?;

        if let Some(window) = window {
            // `collect_matches` hands back a Vec built from a HashMap, so its
            // order is randomized per call. Sorting is not a nicety here: an
            // unsorted slice would return a different arbitrary subset on every
            // request, so consecutive pages would overlap and drop rows.
            matches_with_meta.sort_by(|(a, ..), (b, ..)| a.cmp_page_order(b));
            matches_with_meta = match window {
                KeyWindow::Offset { offset, limit } => matches_with_meta
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .collect(),
                KeyWindow::After { after, limit } => matches_with_meta
                    .into_iter()
                    .filter(|(key, ..)| key.cmp_page_order(&after).is_gt())
                    .take(limit)
                    .collect(),
            };
        }

        let mut resolved = fetch_atoms_with_key_metadata_async_with_prefix(
            db_ops,
            matches_with_meta,
            storage_prefix_owned.as_deref(),
            self.common().molecule_uuid().map(String::as_str),
            None,
            None,
        )
        .await?;

        if !include_tombstones {
            // Count what this drops. The window above was already spent on these
            // rows and `count_rows` already counted them (it gates on
            // `KeyMetadata.tombstoned`, which a content tombstone need not set),
            // so a silent `retain` hands back a short page that is
            // indistinguishable from the end of the set — and leaves `has_more`
            // comparing a delivered-row count against a total that includes
            // them.
            resolved.retain(|_, fv| {
                let live = !crate::atom::is_tombstone_value(&fv.value);
                if !live {
                    crate::db_operations::note_tombstoned_row();
                }
                live
            });
        }

        self.stamp_molecule_meta(&mut resolved);
        self.record_resolved_tip_keys(&resolved);
        Ok(resolved)
    }

    // lint:fn-size-ok moved verbatim from the original module; splitting it is a separate change
    pub(super) async fn resolve_hash_key_fast(
        &self,
        db_ops: &Arc<DbOperations>,
        filter: Option<&HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<Option<HashMap<KeyValue, FieldValue>>, SchemaError> {
        if as_of.is_some() {
            return Ok(None);
        }

        let Some(HashRangeFilter::HashKey(hash)) = filter else {
            return Ok(None);
        };
        let Some(molecule_uuid) = self.common().molecule_uuid().cloned() else {
            return Ok(None);
        };

        let storage_prefix = self.common().storage_prefix();

        // A HashRange HashKey is a partition enumeration, not a point read.
        // Unbounded reads with pending writes use the general overlay path.
        // Pages merge bounded resident and durable tip windows before counting
        // visible rows. Hash fields remain true point reads below.
        if matches!(self.kind, FieldKind::HashRange)
            && crate::db_operations::resident_read::tip_reads_resident_first(storage_prefix)
        {
            let authoritative = matches!(
                db_ops.resident().key_set_completeness_for(&molecule_uuid),
                ResidentKeySetCompleteness::Complete { .. }
            );
            let pending_overlay = db_ops.resident().molecule_has_dirty(&molecule_uuid);
            if authoritative || (pending_overlay && window.is_none()) {
                return Ok(None);
            }
        }

        // A window that selects nothing selects nothing on every path below, and
        // saying so here keeps the empty case from depending on which branch a
        // field kind happens to take.
        if matches!(
            window,
            Some(KeyWindow::Offset { limit: 0, .. } | KeyWindow::After { limit: 0, .. })
        ) {
            return Ok(Some(HashMap::new()));
        }

        // Resident-first HashKey path: serve tip+atom from T0 when both are
        // present (second+ resolve is a hit; cold path rehydrates below).
        // A `Hash` field's `HashKey` read is a point read — one row, or none —
        // so any window past the first row is empty and a window that includes
        // it changes nothing.
        if matches!(self.kind, FieldKind::Hash)
            && as_of.is_none()
            && storage_prefix.is_none()
            && window
                .as_ref()
                .is_none_or(|window| matches!(window, KeyWindow::Offset { offset: 0, .. }))
        {
            if let Some(tip_out) = db_ops.resident().resolve_tip(&molecule_uuid, hash, "") {
                if let Some(atom_out) = db_ops.resident().resolve_atom(&tip_out.value.atom_uuid) {
                    let atom = &atom_out.value;
                    if include_tombstones || !crate::atom::is_tombstone_value(&atom.content) {
                        let mut resolved = HashMap::new();
                        resolved.insert(
                            KeyValue::new(Some(hash.clone()), None),
                            FieldValue {
                                value: atom.content.clone(),
                                atom_uuid: atom.uuid.clone(),
                                source_file_name: tip_out
                                    .value
                                    .key_metadata
                                    .as_ref()
                                    .and_then(|m| m.source_file_name.clone())
                                    .or_else(|| atom.source_file_name.clone()),
                                metadata: tip_out
                                    .value
                                    .key_metadata
                                    .as_ref()
                                    .and_then(|m| m.metadata.clone())
                                    .or_else(|| atom.metadata.clone())
                                    .or_else(|| {
                                        atom.file_blob_ref.as_ref().map(|r| {
                                            let mut m = HashMap::new();
                                            m.insert("file_blob_ref".to_string(), r.clone());
                                            m
                                        })
                                    }),
                                molecule_uuid: Some(molecule_uuid.clone()),
                                molecule_version: None,
                                writer_pubkey: (!tip_out.value.writer_pubkey.is_empty())
                                    .then_some(tip_out.value.writer_pubkey.clone()),
                                written_at: atom
                                    .created_at
                                    .and_then(|t| t.timestamp_nanos_opt())
                                    .and_then(|ns| u64::try_from(ns).ok()),
                            },
                        );
                        return Ok(Some(resolved));
                    }
                }
            }
        }

        // Each row: (api KeyValue, tip record, tip base key for partition).
        let mut window_applied = false;
        let matches: Vec<(KeyValue, _, String)> = match self.kind {
            FieldKind::Hash => {
                let observed = db_ops.resident().observe_slot(&molecule_uuid, hash, "");
                let Some((base_key, rec)) = db_ops
                    .atoms()
                    .get_per_key_1d(&molecule_uuid, hash, "", storage_prefix)
                    .await?
                else {
                    return Ok(Some(HashMap::new()));
                };
                if storage_prefix.is_none() {
                    // The keyed read already supplied the tip. Reusing it
                    // avoids a second tip read and a premature atom fetch
                    // without a partition hint. Keep API coordinates in T0;
                    // get_per_key_1d owns blind-hash / encoded-range storage.
                    db_ops.resident().rehydrate_tip_at(
                        crate::resident::ResidentTip {
                            molecule_uuid: molecule_uuid.clone(),
                            hash: hash.clone(),
                            range: String::new(),
                            atom_uuid: rec.entry.atom_uuid.clone(),
                            written_at: rec.entry.written_at,
                            logical_counter: rec.entry.logical_counter,
                            device_id: rec.entry.lww_device().to_string(),
                            mutation_uuid: rec.entry.mutation_uuid.clone(),
                            key_metadata: rec.meta.clone(),
                            writer_pubkey: rec.entry.writer_pubkey.clone(),
                        },
                        Some((observed.resident_revision, observed.durable_revision)),
                    );
                }
                vec![(KeyValue::new(Some(hash.clone()), None), rec, base_key)]
            }
            FieldKind::HashRange => {
                let use_resident =
                    crate::db_operations::resident_read::tip_reads_resident_first(storage_prefix);
                let cached = use_resident
                    .then(|| {
                        if let Some(window) = &window {
                            db_ops
                                .resident()
                                .resolve_partition_window(
                                    &molecule_uuid,
                                    hash,
                                    window,
                                    include_tombstones,
                                )
                                .or_else(|| {
                                    db_ops.resident().resolve_exact_page(
                                        &molecule_uuid,
                                        hash,
                                        window,
                                        include_tombstones,
                                    )
                                })
                        } else {
                            db_ops.resident().resolve_partition_interval(
                                &molecule_uuid,
                                hash,
                                "",
                                None,
                            )
                        }
                    })
                    .flatten();
                if let Some(tips) = cached {
                    window_applied = window.is_some();
                    let codec = db_ops.atoms().key_codec_for_molecule(&molecule_uuid);
                    tips.into_iter()
                        .map(|tip| {
                            let key = codec
                                .api_hash_range_record_keys_for_read(
                                    &molecule_uuid,
                                    &tip.hash,
                                    &tip.range,
                                )
                                .map_err(|e| SchemaError::InvalidData(e.to_string()))?
                                .into_iter()
                                .next()
                                .expect("primary tip key");
                            let mut entry = crate::atom::AtomEntry::thin_with_author(
                                tip.atom_uuid,
                                tip.written_at,
                                tip.logical_counter,
                                tip.device_id,
                                tip.mutation_uuid,
                                String::new(),
                            );
                            entry.writer_pubkey = tip.writer_pubkey;
                            Ok((
                                KeyValue::new(Some(tip.hash), Some(tip.range)),
                                crate::db_operations::atom_store::PerKeyRecord {
                                    entry,
                                    meta: tip.key_metadata,
                                },
                                key,
                            ))
                        })
                        .collect::<Result<Vec<_>, SchemaError>>()?
                } else {
                    let partition_read = (use_resident && window.is_none()).then(|| {
                        db_ops.resident().begin_partition_interval_read(
                            &molecule_uuid,
                            hash,
                            "",
                            None,
                        )
                    });
                    let page_read = window.as_ref().filter(|_| use_resident).and_then(|window| {
                        db_ops.resident().begin_page_read(
                            &molecule_uuid,
                            hash,
                            window,
                            include_tombstones,
                        )
                    });
                    let paged = if let Some(window) = window.as_ref().filter(|_| use_resident) {
                        partition_window::merged_tip_window(
                            db_ops,
                            &molecule_uuid,
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
                                &molecule_uuid,
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
                        .get_unique_hash_key_record(&molecule_uuid, hash, storage_prefix)
                        .await?
                    {
                        vec![unique]
                    } else {
                        // Storage-form hash (single path).
                        let prefixes = db_ops
                            .atoms()
                            .key_codec_for_molecule(&molecule_uuid)
                            .api_hash_range_scan_prefixes_for_read(&molecule_uuid, hash)
                            .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                        db_ops
                            .atoms()
                            .scan_per_key_prefixes(&prefixes, storage_prefix)
                            .await?
                    };
                    let mut rows = Vec::with_capacity(scanned.len());
                    for (storage_key, rec) in scanned {
                        let (_decoded_hash, decoded_range) =
                            crate::atom::molecule_key_codec::decode_hash_range_any(&storage_key)
                                .ok_or_else(|| {
                                    SchemaError::InvalidData(format!(
                                        "malformed hash-range record key: {storage_key}"
                                    ))
                                })?;
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
                            KeyValue::new(Some(hash.clone()), Some(range)),
                            rec,
                            storage_key,
                        ));
                    }
                    if partition_read.is_some() || (window_applied && page_read.is_some()) {
                        let tips = rows
                            .iter()
                            .map(|(key, rec, _)| crate::resident::ResidentTip {
                                molecule_uuid: molecule_uuid.clone(),
                                hash: hash.clone(),
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
                    rows
                }
            }
            _ => return Ok(None),
        };

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

        if visible_matches.is_empty() {
            return Ok(Some(HashMap::new()));
        }

        // Tip base key → partition (cannot disagree with the tip walk). Fall
        // back to for_slot only if the tip key is unparseable.
        let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = visible_matches
            .iter()
            .map(|(key, rec, tip_key)| {
                let partition =
                    crate::atom::AtomPartition::from_record_key(tip_key).or_else(|| {
                        Some(crate::atom::AtomPartition::for_slot(
                            &molecule_uuid,
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
                db_ops.record_unresolved_atom_skip(
                    &atom_uuid,
                    &key,
                    crate::db_operations::core::UnresolvedAtomContext {
                        molecule_uuid: Some(molecule_uuid.as_str()),
                        atom_partition: crate::atom::AtomPartition::from_record_key(&tip_key)
                            .as_ref(),
                        tip_storage_key: Some(&tip_key),
                        ..Default::default()
                    },
                );
                tracing::warn!(
                    atom_uuid = %atom_uuid,
                    key = %key,
                    "Skipping unresolved atom ref from hash-key query result"
                );
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

            // Install the atom already fetched with its tip-derived partition.
            if storage_prefix.is_none() {
                db_ops
                    .resident()
                    .rehydrate_atom(crate::resident::ResidentAtom::from_atom(&atom));
            }

            resolved.insert(
                key,
                FieldValue {
                    value: atom.content().clone(),
                    atom_uuid,
                    source_file_name,
                    metadata,
                    molecule_uuid: Some(molecule_uuid.clone()),
                    molecule_version: None,
                    writer_pubkey,
                    written_at,
                },
            );
        }
        Ok(Some(resolved))
    }

    /// Stamp molecule uuid/version/writer onto a resolved field map.
    pub(crate) fn stamp_molecule_meta(&self, resolved: &mut HashMap<KeyValue, FieldValue>) {
        let mol_uuid = self.common().molecule_uuid().cloned();
        let mol_version = self.molecule_version();
        let mol_writer_pubkey = self.molecule_writer_pubkey();
        for fv in resolved.values_mut() {
            fv.molecule_uuid = mol_uuid.clone();
            fv.molecule_version = mol_version;
            if let Some(pk) = mol_writer_pubkey.clone() {
                fv.writer_pubkey = Some(pk);
            }
        }
    }

    pub(super) fn entry_meta_at(
        &self,
        kv: &KeyValue,
    ) -> (Option<crate::atom::KeyMetadata>, Option<String>) {
        (
            self.key_metadata_at_key(kv).cloned(),
            self.atom_entry_at_key(kv)
                .map(|entry| entry.writer_pubkey.clone()),
        )
    }
}
