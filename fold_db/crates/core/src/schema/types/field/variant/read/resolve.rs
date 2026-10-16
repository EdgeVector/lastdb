// lint:file-size-ok resolve_hash_key_fast is split into helpers; the helpers stay in this module
use super::*;
pub(super) type TipMatch = (
    KeyValue,
    crate::db_operations::atom_store::PerKeyRecord,
    String,
);

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

        if let Some(hit) = self.resident_hash_point_read(
            db_ops,
            &molecule_uuid,
            hash,
            storage_prefix,
            &window,
            include_tombstones,
        ) {
            return Ok(Some(hit));
        }

        let (matches, window_applied) = match self.kind {
            FieldKind::Hash => (
                Self::hash_field_tip_match(db_ops, &molecule_uuid, hash, storage_prefix).await?,
                false,
            ),
            FieldKind::HashRange => {
                Self::hash_range_tip_matches(
                    db_ops,
                    &molecule_uuid,
                    hash,
                    storage_prefix,
                    &window,
                    include_tombstones,
                )
                .await?
            }
            _ => return Ok(None),
        };

        let visible_matches =
            Self::page_visible_matches(matches, window, window_applied, include_tombstones);
        if visible_matches.is_empty() {
            return Ok(Some(HashMap::new()));
        }
        Self::materialize_tip_matches(
            db_ops,
            &molecule_uuid,
            storage_prefix,
            include_tombstones,
            visible_matches,
        )
        .await
        .map(Some)
    }

    /// Resident-first point read for a `Hash` field `HashKey` filter.
    pub(super) fn resident_hash_point_read(
        &self,
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        hash: &str,
        storage_prefix: Option<&str>,
        window: &Option<KeyWindow>,
        include_tombstones: bool,
    ) -> Option<HashMap<KeyValue, FieldValue>> {
        // Resident-first HashKey path: serve tip+atom from T0 when both are
        // present (second+ resolve is a hit; cold path rehydrates below).
        // A `Hash` field's `HashKey` read is a point read — one row, or none —
        // so any window past the first row is empty and a window that includes
        // it changes nothing.
        if matches!(self.kind, FieldKind::Hash)
            && storage_prefix.is_none()
            && window
                .as_ref()
                .is_none_or(|window| matches!(window, KeyWindow::Offset { offset: 0, .. }))
        {
            if let Some(tip_out) = db_ops.resident().resolve_tip(molecule_uuid, hash, "") {
                if let Some(atom_out) = db_ops.resident().resolve_atom(&tip_out.value.atom_uuid) {
                    let atom = &atom_out.value;
                    if include_tombstones || !crate::atom::is_tombstone_value(&atom.content) {
                        let mut resolved = HashMap::new();
                        resolved.insert(
                            KeyValue::new(Some(hash.to_string()), None),
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
                                molecule_uuid: Some(molecule_uuid.to_string()),
                                molecule_version: None,
                                writer_pubkey: (!tip_out.value.writer_pubkey.is_empty())
                                    .then_some(tip_out.value.writer_pubkey.clone()),
                                written_at: atom
                                    .created_at
                                    .and_then(|t| t.timestamp_nanos_opt())
                                    .and_then(|ns| u64::try_from(ns).ok()),
                            },
                        );
                        return Some(resolved);
                    }
                }
            }
        }
        None
    }

    /// Tip record for a single `Hash` field key (empty when absent).
    pub(super) async fn hash_field_tip_match(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        hash: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<TipMatch>, SchemaError> {
        let Some((base_key, rec)) = db_ops
            .atoms()
            .get_per_key_1d(molecule_uuid, hash, "", storage_prefix)
            .await?
        else {
            return Ok(Vec::new());
        };
        // One home: this disk tip stays out of the graph. The storage read
        // admits it to the warm set.
        Ok(vec![(
            KeyValue::new(Some(hash.to_string()), None),
            rec,
            base_key,
        )])
    }

    /// Tip records for a `HashRange` partition, plus whether the window was
    /// already applied while selecting them.
    pub(super) async fn hash_range_tip_matches(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        hash: &str,
        storage_prefix: Option<&str>,
        window: &Option<KeyWindow>,
        include_tombstones: bool,
    ) -> Result<(Vec<TipMatch>, bool), SchemaError> {
        let use_resident =
            crate::db_operations::resident_read::tip_reads_resident_first(storage_prefix);
        let cached = use_resident
            .then(|| {
                if let Some(window) = &window {
                    db_ops
                        .resident()
                        .resolve_partition_window(molecule_uuid, hash, window, include_tombstones)
                        .or_else(|| {
                            db_ops.resident().resolve_exact_page(
                                molecule_uuid,
                                hash,
                                window,
                                include_tombstones,
                            )
                        })
                } else {
                    db_ops
                        .resident()
                        .resolve_partition_interval(molecule_uuid, hash, "", None)
                }
            })
            .flatten();
        if let Some(tips) = cached {
            let matches = Self::resident_tips_to_matches(db_ops, molecule_uuid, tips)?;
            return Ok((matches, window.is_some()));
        }
        Self::scan_hash_range_matches(
            db_ops,
            molecule_uuid,
            hash,
            storage_prefix,
            window,
            include_tombstones,
            use_resident,
        )
        .await
    }

    pub(super) fn resident_tips_to_matches(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        tips: Vec<crate::resident::ResidentTip>,
    ) -> Result<Vec<TipMatch>, SchemaError> {
        let codec = db_ops.atoms().key_codec_for_molecule(molecule_uuid);
        tips.into_iter()
            .map(|tip| {
                let key = codec
                    .api_hash_range_record_keys_for_read(molecule_uuid, &tip.hash, &tip.range)
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
            .collect::<Result<Vec<_>, SchemaError>>()
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
