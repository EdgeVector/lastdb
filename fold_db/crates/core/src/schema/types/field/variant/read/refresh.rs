use super::*;

impl FieldVariant {
    pub(super) fn record_resolved_tip_keys(&self, values: &HashMap<KeyValue, FieldValue>) {
        let Some(molecule_uuid) = self.common().molecule_uuid() else {
            return;
        };
        for key in values.keys() {
            crate::request_phases::record_tip_key(
                molecule_uuid,
                key.hash.as_deref().unwrap_or(""),
                key.range.as_deref().unwrap_or(""),
            );
        }
    }

    /// Refreshes the field's data from the database.
    pub async fn refresh_from_db(&mut self, db_ops: &DbOperations) -> Result<(), SchemaError> {
        super::super::super::base::refresh_field_molecule_from_db(
            &mut self.inner,
            &mut self.molecule,
            self.kind,
            db_ops,
        )
        .await
    }

    /// Hydrate the field's molecule for a **read**, fetching the minimum needed
    /// to satisfy `filter`. See prior `refresh_for_read` docs.
    ///
    /// `include_tombstones` decides what a page window counts. A default read
    /// hides tombstones, so its `offset`/`limit` must be measured in rows the
    /// caller will be shown — otherwise the bounded fetch spends the window on
    /// deleted keys and the later tombstone `retain` hands back a short page
    /// that is indistinguishable from a complete one.
    pub(super) async fn refresh_for_read(
        &mut self,
        db_ops: &DbOperations,
        filter: &HashRangeFilter,
        include_tombstones: bool,
    ) -> Result<(), SchemaError> {
        let Some(molecule_uuid) = self.common().molecule_uuid().cloned() else {
            return self.refresh_from_db(db_ops).await;
        };
        let storage_prefix = self.common().storage_prefix().map(ToString::to_string);

        if self.hydrate_from_resident_partition(
            db_ops,
            &molecule_uuid,
            storage_prefix.as_deref(),
            filter,
        )? {
            return Ok(());
        }
        let partition_read =
            Self::partition_interval_for_read(self.kind, storage_prefix.as_deref(), filter).map(
                |(hash, start, end)| {
                    db_ops.resident().begin_partition_interval_read(
                        &molecule_uuid,
                        hash,
                        &start,
                        end.as_deref(),
                    )
                },
            );

        let resident_overlay = Self::resident_overlay_for_read(
            db_ops,
            &molecule_uuid,
            storage_prefix.as_deref(),
            filter,
            self.kind,
        )?;

        self.hydrate_molecule_for_read(
            db_ops,
            &molecule_uuid,
            storage_prefix.as_deref(),
            filter,
            include_tombstones,
            resident_overlay.as_ref(),
        )
        .await?;

        if let (Some(read), Some(molecule)) = (partition_read, self.molecule.as_ref()) {
            let tips = Self::resident_tips_of(&molecule_uuid, molecule);
            db_ops.resident().finish_partition_read(&read, tips);
        }

        self.apply_resident_overlay(&molecule_uuid, resident_overlay);
        Ok(())
    }

    /// The partition interval a hash-range read may serve from resident tips.
    fn partition_interval_for_read<'f>(
        kind: FieldKind,
        storage_prefix: Option<&str>,
        filter: &'f HashRangeFilter,
    ) -> Option<(&'f str, String, Option<String>)> {
        if matches!(kind, FieldKind::HashRange)
            && crate::db_operations::resident_read::tip_reads_resident_first(storage_prefix)
        {
            partition_interval(filter)
        } else {
            None
        }
    }

    /// Serve a hash-range partition interval from resident tips when possible.
    /// Returns true when the molecule was hydrated.
    fn hydrate_from_resident_partition(
        &mut self,
        db_ops: &DbOperations,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        filter: &HashRangeFilter,
    ) -> Result<bool, SchemaError> {
        let Some((hash, start, end)) =
            Self::partition_interval_for_read(self.kind, storage_prefix, filter)
        else {
            return Ok(false);
        };
        let Some(tips) = db_ops.resident().resolve_partition_interval(
            molecule_uuid,
            hash,
            &start,
            end.as_deref(),
        ) else {
            return Ok(false);
        };
        let codec = db_ops.atoms().key_codec_for_molecule(molecule_uuid);
        let records = tips
            .into_iter()
            .map(|tip| {
                let range = codec
                    .storage_range(molecule_uuid, &tip.range)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let mut entry = crate::atom::AtomEntry::thin_with_author(
                    tip.atom_uuid,
                    tip.written_at,
                    tip.logical_counter,
                    tip.device_id,
                    tip.mutation_uuid,
                    String::new(),
                );
                entry.writer_pubkey = tip.writer_pubkey;
                entry.origin_device = tip.origin_device;
                Ok((tip.hash, range, entry, tip.key_metadata))
            })
            .collect::<Result<Vec<_>, SchemaError>>()?;
        self.molecule = Some(crate::atom::MoleculeHashRange::from_per_key_records(
            molecule_uuid.to_string(),
            0,
            Utc::now(),
            records,
        ));
        Ok(true)
    }

    /// Build the resident key-set overlay for a read.
    ///
    /// Resident key-set members use API-form hash/range values. Molecules
    /// loaded from `mk:` use storage-form values (blind hash + OPE range),
    /// so convert once at this seam before either building an authoritative
    /// resident molecule or overlaying a durable one.
    fn resident_overlay_for_read(
        db_ops: &DbOperations,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        filter: &HashRangeFilter,
        kind: FieldKind,
    ) -> Result<Option<ResidentOverlay>, SchemaError> {
        if !crate::db_operations::resident_read::tip_reads_resident_first(storage_prefix) {
            return Ok(None);
        }
        let snapshot = if matches!(kind, FieldKind::HashRange) {
            resident_keys_for_filter(db_ops.resident(), molecule_uuid, filter)
        } else {
            // Hash/Range fields accept compatibility filter variants with
            // different semantics (a Hash field ignores the range half).
            // Only apply two-dimensional bounds to a two-dimensional field.
            db_ops
                .resident()
                .resident_key_set_range(molecule_uuid, None, None)
        };
        // The durable load of a HashRange field builds a molecule whose hash
        // slots are API form for every filter. Its range slots are API form for
        // a point or multi-get filter (the load remaps them, see
        // `expand_filter_range_bounds_for_apply`) and storage form (OPE) for
        // every other filter. A dirty overlay entry must use the same form as
        // the molecule it joins, or the filter never matches it and a read after
        // an ack misses the write.
        let is_hash_range = matches!(kind, FieldKind::HashRange);
        let api_range = is_hash_range
            && matches!(
                filter,
                HashRangeFilter::HashRangeKey { .. } | HashRangeFilter::HashRangeKeys(_)
            );
        let molecule_key = |key: &crate::resident::ResidentMoleculeKey| {
            let hash = if is_hash_range {
                key.hash.clone()
            } else {
                db_ops
                    .atoms()
                    .key_codec()
                    .storage_hash(molecule_uuid, &key.hash)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?
            };
            let range = if api_range {
                key.range.clone()
            } else {
                db_ops
                    .atoms()
                    .key_codec()
                    .storage_range(molecule_uuid, &key.range)
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?
            };
            Ok::<_, SchemaError>((hash, range))
        };
        let mut entries = Vec::with_capacity(snapshot.keys.len());
        for key in &snapshot.keys {
            let Some(tip) = db_ops
                .resident()
                .resolve_tip(molecule_uuid, &key.hash, &key.range)
            else {
                continue;
            };
            let (hash, range) = molecule_key(key)?;
            entries.push((hash, range, tip.value.atom_uuid));
        }
        let mut tombstones = Vec::with_capacity(snapshot.tombstones.len());
        for key in &snapshot.tombstones {
            tombstones.push(molecule_key(key)?);
        }
        let authoritative = matches!(
            snapshot.completeness,
            ResidentKeySetCompleteness::Complete { .. }
        ) && entries.len() == snapshot.keys.len();
        let active = authoritative
            || (db_ops.resident().molecule_has_dirty(molecule_uuid)
                && (!entries.is_empty() || !tombstones.is_empty()));
        Ok(active.then_some((authoritative, entries, tombstones)))
    }

    /// Load the durable molecule (or an empty authoritative one) for a read.
    async fn hydrate_molecule_for_read(
        &mut self,
        db_ops: &DbOperations,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        filter: &HashRangeFilter,
        include_tombstones: bool,
        resident_overlay: Option<&ResidentOverlay>,
    ) -> Result<(), SchemaError> {
        let layout = self.kind.filter_layout();
        if resident_overlay.is_some_and(|(authoritative, ..)| *authoritative) {
            self.molecule = Some(crate::atom::MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                0,
                Utc::now(),
                Vec::new(),
            ));
        } else if matches!(filter, HashRangeFilter::SampleN(_))
            && resident_overlay.is_some_and(|(authoritative, ..)| !*authoritative)
        {
            // A dirty key or a resident tombstone can sort into the first n.
            // The page of the first durable keys cannot backfill past that,
            // so this peek still full-loads mk: tips. Disk tombstones are
            // copied out here: remove_atom_uuid would bump version on the
            // query molecule.
            self.refresh_from_db(db_ops).await?;
            if !include_tombstones {
                if let Some(loaded) = self.molecule.take() {
                    let records = loaded
                        .iter_all_atoms()
                        .filter_map(|(hash, range, _)| {
                            let meta = loaded.get_key_metadata(hash, range).cloned();
                            if meta.as_ref().is_some_and(|meta| meta.tombstoned) {
                                return None;
                            }
                            let entry = loaded.get_atom_entry(hash, range)?.clone();
                            Some((hash.clone(), range.clone(), entry, meta))
                        })
                        .collect();
                    self.molecule = Some(crate::atom::MoleculeHashRange::from_per_key_records(
                        loaded.uuid().to_string(),
                        loaded.version(),
                        loaded.updated_at(),
                        records,
                    ));
                }
            }
        } else {
            let narrowed = db_ops
                .atoms()
                .load_filtered_molecule_for_read(
                    molecule_uuid,
                    layout,
                    storage_prefix,
                    filter,
                    include_tombstones,
                )
                .await?;

            match narrowed {
                Some(data) if self.kind.matches_data(&data) => {
                    self.molecule = Some(data);
                }
                // Not narrowable (or no per-key header) → full materialize.
                _ => self.refresh_from_db(db_ops).await?,
            }
        }
        Ok(())
    }

    /// The resident tips of a hydrated molecule, for a completed partition read.
    fn resident_tips_of(
        molecule_uuid: &str,
        molecule: &crate::atom::MoleculeHashRange,
    ) -> Vec<crate::resident::ResidentTip> {
        molecule
            .iter_all_atoms()
            .map(|(hash, range, _)| {
                let entry = molecule.get_atom_entry(hash, range).expect("iterated tip");
                crate::resident::ResidentTip {
                    molecule_uuid: molecule_uuid.to_string(),
                    hash: hash.clone(),
                    range: crate::crypto::E2eKeys::ope_decode_range_plaintext(range)
                        .unwrap_or_else(|| range.clone()),
                    atom_uuid: entry.atom_uuid.clone(),
                    written_at: entry.written_at,
                    logical_counter: entry.logical_counter,
                    device_id: entry.lww_device().to_string(),
                    mutation_uuid: entry.mutation_uuid.clone(),
                    origin_device: entry.origin_device.clone(),
                    writer_pubkey: entry.writer_pubkey.clone(),
                    key_metadata: molecule.get_key_metadata(hash, range).cloned(),
                }
            })
            .collect()
    }

    /// A mutation acknowledged in resident-write mode has published its
    /// atom and tip to T0, while the durable per-key records used to
    /// discover an enumeration's key set may still be pending. Overlay the
    /// molecule's resident generation before applying the existing filter;
    /// using the whole resident generation avoids mixing fields as a batch's
    /// individual dirty tips become durable one by one. This is intentionally
    /// personal-namespace only: share reads
    /// have their own storage prefix and resident writes are not published
    /// under that namespace.
    fn apply_resident_overlay(
        &mut self,
        molecule_uuid: &str,
        resident_overlay: Option<ResidentOverlay>,
    ) {
        let Some((_authoritative, entries, tombstones)) = resident_overlay else {
            return;
        };
        if entries.is_empty() && tombstones.is_empty() {
            return;
        }
        let molecule = self.molecule.get_or_insert_with(|| {
            crate::atom::MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                0,
                Utc::now(),
                Vec::new(),
            )
        });
        for (hash, range) in tombstones {
            molecule.remove_atom_uuid(&hash, &range);
        }
        for (hash, range, atom_uuid) in entries {
            molecule.set_atom_uuid_from_values_unsigned(hash, range, atom_uuid);
        }
    }
}

/// `(authoritative, entries(hash, range, atom_uuid), tombstones(hash, range))`.
type ResidentOverlay = (bool, Vec<(String, String, String)>, Vec<(String, String)>);
