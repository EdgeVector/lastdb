use super::resident_overlay::ResidentOverlay;
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
            Self::partition_interval_for_read(db_ops, self.kind, storage_prefix.as_deref(), filter)
                .map(|(hash, start, end)| {
                    db_ops.resident().begin_partition_interval_read(
                        &molecule_uuid,
                        hash,
                        &start,
                        end.as_deref(),
                    )
                });

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
        db_ops: &DbOperations,
        kind: FieldKind,
        storage_prefix: Option<&str>,
        filter: &'f HashRangeFilter,
    ) -> Option<(&'f str, String, Option<String>)> {
        if matches!(kind, FieldKind::HashRange)
            && crate::db_operations::resident_read::tip_reads_resident_first(db_ops, storage_prefix)
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
            Self::partition_interval_for_read(db_ops, self.kind, storage_prefix, filter)
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
        if resident_overlay.is_some_and(|overlay| overlay.authoritative) {
            self.molecule = Some(crate::atom::MoleculeHashRange::from_per_key_records(
                molecule_uuid.to_string(),
                0,
                Utc::now(),
                Vec::new(),
            ));
        } else if matches!(filter, HashRangeFilter::SampleN(_))
            && resident_overlay.is_some_and(|overlay| !overlay.authoritative)
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
                    writer_pubkey: entry.writer_pubkey.clone(),
                    key_metadata: molecule.get_key_metadata(hash, range).cloned(),
                }
            })
            .collect()
    }
}
