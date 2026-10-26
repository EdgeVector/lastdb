use super::*;
use crate::db_operations::atom_store::MoleculeRangeForm;

impl FieldVariant {
    /// Hydrate + filter into [`KeyedAtomMatch`] tuples **without** fetching
    /// atom bodies.
    ///
    /// The partition hint is derived from the **storage-form** slot (before any
    /// OPE range remapping), so body hydrate can co-locate under
    /// [`crate::atom::AtomKeyEncoding::PartitionPrefix`] without a locator hop.
    ///
    /// Used by the co-key multi-field list plan: the primary field establishes
    /// the page of keys, secondary fields contribute only UUID lookups for
    /// those keys, then a single cross-field atom batch materializes content.
    pub async fn collect_matches(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        include_tombstones: bool,
    ) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
        self.require_supported_read(filter.as_ref())?;
        captured_field_read(
            db_ops,
            self.collect_matches_captured(db_ops, filter, include_tombstones)
                .boxed(),
        )
        .await
    }

    async fn collect_matches_captured(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        include_tombstones: bool,
    ) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
        if let Some(filter) = filter.as_ref() {
            if let Some(matches) = crate::storage::disk_pack::logical_paths::core_filter::matches(
                db_ops.namespaced_store().disk_pack_runtime(),
                self.common().storage_prefix(),
                self.common().molecule_uuid().map(String::as_str),
                self.kind,
                filter,
                include_tombstones,
            )? {
                return Ok(matches);
            }
        }
        let effective_filter = filter.unwrap_or(HashRangeFilter::Page {
            offset: 0,
            limit: DEFAULT_UNFILTERED_PAGE_LIMIT,
        });

        let range_form = self
            .refresh_for_read(db_ops, &effective_filter, include_tombstones)
            .await?;

        // In-memory apply uses string compare on molecule range slots. Under
        // OPE those slots stay storage-form (see load_filtered_molecule), so
        // API plaintext prefixes like "todo#" must be rewritten to OPE space
        // first — otherwise HashRangePrefix drops every match after a correct
        // storage scan.
        let results = match self.common().molecule_uuid().map(String::as_str) {
            Some(mol_uuid) => {
                let codec = db_ops.atoms().key_codec_for_molecule(mol_uuid)?;
                let storage_filter =
                    crate::schema::types::field::filter_utils::filter_range_bounds_for_apply(
                        &effective_filter,
                        &codec,
                        mol_uuid,
                    )
                    .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                self.apply_filter(Some(storage_filter))
            }
            None => self.apply_filter(Some(effective_filter)),
        };
        let source_codec = if range_form == MoleculeRangeForm::Storage {
            let molecule = self.common().molecule_uuid().ok_or_else(|| {
                SchemaError::InvalidData("stored field range has no molecule identity".into())
            })?;
            Some((molecule, db_ops.atoms().key_codec_for_molecule(molecule)?))
        } else {
            None
        };

        let mut matches_with_meta: Vec<KeyedAtomMatch> = results
            .matches
            .into_iter()
            .map(|(storage_key, atom_uuid)| {
                // Metadata uses the transient molecule's own coordinates. Exact
                // API ranges retain their previously selected physical receipt.
                let (key_meta, writer_pubkey) = self.entry_meta_at(&storage_key);
                let kv = match source_codec.as_ref() {
                    Some((molecule, codec)) => {
                        api_key_value_from_storage(storage_key.clone(), codec, molecule)?
                    }
                    None => storage_key.clone(),
                };
                self.note_loaded_tip(db_ops, &storage_key, &kv)?;
                Ok((kv, atom_uuid, key_meta, writer_pubkey))
            })
            .collect::<Result<_, SchemaError>>()?;

        if !include_tombstones {
            matches_with_meta
                .retain(|(_, _, key_meta, _)| !key_meta.as_ref().is_some_and(|m| m.tombstoned));
        }

        Ok(matches_with_meta)
    }

    /// Hydrate one exact hash partition and return its live keys, atom bodies,
    /// and complete winner records from the same tip walk.
    ///
    /// This is a fail-closed integrity primitive. A public query can omit a
    /// dangling atom and report the skip. Aggregate repair cannot do that: an
    /// omitted row would certify an undercount.
    pub(crate) async fn collect_authoritative_hash_partition(
        &mut self,
        db_ops: &Arc<DbOperations>,
        hash: &str,
    ) -> Result<Vec<AuthoritativeFieldMatch>, SchemaError> {
        if !matches!(self.kind, FieldKind::Hash | FieldKind::HashRange) {
            return Err(SchemaError::InvalidData(
                "authoritative hash-partition read requires a Hash or HashRange field".into(),
            ));
        }
        let molecule_uuid = self.common().molecule_uuid().cloned().ok_or_else(|| {
            SchemaError::InvalidData(
                "authoritative hash-partition read requires a molecule identity".into(),
            )
        })?;
        let storage_prefix = self.common().storage_prefix().map(ToString::to_string);
        let filter = HashRangeFilter::HashKey(hash.to_string());

        self.refresh_for_read(db_ops, &filter, false).await?;
        let pending =
            self.authoritative_hash_partition_pending(db_ops, hash, &molecule_uuid, &filter)?;

        Self::hydrate_authoritative_matches(db_ops, pending, storage_prefix.as_deref()).await
    }

    /// Walk the refreshed hash partition into `(api key, tip entry)` records,
    /// with the full winner metadata from the selected molecule.
    fn authoritative_hash_partition_pending(
        &mut self,
        db_ops: &Arc<DbOperations>,
        hash: &str,
        molecule_uuid: &str,
        filter: &HashRangeFilter,
    ) -> Result<Vec<(KeyValue, crate::atom::AtomEntry)>, SchemaError> {
        let codec = db_ops.atoms().key_codec_for_molecule(molecule_uuid)?;
        let storage_filter =
            crate::schema::types::field::filter_utils::filter_range_bounds_for_apply(
                filter,
                &codec,
                molecule_uuid,
            )
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        let raw_matches = self.apply_filter(Some(storage_filter)).matches;

        let mut pending = Vec::with_capacity(raw_matches.len());
        let mut seen_api_keys = HashSet::with_capacity(raw_matches.len());
        for (storage_key, atom_uuid) in raw_matches {
            if self
                .key_metadata_at_key(&storage_key)
                .is_some_and(|metadata| metadata.tombstoned)
            {
                continue;
            }
            let api_key = match self.kind {
                FieldKind::Hash => KeyValue::new(Some(hash.to_string()), None),
                FieldKind::HashRange => {
                    let key =
                        api_key_value_from_storage(storage_key.clone(), &codec, molecule_uuid)?;
                    KeyValue::new(Some(hash.to_string()), key.range)
                }
                _ => unreachable!("field kind checked above"),
            };
            if !seen_api_keys.insert(api_key.clone()) {
                return Err(SchemaError::InvalidData(
                    "authoritative field walk mapped multiple storage slots to one API key".into(),
                ));
            }
            let entry = self
                .atom_entry_at_key(&storage_key)
                .cloned()
                .ok_or_else(|| {
                    SchemaError::InvalidData(
                        "authoritative field walk found a key without a tip entry".into(),
                    )
                })?;
            if entry.atom_uuid != atom_uuid {
                return Err(SchemaError::InvalidData(
                    "authoritative field walk found a changed tip".into(),
                ));
            }
            pending.push((api_key, entry));
        }
        Ok(pending)
    }

    /// Resolve the atom body of every pending row and reject a missing body
    /// or a tombstone at a live tip.
    async fn hydrate_authoritative_matches(
        db_ops: &Arc<DbOperations>,
        pending: Vec<(KeyValue, crate::atom::AtomEntry)>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<AuthoritativeFieldMatch>, SchemaError> {
        let uuids: Vec<&str> = pending
            .iter()
            .map(|(_, entry)| entry.atom_uuid.as_str())
            .collect();
        let atoms =
            crate::db_operations::tip_atoms::get_atoms_for_tips(db_ops, &uuids, storage_prefix)
                .await?;
        let mut authoritative = Vec::with_capacity(pending.len());
        for ((key, entry), atom) in pending.into_iter().zip(atoms) {
            if crate::atom::is_tombstone_value(atom.content()) {
                return Err(SchemaError::InvalidData(
                    "authoritative field walk found a tombstone body at a live tip".into(),
                ));
            }
            authoritative.push(AuthoritativeFieldMatch {
                key,
                entry,
                value: atom.content().clone(),
            });
        }
        Ok(authoritative)
    }
}
