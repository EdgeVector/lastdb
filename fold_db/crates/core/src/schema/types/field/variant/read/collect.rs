use super::*;

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
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
    ) -> Result<Vec<KeyedAtomMatch>, SchemaError> {
        let effective_filter = filter.or(Some(HashRangeFilter::Page {
            offset: 0,
            limit: DEFAULT_UNFILTERED_PAGE_LIMIT,
        }));

        match (as_of, effective_filter.as_ref()) {
            (None, Some(filter)) => {
                self.refresh_for_read(db_ops, filter, include_tombstones)
                    .await?;
            }
            _ => self.refresh_from_db(db_ops).await?,
        }

        if let Some(as_of) = as_of {
            self.rewind_to(db_ops, as_of).await?;
        }

        // In-memory apply uses string compare on molecule range slots. Under
        // OPE those slots stay storage-form (see load_filtered_molecule), so
        // API plaintext prefixes like "todo#" must be rewritten to OPE space
        // first — otherwise HashRangePrefix drops every match after a correct
        // storage scan.
        let results = match (
            effective_filter.as_ref(),
            self.common().molecule_uuid().map(String::as_str),
        ) {
            (Some(filter), Some(mol_uuid)) => {
                let codec = db_ops.atoms().key_codec_for_molecule(mol_uuid);
                let expanded = crate::schema::types::field::filter_utils::expand_filter_range_bounds_for_apply(
                    filter,
                    &codec,
                    mol_uuid,
                )
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
                let mut merged = std::collections::HashMap::new();
                for f in expanded {
                    let r = self.apply_filter(Some(f));
                    merged.extend(r.matches);
                }
                crate::schema::types::field::HashRangeFilterResult::new(merged)
            }
            _ => self.apply_filter(effective_filter),
        };
        let remap_ope_range = db_ops.atoms().key_codec().range_encoding().writes_ope();

        let mut matches_with_meta: Vec<KeyedAtomMatch> = results
            .matches
            .into_iter()
            .map(|(kv, atom_uuid)| {
                // Meta + partition must use storage-form range/hash (molecule keys).
                let (key_meta, writer_pubkey) = self.entry_meta_at(&kv);
                let partition = self.partition_hint(&kv);
                let kv = if remap_ope_range {
                    api_key_value_from_storage(kv)
                } else {
                    kv
                };
                (kv, atom_uuid, key_meta, writer_pubkey, partition)
            })
            .collect();

        if !include_tombstones {
            matches_with_meta
                .retain(|(_, _, key_meta, _, _)| !key_meta.as_ref().is_some_and(|m| m.tombstoned));
        }

        Ok(matches_with_meta)
    }

    /// Hydrate one exact hash partition and return its live keys, atom bodies,
    /// and complete winner records from the same tip walk.
    ///
    /// This is a fail-closed integrity primitive. A public query can omit a
    /// dangling atom and report the skip. Aggregate repair cannot do that: an
    /// omitted row would certify an undercount. Resident overlays receive the
    /// same treatment, including a key whose resident tip is absent.
    // lint:fn-size-ok moved verbatim from the original module; splitting it is a separate change
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

        // Capture the active resident generation before hydration. The normal
        // overlay intentionally installs unsigned molecule entries, so repair
        // must retain each resident tip's real convergence winner separately.
        let mut resident_live: HashMap<KeyValue, crate::resident::ResidentTip> = HashMap::new();
        let mut resident_tombstones = HashSet::new();
        if crate::db_operations::resident_read::tip_reads_resident_first(storage_prefix.as_deref())
        {
            let start = crate::resident::ResidentMoleculeKey::new(hash.to_string(), String::new());
            let end = crate::resident::ResidentMoleculeKey::new(format!("{hash}\0"), String::new());
            let snapshot =
                db_ops
                    .resident()
                    .resident_key_set_range(&molecule_uuid, Some(&start), Some(&end));
            let resident_active = matches!(
                snapshot.completeness,
                ResidentKeySetCompleteness::Complete { .. }
            ) || (db_ops.resident().molecule_has_dirty(&molecule_uuid)
                && (!snapshot.keys.is_empty() || !snapshot.tombstones.is_empty()));
            if resident_active {
                for key in snapshot.keys {
                    let tip = db_ops
                        .resident()
                        .resolve_tip(&molecule_uuid, &key.hash, &key.range)
                        .ok_or_else(|| {
                            SchemaError::InvalidData(
                                "authoritative field walk found a resident key without a tip"
                                    .into(),
                            )
                        })?
                        .value;
                    let key = match self.kind {
                        FieldKind::Hash => KeyValue::new(Some(key.hash), None),
                        FieldKind::HashRange => KeyValue::new(Some(key.hash), Some(key.range)),
                        _ => unreachable!("field kind checked above"),
                    };
                    resident_live.insert(key, tip);
                }
                resident_tombstones.extend(snapshot.tombstones.into_iter().map(
                    |key| match self.kind {
                        FieldKind::Hash => KeyValue::new(Some(key.hash), None),
                        FieldKind::HashRange => KeyValue::new(Some(key.hash), Some(key.range)),
                        _ => unreachable!("field kind checked above"),
                    },
                ));
            }
        }

        self.refresh_for_read(db_ops, &filter, false).await?;
        let codec = db_ops.atoms().key_codec_for_molecule(&molecule_uuid);
        let expanded =
            crate::schema::types::field::filter_utils::expand_filter_range_bounds_for_apply(
                &filter,
                &codec,
                &molecule_uuid,
            )
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        let mut raw_matches = HashMap::new();
        for filter in expanded {
            raw_matches.extend(self.apply_filter(Some(filter)).matches);
        }
        let remap_ope_range = db_ops.atoms().key_codec().range_encoding().writes_ope();

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
                    let key = if remap_ope_range {
                        api_key_value_from_storage(storage_key.clone())
                    } else {
                        storage_key.clone()
                    };
                    KeyValue::new(Some(hash.to_string()), key.range)
                }
                _ => unreachable!("field kind checked above"),
            };
            if !seen_api_keys.insert(api_key.clone()) {
                return Err(SchemaError::InvalidData(
                    "authoritative field walk mapped multiple storage slots to one API key".into(),
                ));
            }
            if resident_tombstones.contains(&api_key) {
                return Err(SchemaError::InvalidData(
                    "authoritative field walk retained a resident tombstone".into(),
                ));
            }
            let entry = if let Some(tip) = resident_live.get(&api_key) {
                if tip.atom_uuid != atom_uuid {
                    return Err(SchemaError::InvalidData(
                        "authoritative field walk disagrees with the resident tip".into(),
                    ));
                }
                crate::atom::AtomEntry::thin_with_author(
                    tip.atom_uuid.clone(),
                    tip.written_at,
                    tip.logical_counter,
                    tip.device_id.clone(),
                    tip.mutation_uuid.clone(),
                    String::new(),
                )
            } else {
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
                entry
            };
            pending.push((api_key, entry, self.partition_hint(&storage_key)));
        }

        let returned_keys: HashSet<KeyValue> =
            pending.iter().map(|(key, ..)| key.clone()).collect();
        if resident_live.keys().any(|key| !returned_keys.contains(key)) {
            return Err(SchemaError::InvalidData(
                "authoritative field walk omitted a live resident key".into(),
            ));
        }

        let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = pending
            .iter()
            .map(|(_, entry, partition)| (entry.atom_uuid.as_str(), partition.clone()))
            .collect();
        let atoms = crate::db_operations::resident_read::get_atoms_located_resident_first(
            db_ops,
            &slots,
            storage_prefix.as_deref(),
        )
        .await?;
        let mut authoritative = Vec::with_capacity(pending.len());
        for ((key, entry, _), atom) in pending.into_iter().zip(atoms) {
            let atom = atom.ok_or_else(|| {
                SchemaError::InvalidData(
                    "authoritative field walk found an unresolved atom body".into(),
                )
            })?;
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
