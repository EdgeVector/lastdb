use super::*;

impl AtomStore {
    /// Incremental budget/churn: tip bytes + bookkeeping + live-replace correction.
    ///
    /// Must not GET the live store. Repair tests (and LastStore wrappers)
    /// treat tip reads as observable; a metering GET would fire them.
    ///
    /// Two passes on purpose. Pass one binds each molecule to the schema that
    /// owns it, using the atom the tip points at; pass two meters. A batch
    /// emits its header and other rows in whatever order the caller built
    /// them, so a single pass would attribute a molecule's tip and drop the
    /// rows that were built before it.
    pub(in super::super) fn account_keep_small_items(&self, items: &[(String, Value)]) {
        for (key, value) in items {
            if !key.contains("mk:") {
                continue;
            }
            let Some(atom_uuid) = value
                .get("entry")
                .and_then(|e| e.get("atom_uuid"))
                .and_then(|v| v.as_str())
            else {
                continue;
            };
            let (Some(molecule_uuid), Some(schema)) = (
                molecule_key_codec::molecule_uuid_from_storage_key(key),
                self.keep_small.pending_atom_schema(atom_uuid),
            ) else {
                continue;
            };
            self.keep_small
                .remember_molecule_schema(molecule_uuid, &schema);
        }

        for (key, value) in items {
            // One classifier and one byte function shared with the bootstrap
            // measurer, so both count the same rows at the same size. The old
            // `contains("mord:")` / `contains("moc:")` test missed the
            // kind-anchored `mord\0` / `moc\0` rows (52 bytes on the
            // schema_storage_is_scan_free fixture).
            let Some((row, row_molecule)) = molecule_key_codec::molecule_counter_row(key) else {
                continue;
            };
            let new_bytes =
                crate::db_operations::keep_small::molecule_counter_row_bytes(value).unwrap_or(0);
            let prev_bytes = self.keep_small.last_key_bytes(key);
            let is_tip = row == molecule_key_codec::MoleculeCounterRow::Tip;
            // Structural rows carry no schema name. The molecule they belong
            // to does, so the owner is honest rather than inferred.
            let schema = self.keep_small.molecule_schema(row_molecule);
            if is_tip {
                if let Some(new_uuid) = value
                    .get("entry")
                    .and_then(|e| e.get("atom_uuid"))
                    .and_then(|v| v.as_str())
                {
                    let old_source = self.keep_small.last_tip_atom(key);
                    if let Some(old) = old_source.as_ref() {
                        if old.atom_uuid != new_uuid {
                            self.keep_small
                                .replace_live_atom(new_uuid, old.atom_value_bytes);
                        }
                    }
                    let contribution = self.keep_small.known_atom_contribution(new_uuid);
                    let (atom_bytes, blob_bytes) = contribution.unwrap_or_default();
                    if contribution.is_none() {
                        self.keep_small.mark_molecule_counters_incomplete();
                    }
                    self.keep_small
                        .remember_tip_atom(key, new_uuid, atom_bytes, blob_bytes);
                    self.keep_small.record_molecule_tip_put(
                        row_molecule,
                        atom_bytes,
                        blob_bytes,
                        old_source.as_ref(),
                        new_bytes,
                        prev_bytes,
                    );
                }
                self.keep_small
                    .record_tip_put(schema.as_deref(), new_bytes, prev_bytes);
                self.keep_small.remember_key_bytes(key, new_bytes);
            } else {
                self.keep_small
                    .record_bookkeeping_put(schema.as_deref(), new_bytes, prev_bytes);
                self.keep_small.record_molecule_bookkeeping_put(
                    row_molecule,
                    new_bytes,
                    prev_bytes,
                );
                self.keep_small.remember_key_bytes(key, new_bytes);
            }
        }
    }

    /// Persist only the keys that actually changed in this write, plus the
    /// `mh:{M}` header — the **O(changed)** write path. Where
    /// [`Self::store_molecule_per_key`] re-serializes every key of the molecule
    /// (O(field cardinality)), this writes one `mk:` record per touched key. The
    /// live mutation path knows exactly which keys it touched (the `FieldKey` it
    /// builds per write), so it never needs to rewrite the unchanged keys.
    ///
    /// A `changed` key that names a hash/range absent from
    /// `data` (e.g. the molecule shrank since the change set was computed) is
    /// silently skipped: removal is handled separately, this method only
    /// upserts. An empty `changed` set still rewrites the header (cheap) so a
    /// version/`updated_at`-only bump is durable.
    ///
    /// One storage batch preserves operation order and restores prior values
    /// when LastStore reports an operation failure. It uses one scoped
    /// durability barrier. LastStore does not provide WAL crash atomicity.
    pub(crate) async fn store_molecule_changed_keys(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &std::collections::HashSet<ChangedKey>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        // A tail shares the append barrier. A full snapshot stays exclusive.
        let _append_guard = if data.order_is_tail() {
            Some(
                self.lock_molecule_append(molecule_uuid, storage_prefix)
                    .await,
            )
        } else {
            None
        };
        let _commit_guard = if data.order_is_tail() {
            None
        } else {
            Some(
                self.lock_molecule_commit(molecule_uuid, storage_prefix)
                    .await,
            )
        };

        let mut items = self
            .changed_key_store_items(molecule_uuid, data, changed, storage_prefix)
            .await?;
        let guarded_automatic_gc_uuids =
            self.automatic_gc_tip_reference_uuids(&items, storage_prefix);
        let _automatic_gc_guards = self
            .lock_automatic_gc_atoms(&guarded_automatic_gc_uuids)
            .await;
        let tip_keys = tip_item_keys(&items);
        let _tip_guards = self.lock_tip_commits(&tip_keys).await;
        self.retain_durable_tip_winners(&mut items).await?;

        let automatic_gc_uuids = self.automatic_gc_tip_reference_uuids(&items, storage_prefix);
        items
            .extend(self.automatic_gc_reference_marker_items(&automatic_gc_uuids, storage_prefix)?);

        refuse_legacy_ref_blob_store_items(&items)?;
        let accounted_items = items.clone();
        self.batch_put_items_with_atom_ref_v2(items, storage_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "Failed to store changed keys for molecule {molecule_uuid}: {e}"
                ))
            })?;
        // The storage counter describes acknowledged source rows. Account only
        // after their ordered durable batch succeeds, so a failed write cannot
        // create a false complete value.
        self.account_keep_small_items(&accounted_items);
        // Debounced persist only. A per-write `flush_keep_small` here wrote
        // the whole counter map per molecule store and filled one `metadata`
        // group to 39 GB in a day (2026-09-21 restart loop). In-process
        // readers use the live meters; only shutdown needs the flush.
        let _ = self.persist_keep_small().await;
        Ok(())
    }

    /// Persist changed keys for several molecules through one durable batch.
    ///
    /// Each molecule still computes its changed `mk:` record and header exactly
    /// like [`Self::store_molecule_changed_keys`]. The durable put is shared so
    /// a multi-key protein fold does not await one store per sibling molecule.
    ///
    /// One molecule may legitimately appear SEVERAL times here: the protein
    /// sibling fold runs once per record, so a mutation carrying N records
    /// hands one sibling molecule N single-key updates in the same batch (see
    /// `fold_protein_siblings_after_write`). Nothing is committed until the
    /// single put below.
    pub(crate) async fn store_molecules_changed_keys_batch(
        &self,
        molecules: &[(String, MoleculeData, std::collections::HashSet<ChangedKey>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let borrowed: Vec<(&str, &MoleculeData, &std::collections::HashSet<ChangedKey>)> =
            molecules
                .iter()
                .map(|(uuid, data, changed)| (uuid.as_str(), data, changed))
                .collect();
        self.store_molecules_changed_keys_batch_ref(&borrowed, storage_prefix)
            .await
    }

    /// Borrowed-input form of [`Self::store_molecules_changed_keys_batch`].
    ///
    /// Nothing in the batch path needs to OWN a molecule —
    /// [`Self::changed_key_store_items`] takes `&MoleculeData` — so the owning
    /// signature above was an accident of its first caller, not a requirement.
    /// It mattered: the mutation write path holds `&MoleculeData` and cloning a
    /// molecule per write to reach the batch would cost O(molecule cardinality)
    /// on the primary's 19,225-key field molecules, i.e. more than the
    /// serialization it was trying to remove. Callers that already own their
    /// molecules keep using the owning wrapper.
    pub(crate) async fn store_molecules_changed_keys_batch_ref(
        &self,
        molecules: &[(&str, &MoleculeData, &std::collections::HashSet<ChangedKey>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if molecules.is_empty() {
            return Ok(());
        }

        // Tails share append barriers. A full snapshot takes the exclusive
        // commit lock. Acquire both in uuid order so a mixed batch cannot
        // deadlock.
        let snapshot_uuids: std::collections::HashSet<&str> = molecules
            .iter()
            .filter(|(_, data, _)| !data.order_is_tail())
            .map(|(molecule_uuid, _, _)| *molecule_uuid)
            .collect();
        let uuid_owned: Vec<String> = {
            let mut uuids: Vec<String> = molecules
                .iter()
                .map(|(molecule_uuid, _, _)| (*molecule_uuid).to_string())
                .collect();
            uuids.sort_unstable();
            uuids.dedup();
            uuids
        };
        // Guards are held for Drop only (lock exclusion), not read.
        #[allow(dead_code)]
        enum MoleculeLock {
            Append(tokio::sync::OwnedRwLockReadGuard<()>),
            Commit(tokio::sync::OwnedRwLockWriteGuard<()>),
        }
        let _append_only_guards = if snapshot_uuids.is_empty() {
            Some(
                self.lock_molecule_appends(&uuid_owned, storage_prefix)
                    .await,
            )
        } else {
            None
        };
        let mut _mixed_locks = Vec::new();
        if !snapshot_uuids.is_empty() {
            for uuid in &uuid_owned {
                if snapshot_uuids.contains(uuid.as_str()) {
                    _mixed_locks.push(MoleculeLock::Commit(
                        self.lock_molecule_commit(uuid, storage_prefix).await,
                    ));
                } else {
                    _mixed_locks.push(MoleculeLock::Append(
                        self.lock_molecule_append(uuid, storage_prefix).await,
                    ));
                }
            }
        }

        let mut all_items = Vec::new();
        for (molecule_uuid, data, changed) in molecules {
            let items = self
                .changed_key_store_items(molecule_uuid, data, changed, storage_prefix)
                .await?;
            all_items.extend(items);
        }
        let guarded_automatic_gc_uuids =
            self.automatic_gc_tip_reference_uuids(&all_items, storage_prefix);
        let _automatic_gc_guards = self
            .lock_automatic_gc_atoms(&guarded_automatic_gc_uuids)
            .await;
        let tip_keys = tip_item_keys(&all_items);
        let _tip_guards = self.lock_tip_commits(&tip_keys).await;
        self.retain_durable_tip_winners(&mut all_items).await?;

        let automatic_gc_uuids = self.automatic_gc_tip_reference_uuids(&all_items, storage_prefix);
        all_items
            .extend(self.automatic_gc_reference_marker_items(&automatic_gc_uuids, storage_prefix)?);

        refuse_legacy_ref_blob_store_items(&all_items)?;
        let accounted_items = all_items.clone();
        self.batch_put_items_with_atom_ref_v2(all_items, storage_prefix)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "Failed to batch store changed keys for {} molecules: {e}",
                    molecules.len()
                ))
            })?;
        // See the single-molecule path above. The counter never advances for
        // a source batch that the store rejected.
        self.account_keep_small_items(&accounted_items);
        // Debounced persist only; see the single-molecule path above.
        let _ = self.persist_keep_small().await;
        Ok(())
    }
}
