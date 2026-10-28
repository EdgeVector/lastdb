use super::*;
use crate::db_operations::atom_store::counter::TipCommit;
use crate::db_operations::keep_small::MoleculeTipCounterSource;
use crate::write_preparation::measure;

impl AtomStore {
    /// Incremental budget/churn: tip bytes + bookkeeping + live-replace correction.
    ///
    /// Meter from the accepted source items without extra store reads. The
    /// old state comes from the commit: `commit.previous` holds the old tip of
    /// each changed key that had one, read by the planner inside the write
    /// gate, and `commit.atom_sizes` the size of each atom whose count
    /// changed. Header rows come from `commit.headers`: the planner merges the
    /// counters into each header and reads the stored row under its scoped
    /// key, so the meter counts the bytes it writes in each scope.
    ///
    /// Two passes on purpose. Pass one binds each molecule to the schema that
    /// owns it, using the atom the tip points at; pass two meters. A batch
    /// emits its header and other rows in whatever order the caller built
    /// them, so a single pass would attribute a molecule's tip and drop the
    /// rows that were built before it.
    pub(in super::super) fn account_keep_small_items(
        &self,
        items: &[(String, Value)],
        commit: &TipCommit,
    ) {
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
            // measured source shape).
            // Header rows are metered from `commit.headers` below.
            let Some((molecule_key_codec::MoleculeCounterRow::Tip, row_molecule)) =
                molecule_key_codec::molecule_counter_row(key)
            else {
                continue;
            };
            let new_bytes =
                crate::db_operations::keep_small::molecule_counter_row_bytes(value).unwrap_or(0);
            // Tip rows carry no schema name. The molecule they belong to
            // does, so the owner is honest rather than inferred.
            let schema = self.keep_small.molecule_schema(row_molecule);
            let old = commit.previous.get(key);
            let prev_bytes = old.map_or(0, |old| old.row_bytes);
            if let Some(new_uuid) = value
                .get("entry")
                .and_then(|e| e.get("atom_uuid"))
                .and_then(|v| v.as_str())
            {
                self.account_tip_atom(row_molecule, old, new_uuid, commit, new_bytes, prev_bytes);
            }
            self.keep_small
                .record_tip_put(schema.as_deref(), new_bytes, prev_bytes);
        }
        for header in &commit.headers {
            let schema = self.keep_small.molecule_schema(&header.molecule);
            self.keep_small.record_bookkeeping_put(
                schema.as_deref(),
                header.new_bytes,
                header.old_bytes,
            );
            self.keep_small.record_molecule_bookkeeping_put(
                &header.molecule,
                header.new_bytes,
                header.old_bytes,
            );
        }
    }

    /// Meter the atom side of one tip put: the new atom's contribution, and
    /// the old atom's when the key leaves it.
    fn account_tip_atom(
        &self,
        molecule: &str,
        old: Option<&crate::db_operations::atom_store::counter::PreviousTip>,
        new_uuid: &str,
        commit: &TipCommit,
        new_bytes: u64,
        prev_bytes: u64,
    ) {
        let meters = &self.keep_small;
        if let Some(old) = old.filter(|old| old.atom_uuid == new_uuid) {
            // Same atom: its contribution does not change. Only the row moves.
            let unchanged = MoleculeTipCounterSource {
                atom_uuid: old.atom_uuid.clone(),
                ..MoleculeTipCounterSource::default()
            };
            meters.record_molecule_tip_put(molecule, 0, 0, Some(&unchanged), new_bytes, prev_bytes);
            return;
        }
        let known = |atom: &str| {
            meters.known_atom_contribution(atom).or_else(|| {
                commit
                    .atom_sizes
                    .get(atom)
                    .map(|size| (size.atom_bytes(), size.blob_bytes()))
            })
        };
        let contribution = known(new_uuid);
        if contribution.is_none() {
            meters.mark_molecule_counters_incomplete();
        }
        let (atom_bytes, blob_bytes) = contribution.unwrap_or_default();
        // An old atom whose net count does not change (another tip of the
        // batch gains it) has no counter read; the batch's bodies list it.
        let old_source = old.and_then(|old| {
            let (atom_value_bytes, blob_reference_bytes) = known(&old.atom_uuid)?;
            Some(MoleculeTipCounterSource {
                atom_uuid: old.atom_uuid.clone(),
                atom_value_bytes,
                blob_reference_bytes,
            })
        });
        if let Some(source) = old_source.as_ref() {
            meters.replace_live_atom(new_uuid, source.atom_value_bytes);
        }
        meters.record_molecule_tip_put(
            molecule,
            atom_bytes,
            blob_bytes,
            old_source.as_ref(),
            new_bytes,
            prev_bytes,
        );
    }

    /// Persist changed keys for several molecules through one durable batch.
    ///
    /// Each molecule computes its changed `mk:` record and header. The planner
    /// writes the winning tips, the bodies of atoms that gain their first
    /// reference, and every counter change in one batch with the headers.
    ///
    /// One molecule may legitimately appear SEVERAL times here: the protein
    /// sibling fold runs once per record, so a mutation carrying N records
    /// hands one sibling molecule N single-key updates in the same batch (see
    /// `fold_protein_siblings_after_write`). Nothing is committed until the
    /// single planner call below.
    pub(crate) async fn store_molecules_changed_keys_batch(
        &self,
        molecules: &[(String, MoleculeData, std::collections::HashSet<ChangedKey>)],
        bodies: &[crate::atom::Atom],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let borrowed: Vec<(&str, &MoleculeData, &std::collections::HashSet<ChangedKey>)> =
            molecules
                .iter()
                .map(|(uuid, data, changed)| (uuid.as_str(), data, changed))
                .collect();
        self.store_molecules_changed_keys_batch_ref(&borrowed, bodies, storage_prefix)
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
        bodies: &[crate::atom::Atom],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if molecules.is_empty() {
            return Ok(());
        }

        let _molecule_locks = self
            .lock_molecules_for_store(molecules, storage_prefix)
            .await;

        let mut all_items = Vec::new();
        for (molecule_uuid, data, changed) in molecules {
            let items =
                self.changed_key_store_items(molecule_uuid, data, changed, storage_prefix)?;
            all_items.extend(items);
        }
        let tip_keys = tip_item_keys(&all_items);
        let _tip_guards = self.lock_tip_commits(&tip_keys).await;
        let winners = measure("tip_winners_us", self.retain_durable_tip_winners(all_items)).await?;
        // The conflict rows join the batch of the tips. See `conflict_record`.
        // Boxed: the store future is already deep in the node's async graph,
        // and the recorder adds a second deep subtree.
        let recorded = if Self::has_conflicts_to_record(molecules, &winners.decisions) {
            Box::pin(self.record_store_conflicts(molecules, winners.decisions, storage_prefix))
                .await?
        } else {
            Recorded::default()
        };
        let mut rows = winners.rows;
        rows.extend(recorded.rows);
        let accounted = accounted_items(&winners.tips, &rows)?;
        let commit = measure(
            "tip_commit_us",
            self.commit_tip_changes(TipChangeSet {
                scope: storage_prefix,
                tips: winners.tips,
                bodies,
                body_source: None,
                records: recorded.refs,
                releases: Vec::new(),
                rows,
                deletes: Vec::new(),
                guarded_deletes: Vec::new(),
            }),
        )
        .await
        .map_err(|e| {
            SchemaError::InvalidData(format!(
                "Failed to batch store changed keys for {} molecules: {e}",
                molecules.len()
            ))
        })?;
        self.account_stored_bodies(bodies, &commit);
        // The storage counter describes acknowledged source rows. Account only
        // after the ordered batch succeeds, so a failed write cannot create a
        // false complete value.
        self.account_keep_small_items(&accounted, &commit);
        self.keep_small
            .settle_pending_atoms(bodies.iter().map(crate::atom::Atom::uuid));
        // Debounced persist only. A per-write `flush_keep_small` wrote the
        // whole counter map per molecule store and filled one `metadata` group
        // to 39 GB in a day (2026-09-21 restart loop). In-process readers use
        // the live meters; only shutdown needs the flush.
        let _ = self.persist_keep_small().await;
        Ok(())
    }
}

/// The rows of one store as JSON, for the keep-small accounting.
pub(in crate::db_operations::atom_store) fn accounted_items(
    tips: &[TipChange],
    rows: &[(String, Value)],
) -> Result<Vec<(String, Value)>, SchemaError> {
    let mut items = Vec::with_capacity(tips.len() + rows.len());
    for tip in tips {
        if let Some(record) = &tip.new {
            items.push((
                tip.key.clone(),
                serde_json::to_value(record).map_err(|error| {
                    SchemaError::InvalidData(format!("serialize tip record: {error}"))
                })?,
            ));
        }
    }
    items.extend(rows.iter().cloned());
    Ok(items)
}

/// Guards held for their exclusion only; nothing reads them.
#[allow(dead_code)]
enum MoleculeLock {
    Append(tokio::sync::OwnedRwLockReadGuard<()>),
    Commit(tokio::sync::OwnedRwLockWriteGuard<()>),
}

impl AtomStore {
    /// Tails share append barriers. A full snapshot takes the exclusive commit
    /// lock. Acquire both in uuid order so a mixed batch cannot deadlock.
    async fn lock_molecules_for_store(
        &self,
        molecules: &[(&str, &MoleculeData, &std::collections::HashSet<ChangedKey>)],
        storage_prefix: Option<&str>,
    ) -> Vec<MoleculeLock> {
        let snapshot_uuids: std::collections::HashSet<&str> = molecules
            .iter()
            .filter(|(_, data, _)| !data.order_is_tail())
            .map(|(molecule_uuid, _, _)| *molecule_uuid)
            .collect();
        let mut uuids: Vec<String> = molecules
            .iter()
            .map(|(molecule_uuid, _, _)| (*molecule_uuid).to_string())
            .collect();
        uuids.sort_unstable();
        uuids.dedup();
        let mut locks = Vec::with_capacity(uuids.len());
        if snapshot_uuids.is_empty() {
            for guard in self.lock_molecule_appends(&uuids, storage_prefix).await {
                locks.push(MoleculeLock::Append(guard));
            }
            return locks;
        }
        for uuid in &uuids {
            if snapshot_uuids.contains(uuid.as_str()) {
                locks.push(MoleculeLock::Commit(
                    self.lock_molecule_commit(uuid, storage_prefix).await,
                ));
            } else {
                locks.push(MoleculeLock::Append(
                    self.lock_molecule_append(uuid, storage_prefix).await,
                ));
            }
        }
        locks
    }
}
