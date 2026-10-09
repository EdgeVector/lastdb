//! Moved verbatim out of the parent module; see the parent for context.

use super::*;

impl MutationManager {
    /// Write-gate key for one molecule *tip slot*.
    ///
    /// The apply gate protects one short memory transaction on a tip slot.
    /// Durable order is the persist lane, not this gate. A concurrent writer
    /// to the same `(molecule, hash, range)` still takes the gate for apply.
    /// Writers to different ranges under the same hash do not share that
    /// resident tip. The durable header they share (`mh:`) is protected by
    /// AtomStore's short molecule commit guard around build+put, so this gate
    /// must not make every
    /// pack object under one `HashRange` hash wait for a previous durable put.
    ///
    /// `hash` is `None` for a molecule-wide write (an empty changed set still
    /// rewrites the header), which takes a gate no per-slot writer holds — the
    /// molecule-wide state it touches is guarded by the short commit lock in
    /// [`crate::db_operations::AtomStore::lock_molecule_commit`] instead.
    pub(super) fn molecule_persist_lock_key(
        molecule_uuid: &str,
        hash: Option<&str>,
        range: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> String {
        match (hash, range) {
            (Some(hash), Some(range)) => format!(
                "{}\u{1f}{molecule_uuid}\u{1f}{hash}\u{1f}{range}",
                storage_prefix.unwrap_or("")
            ),
            (Some(hash), None) => format!(
                "{}\u{1f}{molecule_uuid}\u{1f}{hash}\u{1f}",
                storage_prefix.unwrap_or("")
            ),
            (None, _) => format!("{}\u{1f}{molecule_uuid}", storage_prefix.unwrap_or("")),
        }
    }

    /// Slot-gate identities for one schema's changed keys.
    pub(in crate::fold_db_core::mutation_manager) fn molecule_lock_keys_for(
        schema: &Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Vec<String> {
        changed_keys
            .iter()
            .filter_map(|(field_name, keys)| {
                let field = schema.runtime_fields.get(field_name)?;
                let uuid = field.common().molecule_uuid()?;
                Some((uuid, keys))
            })
            .flat_map(|(uuid, keys)| {
                // An empty changed set still rewrites the header, and has no
                // slot to name — gate it molecule-wide.
                if keys.is_empty() {
                    return vec![Self::molecule_persist_lock_key(uuid, None, None, None)];
                }
                keys.iter()
                    .map(|ck| {
                        Self::molecule_persist_lock_key(
                            uuid,
                            Some(ck.disk_hash()),
                            Some(ck.disk_range()),
                            None,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Slot-gate identities for prepared protein-sibling tip updates.
    pub(in crate::fold_db_core::mutation_manager) fn sibling_molecule_lock_keys(
        sibling_updates: &[(String, MoleculeData, HashSet<ChangedKey>)],
    ) -> Vec<String> {
        sibling_updates
            .iter()
            .flat_map(|(molecule_uuid, _data, changed)| {
                if changed.is_empty() {
                    return vec![Self::molecule_persist_lock_key(
                        molecule_uuid,
                        None,
                        None,
                        None,
                    )];
                }
                changed
                    .iter()
                    .map(|key| {
                        Self::molecule_persist_lock_key(
                            molecule_uuid,
                            Some(key.disk_hash()),
                            Some(key.disk_range()),
                            None,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Acquire already-collected slot gates in one stable global order.
    pub(in crate::fold_db_core::mutation_manager) async fn acquire_molecule_write_lock_keys(
        &self,
        mut lock_keys: Vec<String>,
    ) -> Vec<MoleculeGateGuard> {
        // Sorted + deduped so any two writers acquire their shared slots in the
        // same order and cannot deadlock against each other.
        lock_keys.sort_unstable();
        lock_keys.dedup();

        let mut guards = Vec::with_capacity(lock_keys.len());
        let hold_stats = self.db_ops.molecule_gate_hold_stats();
        for lock_key in lock_keys {
            let mutex = {
                let mut map = self
                    .molecule_persist_locks
                    .lock()
                    .expect("molecule_persist_locks poisoned");
                // Reap unreferenced gates. Keyed per molecule this map was
                // bounded by the molecule count (~900 on the primary); keyed
                // per SLOT it is bounded by every distinct key ever written,
                // which on a long-running daemon grows without bound.
                //
                // `strong_count == 1` means only the map holds the Arc: a
                // holder or a waiter has cloned it (`lock_owned` consumes an
                // Arc), so a live gate always counts >= 2. Sweeping BEFORE the
                // insert below is what makes this safe — reaping after it could
                // evict the entry this caller is about to clone and hand two
                // writers different mutexes for one slot.
                if map.len() >= MOLECULE_PERSIST_LOCK_REAP_AT {
                    map.retain(|_, gate| Arc::strong_count(gate) > 1);
                }
                Arc::clone(map.entry(lock_key).or_default())
            };
            guards.push(MoleculeGateGuard::new(
                mutex.lock_owned().await,
                Arc::clone(&hold_stats),
            ));
        }
        guards
    }

    /// Gate concurrent writes to the same molecule *tip slot*.
    ///
    /// Per-`(molecule, hash, range)`, not per-molecule or per-hash. The
    /// invariant this protects — "a concurrent same-tip write cannot restore a
    /// pre-ack tip from
    /// store mid-flight" — is a per-key hazard: `restore_missing_molecules`
    /// loads only `changed` keys and `store_molecules_changed_unlocked` writes
    /// only `changed` keys, so a writer touching key J cannot observe key K's
    /// pre-ack tip. AtomStore's short molecule commit guard still serializes
    /// the durable shared rows during the actual put. Keying this long gate on
    /// the molecule hash made every LastgitPack object in one repo wait out the
    /// previous object's DEFERRED durable put — measured on the live primary
    /// 2026-08-03 at 53.4% of LastgitPack mutation wall time.
    pub(in crate::fold_db_core::mutation_manager) async fn acquire_molecule_write_locks(
        &self,
        schema: &Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Vec<MoleculeGateGuard> {
        self.acquire_molecule_write_lock_keys(Self::molecule_lock_keys_for(schema, changed_keys))
            .await
    }
}
