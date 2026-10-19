//! Classification and deletion of legacy key-fork candidates.

use super::super::super::atom_store::PerKeyRecord;
use super::*;

impl AtomStore {
    pub(in crate::db_operations::admin_db) async fn classify_legacy_key_fork_candidates(
        &self,
        pending: &mut Vec<LegacyKeyForkCandidate>,
        report: &mut LegacyKeyForkAudit,
        per_molecule: &mut HashMap<String, LegacyKeyForkMoleculeStat>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let candidates = std::mem::take(pending);
        let twin_keys: Vec<String> = candidates.iter().map(|c| c.twin_key.clone()).collect();
        let twins = self
            .raw()
            .get_items::<PerKeyRecord>(&twin_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("read legacy fork twins: {e}")))?;
        let mut forked = Vec::new();
        for (candidate, twin) in candidates.into_iter().zip(twins) {
            let stat = per_molecule
                .get_mut(&candidate.molecule)
                .expect("candidate molecule was counted");
            let Some(twin) = twin else {
                report.legacy_only += 1;
                stat.legacy_only += 1;
                continue;
            };
            report.forked += 1;
            stat.forked += 1;
            forked.push((candidate, twin));
        }
        if forked.is_empty() {
            return Ok(());
        }
        let eligible = self
            .judge_fork_twin_atoms(forked, report, per_molecule, storage_prefix)
            .await?;
        for (molecule, candidates) in eligible {
            self.delete_eligible_legacy_forks(
                &molecule,
                candidates,
                report,
                per_molecule,
                storage_prefix,
            )
            .await?;
        }
        Ok(())
    }

    /// Count each fork twin's atom as missing, tombstoned or live. Under
    /// execute, returns the candidates with a live twin grouped by molecule.
    async fn judge_fork_twin_atoms(
        &self,
        forked: Vec<(LegacyKeyForkCandidate, PerKeyRecord)>,
        report: &mut LegacyKeyForkAudit,
        per_molecule: &mut HashMap<String, LegacyKeyForkMoleculeStat>,
        storage_prefix: Option<&str>,
    ) -> Result<HashMap<String, Vec<LegacyKeyForkCandidate>>, SchemaError> {
        let atom_slots: Vec<_> = forked
            .iter()
            .map(|(_, twin)| twin.entry.atom_uuid.as_str())
            .collect();
        let atoms = self.get_atoms(&atom_slots, storage_prefix).await?;
        let mut eligible: HashMap<String, Vec<LegacyKeyForkCandidate>> = HashMap::new();
        for ((candidate, _), atom) in forked.into_iter().zip(atoms) {
            let stat = per_molecule
                .get_mut(&candidate.molecule)
                .expect("candidate molecule was counted");
            match atom {
                None => {
                    report.twin_atoms_missing += 1;
                    stat.twin_atoms_missing += 1;
                }
                Some(atom) if crate::atom::is_tombstone_value(atom.content()) => {
                    report.twin_atoms_tombstoned += 1;
                    stat.twin_atoms_tombstoned += 1;
                }
                Some(_) => {
                    report.twin_atoms_live += 1;
                    stat.twin_atoms_live += 1;
                    // Dry-run must still report the reclaim it already measured.
                    // Eligible is only queued under execute; without this arm
                    // `bytes_freed_approx` stays structurally pinned at 0.
                    if report.dry_run {
                        report.bytes_freed_approx += candidate.bytes;
                    } else {
                        eligible
                            .entry(candidate.molecule.clone())
                            .or_default()
                            .push(candidate);
                    }
                }
            }
        }
        Ok(eligible)
    }

    /// Re-check each twin under the molecule commit lock, then delete the
    /// legacy tips whose twin still holds a live atom.
    async fn delete_eligible_legacy_forks(
        &self,
        molecule: &str,
        candidates: Vec<LegacyKeyForkCandidate>,
        report: &mut LegacyKeyForkAudit,
        per_molecule: &mut HashMap<String, LegacyKeyForkMoleculeStat>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let _guard = self.lock_molecule_commit(molecule, storage_prefix).await;
        let twin_keys: Vec<_> = candidates.iter().map(|c| c.twin_key.clone()).collect();
        let twins = self
            .raw()
            .get_items::<PerKeyRecord>(&twin_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("recheck legacy fork twins: {e}")))?;
        let present: Vec<_> = candidates
            .into_iter()
            .zip(twins)
            .filter_map(|(candidate, twin)| twin.map(|twin| (candidate, twin)))
            .collect();
        let atom_slots: Vec<_> = present
            .iter()
            .map(|(_, twin)| twin.entry.atom_uuid.as_str())
            .collect();
        let atoms = self.get_atoms(&atom_slots, storage_prefix).await?;
        let mut delete = Vec::new();
        let mut bytes = 0u64;
        for ((candidate, _), atom) in present.into_iter().zip(atoms) {
            if atom
                .as_ref()
                .is_some_and(|atom| !crate::atom::is_tombstone_value(atom.content()))
            {
                bytes += candidate.bytes;
                delete.push(candidate.legacy_key);
            }
        }
        if delete.is_empty() {
            return Ok(());
        }
        self.raw()
            .batch_delete_keys(delete.clone())
            .await
            .map_err(|e| SchemaError::InvalidData(format!("delete legacy fork tips: {e}")))?;
        let deleted = delete.len() as u64;
        report.legacy_tips_deleted += deleted;
        report.bytes_freed_approx += bytes;
        per_molecule
            .get_mut(molecule)
            .expect("candidate molecule was counted")
            .legacy_tips_deleted += deleted;
        Ok(())
    }
}
