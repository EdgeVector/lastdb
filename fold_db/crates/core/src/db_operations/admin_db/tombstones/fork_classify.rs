//! Classification of legacy key-fork candidates. Reads only.

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
        self.judge_fork_twin_atoms(forked, report, per_molecule, storage_prefix)
            .await
    }

    /// Count each fork twin's atom as missing, tombstoned or live.
    async fn judge_fork_twin_atoms(
        &self,
        forked: Vec<(LegacyKeyForkCandidate, PerKeyRecord)>,
        report: &mut LegacyKeyForkAudit,
        per_molecule: &mut HashMap<String, LegacyKeyForkMoleculeStat>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let atom_slots: Vec<_> = forked
            .iter()
            .map(|(_, twin)| twin.entry.atom_uuid.as_str())
            .collect();
        let atoms = self.get_atoms(&atom_slots, storage_prefix).await?;
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
                    report.bytes_freed_approx += candidate.bytes;
                }
            }
        }
        Ok(())
    }
}
