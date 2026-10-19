//! Tombstone candidate classification.

use super::super::super::atom_store::PerKeyRecord;
use super::*;

impl AtomStore {
    /// Resolve one batch of flag-says-live keys and classify each by atom
    /// content. Drains `pending`. Reads only: no tip record is rewritten.
    pub(in crate::db_operations::admin_db) async fn classify_tombstone_candidates(
        &self,
        pending: &mut Vec<(String, String, PerKeyRecord)>,
        report: &mut TombstoneFlagBackfillReport,
        per_molecule: &mut HashMap<String, MoleculeTombstoneFlagStat>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let slots: Vec<&str> = pending
            .iter()
            .map(|(_, _, rec)| rec.entry.atom_uuid.as_str())
            .collect();
        let atoms = self.get_atoms(&slots, storage_prefix).await?;
        report.audit.atoms_fetched += atoms.len() as u64;

        for ((_, molecule, _), atom) in std::mem::take(pending).into_iter().zip(atoms) {
            let stat =
                per_molecule
                    .entry(molecule.clone())
                    .or_insert_with(|| MoleculeTombstoneFlagStat {
                        molecule,
                        ..Default::default()
                    });
            let Some(atom) = atom else {
                report.audit.atoms_missing += 1;
                stat.atoms_missing += 1;
                continue;
            };
            if !crate::atom::is_tombstone_value(atom.content()) {
                report.audit.live += 1;
                continue;
            }
            report.audit.content_tombstoned_meta_false += 1;
            stat.content_tombstoned_meta_false += 1;
        }
        Ok(())
    }
}
