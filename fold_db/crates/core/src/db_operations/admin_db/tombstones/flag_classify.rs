//! Stamp draining and tombstone candidate classification.

use super::super::super::atom_store::PerKeyRecord;
use super::*;

impl AtomStore {
    /// Write queued tombstone-flag stamps in `chunk`-sized batches.
    ///
    /// `min_queued` is the backlog below which this is a no-op — pass the chunk
    /// size mid-loop to write only full batches, and `0` at the end of a pass to
    /// flush whatever is left. Stamps are idempotent (they set a flag the atom
    /// content already implies), so writing them progressively rather than in
    /// one trailing burst changes nothing about the result and keeps the pass's
    /// wall-clock bound honest.
    pub(in crate::db_operations::admin_db) async fn drain_tombstone_stamps(
        &self,
        to_stamp: &mut Vec<(String, Value)>,
        report: &mut TombstoneFlagBackfillReport,
        min_queued: usize,
    ) -> Result<(), SchemaError> {
        while to_stamp.len() > min_queued && !to_stamp.is_empty() {
            let take = to_stamp.len().min(min_queued.max(1000));
            let chunk: Vec<(String, Value)> = to_stamp.drain(..take).collect();
            let n = chunk.len() as u64;
            self.raw()
                .batch_put_items(chunk)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("stamp tombstone flag put: {e}")))?;
            report.keys_stamped += n;
        }
        Ok(())
    }

    /// Resolve one batch of flag-says-live keys and classify each by atom
    /// content. Drains `pending`; queues rewrites into `to_stamp` when stamping.
    pub(in crate::db_operations::admin_db) async fn classify_tombstone_candidates(
        &self,
        pending: &mut Vec<(String, String, PerKeyRecord)>,
        report: &mut TombstoneFlagBackfillReport,
        per_molecule: &mut HashMap<String, MoleculeTombstoneFlagStat>,
        mut to_stamp: Option<&mut Vec<(String, Value)>>,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if pending.is_empty() {
            return Ok(());
        }
        let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = pending
            .iter()
            .map(|(_, _, rec)| (rec.entry.atom_uuid.as_str(), None))
            .collect();
        let atoms = self.get_atoms_located(&slots, storage_prefix).await?;
        report.audit.atoms_fetched += atoms.len() as u64;

        for ((key, molecule, mut rec), atom) in std::mem::take(pending).into_iter().zip(atoms) {
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
            let Some(queue) = to_stamp.as_mut() else {
                continue;
            };
            rec.meta.get_or_insert_with(Default::default).tombstoned = true;
            let value = serde_json::to_value(&rec)
                .map_err(|e| SchemaError::InvalidData(format!("serialize stamped tip: {e}")))?;
            queue.push((key, value));
        }
        Ok(())
    }
}
