use super::collector::Candidates;
use super::*;
use crate::atom::file_pointer::blob_refs_of_atom;
use crate::storage::disk_pack::logical_capture::{preflight_outcome_size, AcceptedFieldOutcome};

impl MutationManager {
    /// Data, original outcomes, and the device head enter one native commit.
    pub(in crate::fold_db_core::mutation_manager) async fn finish_logical_capture(
        &self,
        runtime: &Arc<PackRuntime>,
        identities: &[String],
        candidates: Candidates,
    ) -> Result<(), SchemaError> {
        if !runtime.logical_capture_enabled() {
            return Ok(());
        }
        let outcomes = self.certify_logical_outcomes(runtime, candidates).await?;
        runtime.stage_logical_capture(identities, outcomes).await?;
        Ok(())
    }

    async fn certify_logical_outcomes(
        &self,
        runtime: &Arc<PackRuntime>,
        candidates: Candidates,
    ) -> Result<Vec<AcceptedFieldOutcome>, SchemaError> {
        let keys: Vec<String> = candidates.keys().cloned().collect();
        // Read private stored rows directly. The serving model may contain the
        // prospective field, which is not proof that the final store accepted it.
        let records = self.db_ops.atoms().delete_target_tips(&keys).await?;
        let barriers = self.db_ops.atoms().winning_delete_barriers(&keys).await?;
        if records.len() != candidates.len() {
            return Err(unsupported("an incomplete tip certification"));
        }
        let mut accepted = Vec::new();
        for ((key, candidate), actual) in candidates.into_iter().zip(records) {
            let mut candidate = candidate.outcome;
            let Some(actual) = actual else {
                continue;
            };
            if barriers
                .get(&key)
                .is_some_and(|barrier| barrier.blocks_tip(&actual.entry))
            {
                continue;
            }
            if actual.entry.lww_key() != candidate.tip.lww_key() {
                continue;
            }
            candidate.tip = actual.entry;
            candidate.metadata = actual.meta;
            accepted.push(candidate);
        }
        let body_keys: Vec<String> = accepted
            .iter()
            .map(|outcome| {
                self.db_ops
                    .atoms()
                    .body_storage_key(&outcome.tip.atom_uuid, None)
            })
            .collect();
        let atoms = runtime.logical_capture_atoms(body_keys).await?;
        if atoms.len() != accepted.len() {
            return Err(unsupported("an incomplete atom certification"));
        }
        let mut total_bytes = 0usize;
        for (outcome, atom) in accepted.iter_mut().zip(atoms) {
            if atom.uuid() != outcome.tip.atom_uuid {
                return Err(unsupported("an atom identity mismatch"));
            }
            if !blob_refs_of_atom(atom.content(), atom.metadata()).is_empty()
                || outcome.metadata.as_ref().is_some_and(|metadata| {
                    !blob_refs_of_atom(atom.content(), metadata.metadata.as_ref()).is_empty()
                })
            {
                return Err(unsupported("a stored file blob reference"));
            }
            outcome.atom = atom;
            total_bytes = total_bytes.saturating_add(preflight_outcome_size(outcome)?);
            if total_bytes > 1024 * 1024 {
                return Err(unsupported("certified field outcomes above 1 MiB"));
            }
        }
        Ok(accepted)
    }
}
