//! Resolve user Delete peers under the same native boundary as their removal.

use super::*;
use crate::storage::disk_pack::{transaction, PackRuntime};
use futures::FutureExt;

impl MutationManager {
    /// The lane owns the native gate. A concurrent key move cannot change the
    /// source between its peer-key read and the complete destructive batch.
    pub(super) async fn attempt_current_delete_stages(
        &self,
        job: &mut PurgeEnvelope,
        runtime: &Arc<PackRuntime>,
    ) -> Result<Vec<String>, PurgeAttemptError> {
        if !transaction::publication::active(Some(Arc::clone(runtime))) {
            return Err(PurgeAttemptError::Retry(SchemaError::InvalidData(
                "Delete peer resolution requires a complete native write scope".into(),
            )));
        }
        // Caller conditions pass at the shared erasure entry point. Only the
        // caller's keys carry the must-exist requirement; peers are idempotent.
        self.validate_purge_job_targets(job).await?;
        let ids = job
            .erasures
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        let erasures = self.current_delete_targets(job).await?;
        let (order, mut grouped) = super::super::super::write::group_mutations_by_schema(erasures);
        for schema in order {
            let erasures = grouped.remove(&schema).expect("prepared Delete schema");
            let (_, mut current, _) = self
                .prepare_schema_purge_job(
                    &schema,
                    erasures,
                    job.storage_prefix.as_deref(),
                    crate::fold_db_core::purge::PurgeMissingPolicy::Skip,
                    job.verb,
                )
                .map_err(PurgeAttemptError::Retry)?;
            self.attempt_purge_stages(&mut current).await?;
            self.stage_purge_core_paths(&current, runtime)
                .boxed()
                .await
                .map_err(PurgeAttemptError::Retry)?;
        }
        Ok(ids)
    }

    async fn current_delete_targets(
        &self,
        job: &PurgeEnvelope,
    ) -> Result<Vec<crate::schema::types::Mutation>, PurgeAttemptError> {
        let scope = job.storage_prefix.as_deref();
        let erasures = self
            .expand_protein_member_deletes(job.erasures.clone(), scope)
            .await
            .map_err(target_error)?;
        // This pass also handles scoped HashRange peers and can reach an
        // extra peer through fields declared only on a first-pass sibling.
        // Purge uses it separately. Neither pass changes the source clock.
        let erasures = self
            .expand_protein_hashrange_erasures(erasures, scope)
            .await
            .map_err(target_error)?;
        let mut seen = HashSet::new();
        let erasures: Vec<_> = erasures
            .into_iter()
            .filter(|mutation| {
                seen.insert((
                    mutation.schema_name.clone(),
                    mutation.key_value.to_storage_key(),
                    mutation.uuid.clone(),
                ))
            })
            .collect();
        if erasures.len() > 10_000 {
            return Err(PurgeAttemptError::Rejected(
                SchemaError::WriteResourceLimit {
                    detail: "Delete peer expansion exceeds 10000 keys".into(),
                },
            ));
        }
        self.validate_mutation_keys_and_layouts(&erasures)
            .map_err(PurgeAttemptError::Rejected)?;
        self.reject_blocked_mutation_targets(&erasures)
            .map_err(PurgeAttemptError::Rejected)?;
        self.prepare_batch_molecule_keys(&erasures)
            .await
            .map_err(PurgeAttemptError::Retry)?;
        Ok(erasures)
    }
}

fn target_error(error: SchemaError) -> PurgeAttemptError {
    match error {
        SchemaError::ReadResourceLimit { .. }
        | SchemaError::WriteResourceLimit { .. }
        | SchemaError::InvalidField(_) => PurgeAttemptError::Rejected(error),
        error => PurgeAttemptError::Retry(error),
    }
}
