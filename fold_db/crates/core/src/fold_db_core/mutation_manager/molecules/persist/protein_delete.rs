//! Resolve erasure peers under the same native boundary as their removal.

use super::*;
use crate::storage::disk_pack::{transaction, PackRuntime};
use futures::FutureExt;

impl MutationManager {
    /// The lane owns the native gate. A concurrent key move cannot change the
    /// source between its peer-key read and the complete destructive batch.
    pub(super) async fn attempt_current_erasure_stages(
        &self,
        job: &mut PurgeEnvelope,
        runtime: &Arc<PackRuntime>,
    ) -> Result<Vec<String>, PurgeAttemptError> {
        if !transaction::publication::active(Some(Arc::clone(runtime))) {
            return Err(PurgeAttemptError::Retry(SchemaError::InvalidData(
                "protein erasure resolution requires a complete native write scope".into(),
            )));
        }
        // Caller conditions pass at the shared erasure entry point. Only the
        // caller's keys carry the must-exist requirement; peers are idempotent.
        let inputs = self.current_logical_erasure_inputs(job).await?;
        self.validate_purge_job_targets(job).await?;
        let ids = job
            .erasures
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        let erasures = self
            .current_erasure_targets(inputs, job.storage_prefix.as_deref())
            .await?;
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

    async fn current_erasure_targets(
        &self,
        inputs: Vec<crate::schema::types::Mutation>,
        scope: Option<&str>,
    ) -> Result<Vec<crate::schema::types::Mutation>, PurgeAttemptError> {
        if inputs.is_empty() {
            return Ok(inputs);
        }
        let erasures = self
            .expand_protein_member_deletes(inputs, scope)
            .await
            .map_err(target_error)?;
        // This pass also handles scoped HashRange peers and can reach an
        // extra peer through fields declared only on a first-pass sibling.
        // Neither pass changes the source clock. Purge reaches its reverse
        // HashRange peers here too, after the native boundary is acquired.
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

    /// A user erases logical records, not another layout's physical slots.
    /// The same current membership decision also controls conditional reads.
    async fn current_logical_erasure_inputs(
        &self,
        job: &PurgeEnvelope,
    ) -> Result<Vec<crate::schema::types::Mutation>, PurgeAttemptError> {
        let Some(first) = job.erasures.first() else {
            return Ok(Vec::new());
        };
        let schema = self
            .schema_manager
            .get_schema_metadata(&first.schema_name)
            .map_err(PurgeAttemptError::Retry)?
            .ok_or_else(|| {
                PurgeAttemptError::Retry(SchemaError::InvalidData(
                    "logical erasure schema is absent".into(),
                ))
            })?;
        let keys: Vec<_> = job
            .erasures
            .iter()
            .map(|mutation| mutation.key_value.clone())
            .collect();
        let mut membership = crate::db_operations::logical_membership::logical_key_membership(
            &self.db_ops,
            &schema,
            &keys,
            job.storage_prefix.as_deref(),
        )
        .await
        .map_err(target_error)?;
        if !job.verb.is_delete() {
            let history = crate::fold_db_core::purge::history_key_membership(
                &self.db_ops,
                &schema,
                &keys,
                job.storage_prefix.as_deref(),
            )
            .await
            .map_err(target_error)?;
            for (current, archived) in membership.iter_mut().zip(history) {
                *current |= archived;
            }
        }
        if job.missing == crate::fold_db_core::purge::PurgeMissingPolicy::Refuse
            && !job.validated_present
        {
            if let Some((mutation, _)) = job
                .erasures
                .iter()
                .zip(&membership)
                .find(|(_, member)| !**member)
            {
                return Err(PurgeAttemptError::Rejected(SchemaError::InvalidData(format!(
                    "{} target not found: schema '{}', key {} belongs to another declared key layout",
                    job.verb.ledger_verb(), mutation.schema_name, mutation.key_value.to_storage_key(),
                ))));
            }
        }
        let skipped: Vec<_> = job
            .erasures
            .iter()
            .zip(&membership)
            .filter_map(|(mutation, member)| (!member).then_some(mutation.uuid.as_str()))
            .collect();
        self.db_ops
            .atoms()
            .clear_pending_delete_barriers_of(&skipped);
        Ok(job
            .erasures
            .iter()
            .zip(membership)
            .filter(|(_, member)| *member)
            .map(|(mutation, _)| mutation.clone())
            .collect())
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
