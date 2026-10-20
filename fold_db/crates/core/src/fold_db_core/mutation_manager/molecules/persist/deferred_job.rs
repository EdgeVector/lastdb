//! Private physical stages for one native logical write.

use super::*;
use futures::FutureExt;

impl MutationManager {
    /// Produce all stored bytes in the native request's private preparation
    /// scope. This publishes no live graph tip and clears no dirty state.
    pub(in crate::fold_db_core::mutation_manager) async fn stage_native_job(
        &self,
        job: &mut DeferredPersistJob,
    ) -> Result<(), SchemaError> {
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        // The planner writes each body in the batch of the first tip that names
        // it, so no body is stored before the tips of this job.
        self.persist_deferred_molecules(job).await?;
        if let Some(runtime) = &runtime {
            runtime.record_read_stage("molecule-stores-done", job.modified_fields.len());
        }
        self.store_deferred_schema(job).await?;
        self.store_deferred_side_records(job).await
    }

    /// Persist modified molecules, then the protein sibling tip updates.
    async fn persist_deferred_molecules(
        &self,
        job: &mut DeferredPersistJob,
    ) -> Result<(), SchemaError> {
        self.persist_sibling_key_moves(job).boxed().await?;
        let bodies = job.deferred_atoms.as_deref().unwrap_or_default();
        self.persist_modified_molecules(
            &mut job.schema,
            &job.modified_fields,
            &job.share_prefixes,
            bodies,
        )
        .await
        .map_err(|error| {
            tracing::error!(
                error = %error,
                schema = %job.schema_name,
                "deferred resident-write molecule persist failed"
            );
            error
        })?;

        let prefix = job
            .schema
            .runtime_fields
            .values()
            .find_map(|field| field.common().storage_prefix())
            .map(str::to_string);
        self.persist_sibling_tip_updates(&job.sibling_updates, bodies, prefix.as_deref())
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    siblings = job.sibling_updates.len(),
                    "deferred resident-write protein sibling tip persist failed"
                );
                error
            })
    }

    /// Store the durable schema row, then refresh the in-process schema cache.
    async fn store_deferred_schema(&self, job: &DeferredPersistJob) -> Result<(), SchemaError> {
        // A request scope must not enter durable schema metadata. Keep the
        // envelope schema unchanged because a later stage can force retry.
        let mut durable_schema = job.schema.clone();
        super::super::super::helpers::clear_storage_prefix_on_schema(&mut durable_schema);
        durable_schema.sync_molecule_uuids();
        self.db_ops
            .store_schema(&job.schema_name, &durable_schema)
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    "deferred resident-write schema store failed"
                );
                error
            })?;
        self.schema_manager
            .load_schema_internal(durable_schema)
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    "deferred resident-write schema reload failed"
                );
                // The catalog row is already durable (store_schema above
                // succeeded); this reload only refreshes the in-process
                // cache. A partial failure here must not leave a stale
                // copy silently cached — evict so the next lookup
                // self-heals from the just-persisted row instead of
                // drifting until the next reload or restart.
                if let Err(evict_error) = self
                    .schema_manager
                    .evict_stale_schema_cache(&job.schema_name)
                {
                    tracing::error!(
                        error = %evict_error,
                        schema = %job.schema_name,
                        "failed to evict stale schema cache after reload failure"
                    );
                }
                error
            })
    }

    /// Idempotency and retention records of one native job.
    async fn store_deferred_side_records(
        &self,
        job: &DeferredPersistJob,
    ) -> Result<(), SchemaError> {
        if !job.idempotency_entries.is_empty() {
            self.db_ops
                .metadata()
                .batch_put_idempotency(job.idempotency_entries.clone())
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        "deferred resident-write idempotency store failed"
                    );
                    error
                })?;
        }

        if let Some((keys, written_at)) = &job.retention_write {
            self.db_ops
                .schemas()
                .record_schema_retention_writes(&job.schema_name, keys, *written_at)
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        "deferred resident-write retention store failed"
                    );
                    error
                })?;
        }

        if let Some(keys) = &job.retention_hash_partitions {
            self.db_ops
                .schemas()
                .record_schema_retention_hash_partitions(&job.schema_name, keys)
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        "deferred resident-write retention partition store failed"
                    );
                    error
                })?;
        }

        Ok(())
    }
}
