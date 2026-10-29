//! Private physical stages for one native logical write.

use super::*;
use crate::write_preparation::measure;
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
        // Record writes keep the registered schema definition; only their
        // tips, values and molecule headers enter the native publication.
        measure("molecule_store_us", self.persist_deferred_molecules(job)).await?;
        if let Some(runtime) = &runtime {
            runtime.record_read_stage("molecule-stores-done", job.modified_fields.len());
        }
        // Declaration binding is best effort. Keep its existing per-write
        // retry without storing or reloading the unchanged schema catalog.
        measure(
            "schema_coherence_us",
            self.schema_manager
                .apply_field_hash_coherence_on_load(&job.schema_name),
        )
        .await?;
        measure("side_records_us", self.store_deferred_side_records(job)).await
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

        if let Some(keys) = &job.retention_write {
            self.store_deferred_retention_ages(job, keys).await?;
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

    async fn store_deferred_retention_ages(
        &self,
        job: &DeferredPersistJob,
        keys: &[crate::schema::types::KeyValue],
    ) -> Result<(), SchemaError> {
        if self
            .db_ops
            .schemas()
            .get_schema_retention_policy(&job.schema_name)
            .await?
            .is_none()
        {
            return Ok(());
        }
        // Read the final winners after the counted writer, inside this native
        // scope. A losing replay must not refresh a key's age; a revived old
        // Put must retain its original age. Include untouched fields too.
        let clocks = crate::fold_db_core::purge::retention_tip_clocks(
            &self.db_ops,
            &self.schema_manager,
            &job.schema_name,
            keys,
        )
        .await?;
        self.db_ops
            .schemas()
            .record_schema_retention_write_ages(&job.schema_name, &clocks)
            .await
    }
}
