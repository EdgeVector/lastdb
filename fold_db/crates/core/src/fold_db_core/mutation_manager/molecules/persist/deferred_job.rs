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
        self.store_deferred_atoms(job, 0).await?;
        if let Some(runtime) = &runtime {
            runtime.record_read_stage(
                "atom-stores-done",
                job.deferred_atoms.as_ref().map_or(0, Vec::len),
            );
        }
        self.persist_deferred_molecules(job, 0).await?;
        if let Some(runtime) = &runtime {
            runtime.record_read_stage("molecule-stores-done", job.modified_fields.len());
        }
        self.settle_deferred_atom_births(job, 0).await?;
        if let Some(runtime) = &runtime {
            runtime.record_read_stage(
                "birth-settle-done",
                job.deferred_atoms.as_ref().map_or(0, Vec::len),
            );
        }
        self.store_deferred_schema(job, 0).await?;
        let search = job.search_batch.take();
        let result = self.store_deferred_side_records(job, 0).await;
        job.search_batch = search;
        result
    }

    /// Keep the Atom batch private until every stage succeeds.
    async fn store_deferred_atoms(
        &self,
        job: &DeferredPersistJob,
        commit_id: u64,
    ) -> Result<(), SchemaError> {
        let Some(located) = job.deferred_atoms.as_deref().filter(|l| !l.is_empty()) else {
            return Ok(());
        };
        // The tip phase of this job writes each atom's count and cancels its GC
        // candidate, so the birth rows are not written here. The settle step
        // after the tip phase covers an atom that no tip took.
        self.db_ops
            .atoms()
            .batch_store_atoms_located_tip_follows(located, job.storage_prefix.as_deref())
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    commit_id,
                    "deferred resident-write atom store failed"
                );
                error
            })?;

        if job.storage_prefix.is_none() {
            for share_prefix in &job.share_prefixes {
                self.db_ops
                    .atoms()
                    .batch_store_atoms_located_borrowed(located, Some(share_prefix))
                    .await?;
            }
        }
        Ok(())
    }

    /// Give every atom that no tip took its birth rows, so it stays reclaimable.
    async fn settle_deferred_atom_births(
        &self,
        job: &DeferredPersistJob,
        commit_id: u64,
    ) -> Result<(), SchemaError> {
        let Some(located) = job.deferred_atoms.as_deref().filter(|l| !l.is_empty()) else {
            return Ok(());
        };
        self.db_ops
            .atoms()
            .settle_atom_births(located, job.storage_prefix.as_deref())
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    commit_id,
                    "deferred resident-write atom birth settle failed"
                );
                error
            })
    }

    /// Persist modified molecules, then the protein sibling tip updates.
    async fn persist_deferred_molecules(
        &self,
        job: &mut DeferredPersistJob,
        commit_id: u64,
    ) -> Result<(), SchemaError> {
        let mut timing = std::collections::HashMap::new();
        self.persist_sibling_key_moves(job).boxed().await?;
        self.persist_modified_molecules(
            &mut job.schema,
            &job.modified_fields,
            &job.mutation_events,
            &job.share_prefixes,
            &mut timing,
        )
        .await
        .map_err(|error| {
            tracing::error!(
                error = %error,
                schema = %job.schema_name,
                commit_id,
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
        self.persist_sibling_tip_updates(&job.sibling_updates, prefix.as_deref())
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    schema = %job.schema_name,
                    siblings = job.sibling_updates.len(),
                    commit_id,
                    "deferred resident-write protein sibling tip persist failed"
                );
                error
            })
    }

    /// Store the durable schema row, then refresh the in-process schema cache.
    async fn store_deferred_schema(
        &self,
        job: &DeferredPersistJob,
        commit_id: u64,
    ) -> Result<(), SchemaError> {
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
                    commit_id,
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
                    commit_id,
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
                        commit_id,
                        "failed to evict stale schema cache after reload failure"
                    );
                }
                error
            })
    }

    /// Idempotency, retention and Search outbox records of one deferred job.
    async fn store_deferred_side_records(
        &self,
        job: &DeferredPersistJob,
        commit_id: u64,
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
                        commit_id,
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
                        commit_id,
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
                        commit_id,
                        "deferred resident-write retention partition store failed"
                    );
                    error
                })?;
        }

        if let Some(batch) = &job.search_batch {
            self.deliver_index_change_batch(batch)
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = %error,
                        schema = %job.schema_name,
                        commit_id,
                        "schema-lane Search outbox delivery failed"
                    );
                    error
                })?;
        }
        Ok(())
    }
}
