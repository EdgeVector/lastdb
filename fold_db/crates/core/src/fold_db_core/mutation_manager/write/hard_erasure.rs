//! Ordered durable erasure envelopes and completion ownership.

use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn enqueue_hard_erasures_on_lanes(
        &self,
        erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        missing: super::super::super::purge::PurgeMissingPolicy,
        verb: super::super::super::purge::HardEraseVerb,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<Vec<String>, SchemaError> {
        let ids: Vec<String> = erasures
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        if erasures.is_empty() {
            return Ok(ids);
        }

        let (schema_order, mut by_schema) = group_mutations_by_schema(erasures);

        let mut completions = Vec::new();
        for schema_name in schema_order {
            let schema_erasures = by_schema
                .remove(&schema_name)
                .expect("schema order came from grouped erasures");
            let receiver = self
                .enqueue_schema_purge_envelope(
                    schema_name,
                    schema_erasures,
                    storage_prefix,
                    (missing, verb),
                    author_clock_barrier.clone(),
                )
                .await?;
            completions.push(receiver);
        }

        for completion in completions {
            let completion = completion.await.map_err(|_| {
                SchemaError::InvalidData("purge persist lane stopped before completion".into())
            })?;
            crate::request_phases::add_purge_totals(&completion.phases);
            completion.result?;
        }
        Ok(ids)
    }

    /// Validate one schema's erasures and reserve its ordered durable purge.
    /// The envelope returns its durable completion receiver.
    ///
    /// The lane runs the erase inside the native write gate. The gate orders it
    /// against every writer, so the envelope takes no molecule lock and no
    /// schema barrier.
    pub(super) async fn enqueue_schema_purge_envelope(
        &self,
        schema_name: String,
        schema_erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        (missing, verb): (
            super::super::super::purge::PurgeMissingPolicy,
            super::super::super::purge::HardEraseVerb,
        ),
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<tokio::sync::oneshot::Receiver<super::super::molecules::PurgeCompletion>, SchemaError>
    {
        let (schema, mut job, purge_bytes) = self.prepare_schema_purge_job(
            &schema_name,
            schema_erasures,
            storage_prefix,
            missing,
            verb,
        )?;
        let lane_key =
            crate::resident::PersistLaneKey::new(storage_prefix.unwrap_or(""), schema_name.clone());
        let reservation = self
            .persist_lanes
            .reserve(lane_key, purge_bytes)
            .map_err(|kind| SchemaError::PersistQueueFull {
                schema: schema_name.clone(),
                kind: match kind {
                    crate::resident::PersistLaneFull::Entries => "entries".into(),
                    crate::resident::PersistLaneFull::Bytes => "bytes".into(),
                    crate::resident::PersistLaneFull::Unhealthy => "unhealthy".into(),
                },
            })?;
        let delete_publication = if verb.is_delete() {
            Some(
                self.prepare_delete_publication(&schema, &job.erasures)
                    .await?,
            )
        } else {
            None
        };

        crate::sync::capture::mark_logical_commit_published();
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let pending_task = self.pending_tasks.begin();
        job.author_clock_barrier = author_clock_barrier;
        job.completion = Some(completion);
        let envelope = crate::resident::PersistEnvelope::new(
            super::super::molecules::LanePersistJob::Purge {
                job: Box::new(job),
                _pending_task: pending_task,
            },
            purge_bytes,
        );
        reservation.fill(envelope);
        drop(delete_publication);
        Ok(receiver)
    }

    /// Prepare one schema's cleanup facts without an enqueue or a lane wait.
    pub(in crate::fold_db_core::mutation_manager) fn prepare_schema_purge_job(
        &self,
        schema_name: &str,
        erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        missing: super::super::super::purge::PurgeMissingPolicy,
        verb: super::super::super::purge::HardEraseVerb,
    ) -> Result<
        (
            crate::schema::Schema,
            super::super::molecules::PurgeEnvelope,
            u64,
        ),
        SchemaError,
    > {
        let mut schema = self
            .schema_manager
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{schema_name}' not found for purge envelope"
                ))
            })?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        for mutation in &erasures {
            super::super::super::purge::validate_purge_key_shape(
                &schema,
                schema_name,
                &mutation.key_value,
            )?;
        }
        // Count the operations before the lane takes the write gate. A request
        // that cannot fit in one batch is refused here, with the key count that
        // fits, and not after it staged part of its rows.
        if let Some(limit) = self.db_ops.batch_operation_limit() {
            super::super::super::purge::budget::check_erase_size(
                verb.is_delete(),
                erasures.len(),
                schema.runtime_fields.len(),
                limit,
            )?;
        }
        let key_values: Vec<_> = erasures
            .iter()
            .map(|mutation| mutation.key_value.clone())
            .collect();
        let retention_keys = (storage_prefix.is_none()
            && matches!(
                &schema.schema_type,
                crate::schema::types::schema::DeclarativeSchemaType::Hash
                    | crate::schema::types::schema::DeclarativeSchemaType::Single
                    | crate::schema::types::schema::DeclarativeSchemaType::HashRange
            ))
        .then_some(key_values);
        let purge_bytes = serde_json::to_vec(&erasures)
            .map_or(1, |body| body.len() as u64)
            .max(1);
        let job = super::super::molecules::PurgeEnvelope {
            author_clock_barrier: None,
            erasures,
            storage_prefix: storage_prefix.map(str::to_string),
            missing,
            validated_present: false,
            retention_keys,
            verb,
            completion: None,
            pending_completion: None,
        };
        Ok((schema, job, purge_bytes))
    }
}

/// Group mutations by schema name, keeping first-seen schema order.
pub(in crate::fold_db_core::mutation_manager) fn group_mutations_by_schema(
    mutations: Vec<Mutation>,
) -> (Vec<String>, HashMap<String, Vec<Mutation>>) {
    let mut schema_order = Vec::new();
    let mut by_schema: HashMap<String, Vec<Mutation>> = HashMap::new();
    for mutation in mutations {
        if !by_schema.contains_key(&mutation.schema_name) {
            schema_order.push(mutation.schema_name.clone());
        }
        by_schema
            .entry(mutation.schema_name.clone())
            .or_default()
            .push(mutation);
    }
    (schema_order, by_schema)
}
