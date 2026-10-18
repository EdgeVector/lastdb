//! Moved verbatim out of the parent module; see the parent for context.

use super::erase_guards::PurgeEnqueue;
use super::*;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn enqueue_hard_erasures_on_lanes(
        &self,
        erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        missing: super::super::super::purge::PurgeMissingPolicy,
        verb: super::super::super::purge::HardEraseVerb,
        wait_for_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<Vec<String>, SchemaError> {
        let ids: Vec<String> = erasures
            .iter()
            .map(|mutation| mutation.uuid.clone())
            .collect();
        if erasures.is_empty() {
            return Ok(ids);
        }

        let erasures = self
            .expand_protein_hashrange_erasures(erasures, storage_prefix)
            .await?;

        let (schema_order, mut by_schema) = group_mutations_by_schema(erasures);

        let mut completions = Vec::new();
        for schema_name in schema_order {
            let schema_erasures = by_schema
                .remove(&schema_name)
                .expect("schema order came from grouped erasures");
            let receiver = self
                .enqueue_schema_purge_checked(
                    schema_name,
                    schema_erasures,
                    storage_prefix,
                    (missing, verb),
                    wait_for_durable,
                    author_clock_barrier.clone(),
                )
                .await?;
            if let Some(receiver) = receiver {
                completions.push(receiver);
            }
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
    /// Returns the completion receiver when the caller waits for durability.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn enqueue_schema_purge_envelope(
        &self,
        schema_name: String,
        schema_erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        missing: super::super::super::purge::PurgeMissingPolicy,
        verb: super::super::super::purge::HardEraseVerb,
        wait_for_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<PurgeEnqueue, SchemaError> {
        let (schema, plan) = self
            .plan_schema_purge(&schema_name, &schema_erasures, storage_prefix, verb)
            .await?;
        let purge_bytes = plan.purge_bytes;
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

        let guards = self
            .acquire_purge_apply_guards(&schema, &schema_name, &schema_erasures)
            .await?;

        if let Some(spared) = self
            .spared_erasures(&schema, &schema_erasures, (storage_prefix, verb))
            .await?
        {
            return Ok(PurgeEnqueue::Spared(spared, schema_erasures));
        }

        let delete_publication = if verb == super::super::super::purge::HardEraseVerb::Delete {
            Some(
                self.prepare_delete_publication(&schema, &schema_erasures)
                    .await?,
            )
        } else {
            None
        };

        crate::sync::capture::mark_logical_commit_published();
        let (completion, receiver) = if wait_for_durable {
            let (tx, rx) = tokio::sync::oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let pending_task = self.pending_tasks.begin();
        let job = super::super::molecules::PurgeEnvelope {
            author_clock_barrier,
            erasures: schema_erasures,
            storage_prefix: storage_prefix.map(str::to_string),
            missing,
            validated_present: false,
            retry_requires_finalize: false,
            search_batch: plan.search_batch,
            retention_keys: plan.retention_keys,
            verb,
            guarded_complement_retained: plan.guarded_complement_retained,
            completion,
            pending_completion: None,
        };
        let envelope = crate::resident::PersistEnvelope::new(
            super::super::molecules::LanePersistJob::Purge {
                job: Box::new(job),
                _pending_task: pending_task,
            },
            purge_bytes,
        );
        // Enqueue under the slot gates so successors cannot precede this revision.
        reservation.fill(envelope);
        drop(delete_publication);
        drop(guards);
        Ok(PurgeEnqueue::Queued(receiver))
    }

    /// Load and validate the schema, then compute everything the purge
    /// envelope needs before it occupies a persist slot.
    async fn plan_schema_purge(
        &self,
        schema_name: &str,
        schema_erasures: &[Mutation],
        storage_prefix: Option<&str>,
        verb: super::super::super::purge::HardEraseVerb,
    ) -> Result<(crate::schema::Schema, SchemaPurgePlan), SchemaError> {
        let mut schema = self
            .schema_manager
            .get_schema_metadata(schema_name)?
            .ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "Schema '{schema_name}' not found for purge envelope"
                ))
            })?;
        apply_storage_prefix_to_schema(&mut schema, storage_prefix);
        for mutation in schema_erasures {
            super::super::super::purge::validate_purge_key_shape(
                &schema,
                schema_name,
                &mutation.key_value,
            )?;
        }
        let key_values: Vec<_> = schema_erasures
            .iter()
            .map(|mutation| mutation.key_value.clone())
            .collect();
        let search_batch =
            Self::build_index_change_batch(schema_name, &schema, schema_erasures, &key_values);
        let retention_keys = (storage_prefix.is_none()
            && matches!(
                &schema.schema_type,
                crate::schema::types::schema::DeclarativeSchemaType::Hash
                    | crate::schema::types::schema::DeclarativeSchemaType::Single
                    | crate::schema::types::schema::DeclarativeSchemaType::HashRange
            ))
        .then_some(key_values);
        let serialized_purge_bytes = serde_json::to_vec(schema_erasures)
            .map_or(1, |body| body.len() as u64)
            .saturating_add(
                search_batch
                    .as_ref()
                    .and_then(|batch| serde_json::to_vec(batch).ok())
                    .map_or(0, |body| body.len() as u64),
            );
        // Walk retained chains on this thread, before occupying a persist
        // slot. The persist worker only commits. A one-record complement
        // walk on the worker froze LastgitCiStatus for ~328 s and 503'd
        // every merge writer (2026-09-04).
        // Plain Delete only converges tips. It never needs the atom-ref
        // manifest or its history upgrade before the resident tombstone.
        let atom_ref_cutover = if verb == super::super::super::purge::HardEraseVerb::Delete {
            false
        } else {
            super::super::super::purge::atom_ref_cutover_ready(&self.db_ops, &schema).await?
        };
        let guarded_complement_retained =
            if verb != super::super::super::purge::HardEraseVerb::Delete && !atom_ref_cutover {
                let keys: Vec<crate::schema::types::KeyValue> = schema_erasures
                    .iter()
                    .map(|mutation| mutation.key_value.clone())
                    .collect();
                Some(
                    super::super::super::purge::plan_guarded_complement_retained(
                        &self.db_ops,
                        &self.schema_manager,
                        schema_name,
                        &keys,
                    )
                    .await?,
                )
            } else {
                None
            };
        let purge_bytes = serialized_purge_bytes
            .saturating_add(estimate_retained_atom_set_bytes(
                guarded_complement_retained.as_ref(),
            ))
            .max(1);
        Ok((
            schema,
            SchemaPurgePlan {
                search_batch,
                retention_keys,
                guarded_complement_retained,
                purge_bytes,
            },
        ))
    }

    /// Take the molecule write locks for every slot the purge touches, and
    /// retry until the prepared slot revisions still match under the locks.
    async fn acquire_purge_apply_guards(
        &self,
        schema: &crate::schema::Schema,
        _schema_name: &str,
        schema_erasures: &[Mutation],
    ) -> Result<Vec<super::super::molecules::MoleculeGateGuard>, SchemaError> {
        let mut changed: HashMap<String, HashSet<crate::db_operations::ChangedKey>> =
            HashMap::new();
        for (field_name, field) in &schema.runtime_fields {
            let entry = changed.entry(field_name.clone()).or_default();
            for mutation in schema_erasures {
                if let Some(key) = field.changed_key_for(&mutation.key_value) {
                    entry.insert(key);
                }
            }
        }

        Ok(self.acquire_molecule_write_locks(schema, &changed).await)
    }
}

/// Everything a purge envelope needs that is computed before the lane slot.
struct SchemaPurgePlan {
    search_batch: Option<crate::db_operations::search_index::IndexChangeBatch>,
    retention_keys: Option<Vec<crate::schema::types::KeyValue>>,
    guarded_complement_retained: Option<HashSet<String>>,
    purge_bytes: u64,
}

/// Group mutations by schema name, keeping first-seen schema order.
pub(super) fn group_mutations_by_schema(
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
