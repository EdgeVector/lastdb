//! Moved verbatim out of the parent module; see the parent for context.

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
                .enqueue_schema_purge_envelope(
                    schema_name,
                    schema_erasures,
                    storage_prefix,
                    missing,
                    verb,
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

    /// Validate one schema's erasures, reserve a persist-lane slot, apply the
    /// resident tombstones under the apply gate and fill the lane envelope.
    /// Returns the completion receiver when the caller waits for durability.
    #[allow(clippy::too_many_arguments)]
    async fn enqueue_schema_purge_envelope(
        &self,
        schema_name: String,
        schema_erasures: Vec<Mutation>,
        storage_prefix: Option<&str>,
        missing: super::super::super::purge::PurgeMissingPolicy,
        verb: super::super::super::purge::HardEraseVerb,
        wait_for_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<
        Option<tokio::sync::oneshot::Receiver<super::super::molecules::PurgeCompletion>>,
        SchemaError,
    > {
        let (schema, plan) = self
            .plan_schema_purge(&schema_name, &schema_erasures, storage_prefix, verb)
            .await?;
        let SchemaPurgePlan {
            search_batch,
            retention_keys,
            guarded_complement_retained,
            purge_bytes,
        } = plan;
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

        let delete_publication = if verb == super::super::super::purge::HardEraseVerb::Delete {
            Some(
                self.prepare_delete_publication(&schema, &schema_erasures, storage_prefix)
                    .await?,
            )
        } else {
            None
        };

        let tombstone_id = self.db_ops.resident().next_tombstone_id();
        let purge_apply = self.purge_apply_gate_transaction(
            &schema,
            &schema_erasures,
            tombstone_id,
            delete_publication.as_ref().map(|plan| &plan.winning),
        );
        crate::sync::capture::mark_logical_commit_published();
        let schema_tombstones = purge_apply.schema_tombstones;
        let slots = purge_apply.slots;
        let slot_revisions = purge_apply.slot_revisions;

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
            search_batch,
            retention_keys,
            verb,
            tombstone_id,
            schema_tombstones,
            slots,
            guarded_complement_retained,
            completion,
            pending_completion: None,
        };
        let mut envelope = crate::resident::PersistEnvelope::new(
            super::super::molecules::LanePersistJob::Purge {
                job: Box::new(job),
                _pending_task: pending_task,
            },
            purge_bytes,
        );
        envelope.slot_revisions = slot_revisions;
        drop(delete_publication);
        drop(guards);
        reservation.fill(envelope);
        Ok(receiver)
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
        schema_name: &str,
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

        const PURGE_APPLY_GATE_RETRIES: usize = 64;
        let mut prepared = self.prepared_slot_revisions(schema, &changed);
        for _ in 0..PURGE_APPLY_GATE_RETRIES {
            let guards = self.acquire_molecule_write_locks(schema, &changed).await;
            if self.prepared_revisions_match(&prepared) {
                return Ok(guards);
            }
            drop(guards);
            prepared = self.prepared_slot_revisions(schema, &changed);
        }
        Err(SchemaError::InvalidData(format!(
            "purge apply-gate revision retries exceeded for schema '{schema_name}'"
        )))
    }

    /// Dispatch hard-erasure mutations (`Purge` or `Delete`) through the
    /// destructive purge path, **grouped by schema** — one pass per schema,
    /// not one per mutation.
    ///
    /// This loop used to run `purge_record` per mutation, justified by "purge
    /// is rare and there's no batching benefit from grouping by schema". The
    /// first half was an observation about how the compliance verb was called;
    /// the second was a claim about the code, and it was false. `purge_record`
    /// redoes the whole schema's work — molecule reload, a full mutation-event
    /// read per field, and a tip-chain walk over *every per-key record* — for
    /// each record it removes. Grouping turns `O(N·F·R)` into `O(F·R)`; see
    /// [`purge_records_bulk`](super::super::super::purge::purge_records_bulk) for the
    /// full argument and for which guards had to be re-derived over the batch.
    ///
    /// Ids are returned in the caller's submitted order, which grouping makes
    /// explicit rather than incidental.
    ///
    /// # Barrier chunking
    ///
    /// The exclusive purge barrier covers snapshot-to-delete for content-
    /// addressed atoms. We cap each guarded critical section at
    /// [`PURGE_BARRIER_CHUNK`] keys and release it between chunks. This bounds
    /// one hold and lets another guarded purge use the same schema barrier.
    pub(in crate::fold_db_core::mutation_manager) async fn process_hard_erasures_durable(
        &self,
        erasures: Vec<Mutation>,
        options: HardErasureOptions<'_>,
    ) -> Result<Vec<String>, SchemaError> {
        let ids: Vec<String> = erasures.iter().map(|m| m.uuid.clone()).collect();
        // Group mutations by schema (not just keys) so we can emit Search
        // tombstones with the original mutation ids after each chunk.
        let (schema_order, mut by_schema) = group_mutations_by_schema(erasures);

        for schema_name in schema_order {
            let schema_purges = by_schema
                .remove(&schema_name)
                .expect("schema_order entries were sourced from by_schema keys");

            // Load schema once for Search outbox field allowlist (tombstones).
            let schema_for_index = self
                .schema_manager
                .get_schema_metadata(&schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' not found for purge index delivery"
                    ))
                })?;
            // Live Delete does not reclaim atoms. It must not wait on (or
            // advance) the compliance purge's reverse-edge history upgrade.
            let atom_ref_cutover = options.verb
                != super::super::super::purge::HardEraseVerb::Delete
                && super::super::super::purge::atom_ref_cutover_ready(
                    &self.db_ops,
                    &schema_for_index,
                )
                .await?;

            for chunk in schema_purges.chunks(PURGE_BARRIER_CHUNK) {
                self.process_hard_erasure_chunk(
                    &schema_name,
                    &schema_for_index,
                    chunk,
                    atom_ref_cutover,
                    &options,
                )
                .await?;
            }
        }
        Ok(ids)
    }

    /// Purge one barrier-sized chunk of a schema, record its stats, deliver
    /// the derived Search events and log the commit.
    async fn process_hard_erasure_chunk(
        &self,
        schema_name: &str,
        schema_for_index: &crate::schema::Schema,
        chunk: &[Mutation],
        atom_ref_cutover: bool,
        options: &HardErasureOptions<'_>,
    ) -> Result<(), SchemaError> {
        let verb = options.verb;
        let missing = options.missing;
        let keys: Vec<crate::schema::types::KeyValue> =
            chunk.iter().map(|m| m.key_value.clone()).collect();

        // Live `Delete` (Skip): converge disk to the resident
        // tombstone already applied at apply time. No atom
        // reachability walk and no exclusive schema barrier — the
        // persist lane's slot order is enough (invariant 4,
        // design-lastdb-delete-converge-then-reclaim). `Purge` and
        // `DeleteMustExist` are untouched below: compliance erasure
        // still wants the full atom sweep and the loud not-found
        // contract this converge path does not attempt.
        let outcome = if verb == super::super::super::purge::HardEraseVerb::Delete {
            self.converge_delete_chunk(schema_name, chunk, options.storage_prefix)
                .await?
        } else if atom_ref_cutover {
            Some(
                self.purge_chunk_by_atom_refs(schema_name, &keys, options)
                    .await?,
            )
        } else {
            Some(
                self.purge_chunk_guarded(schema_name, &keys, options)
                    .await?,
            )
        };
        let Some((report, exclusive_hold)) = outcome else {
            return Ok(());
        };
        self.record_purge(
            schema_name,
            u64::try_from(report.records_purged).unwrap_or(u64::MAX),
            exclusive_hold,
        );
        self.record_purge_path(
            schema_name,
            report.target_slots,
            report.candidate_atoms,
            report.reverse_edge_reads,
        );

        // Hard erasures are peeled off before the Create/Update
        // pipeline, so they never hit `spawn_index_mutations` there.
        // Deliver Search tombstones outside the exclusive barrier.
        // The best-effort outbox write must not extend the guarded
        // critical section.
        if options.emit_derived_events {
            self.emit_hard_erasure_derived_events(
                schema_name,
                schema_for_index,
                chunk,
                options.storage_prefix,
            )
            .await?;
        }

        info!(
            schema = %schema_name,
            records = report.records_purged,
            history_rows = report.history_rows_deleted,
            tip_versions = report.tip_versions_pruned,
            atom_rows = report.atom_rows_deleted,
            embedding_rows = report.embedding_rows_deleted,
            exclusive_hold_us = exclusive_hold.as_micros(),
            chunk_size = chunk.len(),
            verb = verb.ledger_verb(),
            policy = match missing {
                super::super::super::purge::PurgeMissingPolicy::Refuse => "refuse",
                super::super::super::purge::PurgeMissingPolicy::Skip => "skip",
            },
            "hard erasure committed",
        );
        Ok(())
    }

    /// Live `Delete` chunk. `None` means nothing traced: already converged, or
    /// never persisted before this delete arrived. It is still counted so the
    /// Purge stats block shows the call happened.
    async fn converge_delete_chunk(
        &self,
        schema_name: &str,
        chunk: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<Option<(HardErasureReport, std::time::Duration)>, SchemaError> {
        let started = std::time::Instant::now();
        let report = super::super::super::purge::converge_delete_tips(
            &self.db_ops,
            &self.schema_manager,
            schema_name,
            chunk,
            storage_prefix,
        )
        .await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeCommit,
            started.elapsed(),
        );
        if report.is_err() {
            self.record_purge(schema_name, 0, std::time::Duration::ZERO);
        }
        let report = report?;
        if report.records_purged == 0 {
            self.record_purge(schema_name, 0, std::time::Duration::ZERO);
            return Ok(None);
        }
        Ok(Some((report, std::time::Duration::ZERO)))
    }

    /// Compliance purge over the atom-ref edge manifest. No exclusive barrier.
    async fn purge_chunk_by_atom_refs(
        &self,
        schema_name: &str,
        keys: &[crate::schema::types::KeyValue],
        options: &HardErasureOptions<'_>,
    ) -> Result<(HardErasureReport, std::time::Duration), SchemaError> {
        let started = std::time::Instant::now();
        let mut acct = super::super::super::purge::PurgeCommitAccounting::default();
        let report = super::super::super::purge::purge_records_bulk(
            &self.db_ops,
            &self.schema_manager,
            schema_name,
            keys,
            options.missing,
            options.verb,
            &mut acct,
            options.evict_resident,
            super::super::super::purge::PurgeReachability::AtomRefEdges,
            None,
        )
        .await;
        let elapsed = started.elapsed();
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeCommit,
            elapsed.saturating_sub(acct.named()),
        );
        if report.is_err() {
            self.record_purge(schema_name, 0, std::time::Duration::ZERO);
        }
        Ok((report?, std::time::Duration::ZERO))
    }

    /// Compliance purge under the exclusive schema purge barrier.
    async fn purge_chunk_guarded(
        &self,
        schema_name: &str,
        keys: &[crate::schema::types::KeyValue],
        options: &HardErasureOptions<'_>,
    ) -> Result<(HardErasureReport, std::time::Duration), SchemaError> {
        // LEGACY-SUNSET: class=residue; remove this ordinary-purge
        // fallback after the open cutover records a clean real-data
        // atom-ref audit, every molecule manifest is complete after
        // one restart, and the supported Mini floor includes aref:v1.
        // Keep `purge_storage_slots_guarded` for explicit migrations.
        let barrier = self.schema_purge_barrier(schema_name);
        let acquire_start = std::time::Instant::now();
        let _purge_guard = barrier.write_owned().await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeBarrier,
            acquire_start.elapsed(),
        );
        self.record_schema_barrier_acquisition(schema_name);
        let held_from = std::time::Instant::now();
        let mut acct = super::super::super::purge::PurgeCommitAccounting::default();
        let report = super::super::super::purge::purge_records_bulk(
            &self.db_ops,
            &self.schema_manager,
            schema_name,
            keys,
            options.missing,
            options.verb,
            &mut acct,
            options.evict_resident,
            super::super::super::purge::PurgeReachability::GuardedComplement,
            options.precomputed_retained.clone(),
        )
        .await;
        let held = held_from.elapsed();
        // Attributed BEFORE `?`. A purge that errors still used
        // the guarded critical section for `held`. Dropping that
        // sample on the error path would under-report the cost.
        // `acct` is populated by reference for the same reason:
        // completed sub-steps stay visible, so the residual below
        // remains the unclassified part of the hold.
        //
        // RESIDUAL, not total: the named sub-steps are already
        // reported under their own phases, and every phase
        // surface sums its buckets as disjoint. Reporting the
        // full hold here as well would double-count the hold and
        // render every purging request as `over=`.
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeCommit,
            held.saturating_sub(acct.named()),
        );
        if report.is_err() {
            self.record_purge(schema_name, 0, held);
        }
        Ok((report?, held))
    }

    async fn emit_hard_erasure_derived_events(
        &self,
        schema_name: &str,
        schema_for_index: &crate::schema::Schema,
        chunk: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let key_values: Vec<_> = chunk.iter().map(|m| m.key_value.clone()).collect();
        self.spawn_index_mutations(schema_name, schema_for_index, chunk, &key_values);
        if storage_prefix.is_none()
            && matches!(
                &schema_for_index.schema_type,
                crate::schema::types::schema::DeclarativeSchemaType::Hash
                    | crate::schema::types::schema::DeclarativeSchemaType::Single
                    | crate::schema::types::schema::DeclarativeSchemaType::HashRange
            )
        {
            self.db_ops
                .schemas()
                .remove_schema_retention_keys(schema_name, &key_values)
                .await?;
        }
        Ok(())
    }
}

type HardErasureReport = super::super::super::purge::BulkPurgeReport;

/// Everything a purge envelope needs that is computed before the lane slot.
struct SchemaPurgePlan {
    search_batch: Option<crate::db_operations::search_index::IndexChangeBatch>,
    retention_keys: Option<Vec<crate::schema::types::KeyValue>>,
    guarded_complement_retained: Option<HashSet<String>>,
    purge_bytes: u64,
}

/// Group mutations by schema name, keeping first-seen schema order.
fn group_mutations_by_schema(
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
