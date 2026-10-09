//! Moved verbatim out of the parent module; see the parent for context.
// lint:file-size-ok verbatim move; the one oversized function is separate split work

use super::*;

impl MutationManager {
    // lint:fn-size-ok verbatim move from write.rs; splitting this function is separate work
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

        let mut schema_order = Vec::new();
        let mut by_schema: HashMap<String, Vec<Mutation>> = HashMap::new();
        for mutation in erasures {
            if !by_schema.contains_key(&mutation.schema_name) {
                schema_order.push(mutation.schema_name.clone());
            }
            by_schema
                .entry(mutation.schema_name.clone())
                .or_default()
                .push(mutation);
        }

        let mut completions = Vec::new();
        for schema_name in schema_order {
            let schema_erasures = by_schema
                .remove(&schema_name)
                .expect("schema order came from grouped erasures");
            let mut schema = self
                .schema_manager
                .get_schema_metadata(&schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' not found for purge envelope"
                    ))
                })?;
            apply_storage_prefix_to_schema(&mut schema, storage_prefix);
            for mutation in &schema_erasures {
                super::super::super::purge::validate_purge_key_shape(
                    &schema,
                    &schema_name,
                    &mutation.key_value,
                )?;
            }
            let key_values: Vec<_> = schema_erasures
                .iter()
                .map(|mutation| mutation.key_value.clone())
                .collect();
            let search_batch = Self::build_index_change_batch(
                &schema_name,
                &schema,
                &schema_erasures,
                &key_values,
            );
            let retention_keys = (storage_prefix.is_none()
                && matches!(
                    &schema.schema_type,
                    crate::schema::types::schema::DeclarativeSchemaType::Hash
                        | crate::schema::types::schema::DeclarativeSchemaType::Single
                        | crate::schema::types::schema::DeclarativeSchemaType::HashRange
                ))
            .then_some(key_values);
            let serialized_purge_bytes = serde_json::to_vec(&schema_erasures)
                .map_or(1, |body| body.len() as u64)
                .saturating_add(
                    search_batch
                        .as_ref()
                        .and_then(|batch| serde_json::to_vec(batch).ok())
                        .map_or(0, |body| body.len() as u64),
                );
            let lane_key = crate::resident::PersistLaneKey::new(
                storage_prefix.unwrap_or(""),
                schema_name.clone(),
            );
            // Walk retained chains on this thread, before occupying a persist
            // slot. The persist worker only commits. A one-record complement
            // walk on the worker froze LastgitCiStatus for ~328 s and 503'd
            // every merge writer (2026-09-04).
            let atom_ref_cutover =
                super::super::super::purge::atom_ref_cutover_ready(&self.db_ops, &schema).await?;
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
                            &schema_name,
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
            let reservation =
                self.persist_lanes
                    .reserve(lane_key, purge_bytes)
                    .map_err(|kind| SchemaError::PersistQueueFull {
                        schema: schema_name.clone(),
                        kind: match kind {
                            crate::resident::PersistLaneFull::Entries => "entries".into(),
                            crate::resident::PersistLaneFull::Bytes => "bytes".into(),
                            crate::resident::PersistLaneFull::Unhealthy => "unhealthy".into(),
                        },
                    })?;

            let mut changed: HashMap<String, HashSet<crate::db_operations::ChangedKey>> =
                HashMap::new();
            for (field_name, field) in &schema.runtime_fields {
                let entry = changed.entry(field_name.clone()).or_default();
                for mutation in &schema_erasures {
                    if let Some(key) = field.changed_key_for(&mutation.key_value) {
                        entry.insert(key);
                    }
                }
            }

            const PURGE_APPLY_GATE_RETRIES: usize = 64;
            let mut prepared = self.prepared_slot_revisions(&schema, &changed);
            let mut acquired = None;
            for _ in 0..PURGE_APPLY_GATE_RETRIES {
                let guards = self.acquire_molecule_write_locks(&schema, &changed).await;
                if self.prepared_revisions_match(&prepared) {
                    acquired = Some(guards);
                    break;
                }
                drop(guards);
                prepared = self.prepared_slot_revisions(&schema, &changed);
            }
            let guards = acquired.ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "purge apply-gate revision retries exceeded for schema '{schema_name}'"
                ))
            })?;

            let mut delete_winning_slots = None;
            let mut delete_tip_guards = None;
            if verb == super::super::super::purge::HardEraseVerb::Delete {
                let barriers = super::super::super::purge::plan_normal_delete_barriers(
                    &self.db_ops,
                    &schema,
                    &schema_erasures,
                )?;
                let tip_keys: Vec<String> = barriers
                    .iter()
                    .map(|barrier| barrier.mk_key.clone())
                    .collect();
                let tip_guards = self.db_ops.atoms().lock_tip_commits(&tip_keys).await;
                let publication_guards = self.db_ops.atoms().lock_tip_publications(&tip_keys).await;
                let mut slots_by_key: HashMap<String, Vec<(String, String, String)>> =
                    HashMap::new();
                for mutation in &schema_erasures {
                    for field in schema.runtime_fields.values() {
                        let Some(molecule_uuid) = field.common().molecule_uuid() else {
                            continue;
                        };
                        let Some((resident_hash, resident_range)) =
                            field.disk_slot_for_key(&mutation.key_value)
                        else {
                            continue;
                        };
                        let storage_key = super::super::super::purge::storage_form_key(
                            &self.db_ops,
                            field,
                            &mutation.key_value,
                        )?;
                        let Some((disk_hash, disk_range)) = field.disk_slot_for_key(&storage_key)
                        else {
                            continue;
                        };
                        let mk_key = crate::schema::types::field::build_storage_key(
                            field.common().storage_prefix(),
                            &crate::atom::molecule_key_codec::hash_range_record_key(
                                molecule_uuid,
                                &disk_hash,
                                &disk_range,
                            ),
                        );
                        slots_by_key.entry(mk_key).or_default().push((
                            molecule_uuid.clone(),
                            resident_hash,
                            resident_range,
                        ));
                    }
                }
                let mut winning = HashSet::new();
                let disk_tips = self.db_ops.atoms().delete_target_tips(&tip_keys).await?;
                for (barrier, disk_tip) in barriers.iter().zip(disk_tips) {
                    let slots = slots_by_key.get(&barrier.mk_key).ok_or_else(|| {
                        SchemaError::InvalidData(
                            "Delete barrier has no matching memory molecule key".into(),
                        )
                    })?;
                    let newer_in_memory = slots.iter().any(|(molecule_uuid, hash, range)| {
                        self.db_ops
                            .resident()
                            .resolve_tip(molecule_uuid, hash, range)
                            .is_some_and(|tip| !barrier.blocks_resident_tip(&tip.value))
                    });
                    if !newer_in_memory
                        && disk_tip
                            .as_ref()
                            .is_none_or(|tip| barrier.blocks_tip(&tip.entry))
                    {
                        winning.extend(slots.iter().cloned());
                    }
                }
                self.db_ops
                    .atoms()
                    .register_pending_delete_barriers(barriers);
                delete_winning_slots = Some(winning);
                delete_tip_guards = Some((publication_guards, tip_guards));
            }

            let tombstone_id = self.db_ops.resident().next_tombstone_id();
            let purge_apply = self.purge_apply_gate_transaction(
                &schema,
                &schema_erasures,
                tombstone_id,
                delete_winning_slots.as_ref(),
            );
            #[cfg(feature = "cloud-sync")]
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
                author_clock_barrier: author_clock_barrier.clone(),
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
            let pause_after_purge_publish: Option<(
                Arc<tokio::sync::Notify>,
                Arc<tokio::sync::Notify>,
            )> = None;
            drop(delete_tip_guards);
            drop(guards);
            if let Some((reached, release)) = pause_after_purge_publish {
                reached.notify_one();
                release.notified().await;
            }
            reservation.fill(envelope);
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
    // lint:fn-size-ok verbatim move from write.rs; splitting this function is separate work
    pub(in crate::fold_db_core::mutation_manager) async fn process_hard_erasures_durable(
        &self,
        erasures: Vec<Mutation>,
        options: HardErasureOptions<'_>,
    ) -> Result<Vec<String>, SchemaError> {
        let HardErasureOptions {
            storage_prefix,
            missing,
            verb,
            evict_resident,
            precomputed_retained,
            emit_derived_events,
        } = options;
        let db_ops_arc = Arc::clone(&self.db_ops);
        let schema_manager_arc = Arc::clone(&self.schema_manager);
        let ids: Vec<String> = erasures.iter().map(|m| m.uuid.clone()).collect();
        // Group mutations by schema (not just keys) so we can emit Search
        // tombstones with the original mutation ids after each chunk.
        let mut schema_order: Vec<String> = Vec::new();
        let mut by_schema: HashMap<String, Vec<Mutation>> = HashMap::new();
        for mutation in erasures {
            if !by_schema.contains_key(&mutation.schema_name) {
                schema_order.push(mutation.schema_name.clone());
            }
            by_schema
                .entry(mutation.schema_name.clone())
                .or_default()
                .push(mutation);
        }

        for schema_name in schema_order {
            let schema_purges = by_schema
                .remove(&schema_name)
                .expect("schema_order entries were sourced from by_schema keys");

            // Load schema once for Search outbox field allowlist (tombstones).
            let schema_for_index = schema_manager_arc
                .get_schema_metadata(&schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' not found for purge index delivery"
                    ))
                })?;
            // Live Delete does not reclaim atoms. It must not wait on (or
            // advance) the compliance purge's reverse-edge history upgrade.
            let atom_ref_cutover = verb != super::super::super::purge::HardEraseVerb::Delete
                && super::super::super::purge::atom_ref_cutover_ready(
                    &db_ops_arc,
                    &schema_for_index,
                )
                .await?;

            for chunk in schema_purges.chunks(PURGE_BARRIER_CHUNK) {
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
                let (report, exclusive_hold) =
                    if verb == super::super::super::purge::HardEraseVerb::Delete {
                        let started = std::time::Instant::now();
                        let report = super::super::super::purge::converge_delete_tips(
                            &db_ops_arc,
                            &schema_manager_arc,
                            &schema_name,
                            chunk,
                            storage_prefix,
                        )
                        .await;
                        crate::request_phases::add_phase(
                            crate::request_phases::RequestPhase::PurgeCommit,
                            started.elapsed(),
                        );
                        if report.is_err() {
                            self.record_purge(&schema_name, 0, std::time::Duration::ZERO);
                        }
                        let report = report?;
                        if report.records_purged == 0 {
                            // Nothing traced: already converged, or never
                            // persisted before this delete arrived. Counted so
                            // the Purge stats block still shows the call
                            // happened, same as the old `traces_nothing` skip.
                            self.record_purge(&schema_name, 0, std::time::Duration::ZERO);
                            continue;
                        }
                        (report, std::time::Duration::ZERO)
                    } else if atom_ref_cutover {
                        let started = std::time::Instant::now();
                        let mut acct = super::super::super::purge::PurgeCommitAccounting::default();
                        let report = super::super::super::purge::purge_records_bulk(
                            &db_ops_arc,
                            &schema_manager_arc,
                            &schema_name,
                            &keys,
                            missing,
                            verb,
                            &mut acct,
                            evict_resident,
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
                            self.record_purge(&schema_name, 0, std::time::Duration::ZERO);
                        }
                        (report?, std::time::Duration::ZERO)
                    } else {
                        // LEGACY-SUNSET: class=residue; remove this ordinary-purge
                        // fallback after the open cutover records a clean real-data
                        // atom-ref audit, every molecule manifest is complete after
                        // one restart, and the supported Mini floor includes aref:v1.
                        // Keep `purge_storage_slots_guarded` for explicit migrations.
                        let barrier = self.schema_purge_barrier(&schema_name);
                        let acquire_start = std::time::Instant::now();
                        let _purge_guard = barrier.write_owned().await;
                        crate::request_phases::add_phase(
                            crate::request_phases::RequestPhase::PurgeBarrier,
                            acquire_start.elapsed(),
                        );
                        super::super::super::purge::record_barrier_write_acquisition(&schema_name);
                        self.record_schema_barrier_acquisition(&schema_name);
                        let held_from = std::time::Instant::now();
                        let mut acct = super::super::super::purge::PurgeCommitAccounting::default();
                        let report = super::super::super::purge::purge_records_bulk(
                            &db_ops_arc,
                            &schema_manager_arc,
                            &schema_name,
                            &keys,
                            missing,
                            verb,
                            &mut acct,
                            evict_resident,
                            super::super::super::purge::PurgeReachability::GuardedComplement,
                            precomputed_retained.clone(),
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
                            self.record_purge(&schema_name, 0, held);
                        }
                        (report?, held)
                    };
                self.record_purge(
                    &schema_name,
                    u64::try_from(report.records_purged).unwrap_or(u64::MAX),
                    exclusive_hold,
                );
                self.record_purge_path(
                    &schema_name,
                    report.target_slots,
                    report.candidate_atoms,
                    report.reverse_edge_reads,
                );

                // Hard erasures are peeled off before the Create/Update
                // pipeline, so they never hit `spawn_index_mutations` there.
                // Deliver Search tombstones outside the exclusive barrier.
                // The best-effort outbox write must not extend the guarded
                // critical section.
                if emit_derived_events {
                    let key_values: Vec<_> = chunk.iter().map(|m| m.key_value.clone()).collect();
                    self.spawn_index_mutations(&schema_name, &schema_for_index, chunk, &key_values);
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
                            .remove_schema_retention_keys(&schema_name, &key_values)
                            .await?;
                    }
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
            }
        }
        Ok(ids)
    }
}
