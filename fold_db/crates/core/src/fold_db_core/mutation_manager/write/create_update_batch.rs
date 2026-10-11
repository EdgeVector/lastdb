//! Create / Update batch pipeline, one helper per phase.

use super::super::molecules::PreparedSchemaDelta;
use super::*;

/// How many times the apply gate re-prepares after a concurrent writer moves a
/// slot revision before the batch gives up.
const APPLY_GATE_RETRIES: usize = 64;

type PersistCompletion = (
    String,
    tokio::sync::oneshot::Receiver<crate::request_phases::RequestCounts>,
);

/// A protein sibling tip that must not lose to a newer Delete:
/// (delta index, sibling index, changed key, final storage key, entry).
type SiblingCandidate = (
    usize,
    usize,
    crate::db_operations::ChangedKey,
    String,
    crate::atom::AtomEntry,
);

impl MutationManager {
    pub(super) async fn write_create_update_batch_in_log(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        require_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if mutations.is_empty() {
            return Ok(ResidentCommitReceipt::empty());
        }
        // Replay and an explicit synchronous request wait for the complete
        // schema-lane envelope. Ordinary writes keep the accepted resident-ack
        // contract; LastStore restores a failed multi-op durable attempt.
        let force_durable = require_durable
            || mutations
                .iter()
                .any(|mutation| mutation.synchronous == Some(true));
        let mut operations = Self::count_requested_operations(&mutations);
        let mut stages = ResidentCommitStages::default();

        // Phase -1: Reject historical derived writes before anything else
        // touches them. No live writer is allowed to mint
        // `Provenance::Derived`; keeping the check here prevents forged or
        // stale derived writes from polluting atoms. User mutations
        // (`Provenance::User` or `None`) are untouched.
        for mutation in &mutations {
            validate_derived_provenance(mutation, &self.schema_manager)?;
        }

        self.reject_blocked_mutation_targets(&mutations)?;

        // Create/update do not wait on the schema-wide purge barrier. Janitor
        // purge is later (mixed batches enqueue; this path never runs
        // process_hard_erasures). Last writer wins. Compliance Purge keeps
        // exclusive on its own request. Recording the phase as ~0 keeps
        // request-ops comparable.
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeBarrier,
            std::time::Duration::ZERO,
        );

        // Phase 0.5a: Compare-and-set lock acquisition (same-node atomicity).
        //
        // For every mutation carrying a `CasExpectation`, take the per-key CAS
        // lock before either the idempotency filter or the precondition check
        // below runs. The `_cas_guards` bindings hold those locks until this
        // function returns, so no other same-key writer can read-modify-write
        // in between our compare and our commit — that window is exactly
        // where a naive check-then-set silently loses a concurrent writer. A
        // batch with no CAS mutations acquires nothing and is byte-for-byte
        // unchanged.
        let lock_wait_start = std::time::Instant::now();
        let _cas_guards = self.acquire_cas_locks(&mutations).await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::LockWait,
            lock_wait_start.elapsed(),
        );

        let batch_len = mutations.len();
        tracing::info!(
            "write_mutations_batch_async: Starting batch of {} mutations",
            batch_len
        );
        tracing::debug!(
            "DEBUG: MutationManager::write_mutations_batch_async started for {} mutations",
            batch_len
        );

        let start_time = std::time::Instant::now();
        let mut timing_breakdown = std::collections::HashMap::new();
        // Accumulated across schema groups; reported once as `LockWait` and
        // excluded from the residual `apply` bucket. See the acquisition site.
        let mut molecule_lock_wait = std::time::Duration::ZERO;

        // Phase 1: Filter out already-processed mutations BEFORE the CAS
        // precondition check below. A byte-identical retry of an applied
        // CAS-guarded mutation must be recognized as a safe no-op here — the
        // CAS check compares against the head the first attempt already
        // advanced, so running it before this filter rejects the retry as a
        // conflict for exactly the write type ("same-node atomicity") whose
        // contract depends on safe retries. This filter and the CAS check
        // below both still run while `_cas_guards` is held, so the
        // same-node atomicity invariant is unchanged for every mutation that
        // survives the filter.
        let idem_start = std::time::Instant::now();
        let (already_seen_ids, new_mutations, new_hashes) = self
            .filter_idempotent_mutations(mutations, storage_prefix)
            .await?;
        Self::add_timing(
            &mut timing_breakdown,
            "idempotency_check",
            idem_start.elapsed(),
        );

        if new_mutations.is_empty() {
            return self
                .finish_all_duplicate_batch(
                    already_seen_ids,
                    force_durable,
                    &timing_breakdown,
                    start_time,
                    stages,
                )
                .await;
        }

        self.check_batch_preconditions(&new_mutations, storage_prefix)
            .await?;

        tracing::debug!(
            "Idempotency: {} new, {} duplicates",
            new_mutations.len(),
            already_seen_ids.len()
        );

        let mut prepared_deltas = self
            .prepare_grouped_deltas(
                new_mutations,
                new_hashes,
                storage_prefix,
                force_durable,
                &mut timing_breakdown,
                &mut stages,
            )
            .await?;

        let persist_completions = self
            .apply_prepared_deltas(
                &mut prepared_deltas,
                storage_prefix,
                require_durable,
                author_clock_barrier,
                &mut timing_breakdown,
                &mut molecule_lock_wait,
                &mut stages,
            )
            .await?;

        let mut mutation_ids = Vec::new();
        for delta in &prepared_deltas {
            for mutation in &delta.schema_mutations {
                mutation_ids.push(mutation.uuid.clone());
            }
        }
        Self::await_persist_completions(persist_completions).await?;

        // Phase 8: apply the optional caller durability barrier. A failed
        // barrier cannot produce a Durable receipt.
        self.finalize_batch(&mut timing_breakdown, force_durable)
            .await?;

        let total_time = start_time.elapsed();
        stages.gate_wait_us = crate::request_phases::duration_us(molecule_lock_wait);
        stages.ack_us = crate::request_phases::duration_us(total_time);

        // Combine already-seen ids with newly processed mutation ids
        let mut all_ids = already_seen_ids;
        all_ids.extend(mutation_ids.iter().cloned());
        // Duplicates the filter removed. The per-verb counts above describe
        // the requested batch, so this is additive, not a correction.
        let duplicates = (all_ids.len() - mutation_ids.len()) as u64;
        operations.no_op = operations.no_op.saturating_add(duplicates);

        tracing::info!(
            "write_mutations_batch_async: Completed {} mutations ({} new, {} cached) in {:.2}ms",
            all_ids.len(),
            mutation_ids.len(),
            all_ids.len() - mutation_ids.len(),
            total_time.as_millis()
        );

        Self::log_timing_breakdown(&timing_breakdown, total_time);
        Self::report_phase_totals(&timing_breakdown, total_time, molecule_lock_wait);
        Self::report_resident_commit_counts(&operations);

        Ok(ResidentCommitReceipt {
            mutation_ids: all_ids,
            // Stamped by the caller batch entry point, which is the only
            // scope that knows this pipeline run is one logical operation —
            // a mixed batch calls this function once and peels erasures
            // around it.
            revision: None,
            operations,
            // The default path returns after the resident commit; the
            // per-schema FIFO lane writes the envelope afterwards. Anything
            // stronger would be a claim about disk this function did not wait
            // for.
            durability: Self::commit_durability(force_durable),
            cloud: None,
            stages,
            touched_group_ids: crate::durable_flush::current_touched_groups(),
        })
    }

    fn commit_durability(force_durable: bool) -> ResidentDurability {
        if force_durable {
            ResidentDurability::Durable
        } else {
            ResidentDurability::Queued
        }
    }

    /// Per-class counts are taken from the verbs the CALLER sent, before the
    /// idempotency filter runs, so `created + updated` describes the batch
    /// that was asked for. The duplicates it removes are reported separately
    /// as `no_op` rather than silently shrinking these.
    fn count_requested_operations(mutations: &[Mutation]) -> ResidentCommitOperations {
        let mut operations = ResidentCommitOperations::default();
        for mutation in mutations {
            match mutation.mutation_type {
                crate::schema::types::MutationType::Create => operations.created += 1,
                crate::schema::types::MutationType::Update => operations.updated += 1,
                crate::schema::types::MutationType::Delete
                | crate::schema::types::MutationType::Purge => operations.deleted += 1,
            }
        }
        operations
    }

    /// Receipt for a batch whose every mutation the idempotency filter
    /// recognised as already applied.
    async fn finish_all_duplicate_batch(
        &self,
        already_seen_ids: Vec<String>,
        force_durable: bool,
        timing_breakdown: &HashMap<&str, std::time::Duration>,
        start_time: std::time::Instant,
        mut stages: ResidentCommitStages,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        if force_durable {
            self.db_ops.flush().await.map_err(|error| {
                SchemaError::InvalidData(format!("forced durable duplicate flush failed: {error}"))
            })?;
        }
        tracing::info!(
            "All {} mutations were idempotency duplicates, skipping processing",
            already_seen_ids.len()
        );
        // The idempotency scan was real work; report it even though the
        // batch turned out to be all duplicates. No schema group ran, so
        // no molecule locks were taken.
        Self::report_phase_totals(
            timing_breakdown,
            start_time.elapsed(),
            std::time::Duration::ZERO,
        );
        // Every requested operation was recognised as already applied, so
        // the whole batch moves to `no_op`. The commit still counts: it
        // paid a real idempotency scan, and reporting zero here would make
        // a duplicate storm look like no traffic at all.
        let duplicates = already_seen_ids.len() as u64;
        let operations = ResidentCommitOperations {
            no_op: duplicates,
            ..Default::default()
        };
        stages.ack_us = crate::request_phases::duration_us(start_time.elapsed());
        Self::report_resident_commit_counts(&operations);
        Ok(ResidentCommitReceipt {
            mutation_ids: already_seen_ids,
            revision: None,
            operations,
            durability: Self::commit_durability(force_durable),
            cloud: None,
            stages,
            touched_group_ids: crate::durable_flush::current_touched_groups(),
        })
    }

    /// Phase 0.5b: Compare-and-set gate (same-node atomicity).
    ///
    /// Verify each surviving mutation's precondition against the CURRENT
    /// persisted head before the write pipeline runs. Already-seen
    /// duplicates were filtered out earlier and never reach this check, so
    /// a safe retry of an applied CAS-guarded mutation cannot be rejected
    /// as a conflict against a head its own earlier attempt already
    /// produced.
    ///
    /// Reported separately from the lock wait: this is storage IO (one head
    /// read per surviving CAS mutation), not queueing, and the two have
    /// opposite remedies. The same window also carries the `must_exist`
    /// update gate. It is the same class of work — one persisted-head read per
    /// guarded mutation, under the same locks — so it shares the phase rather
    /// than minting a second name for the same number.
    async fn check_batch_preconditions(
        &self,
        new_mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let cas_precondition_start = std::time::Instant::now();
        let cas_result = match self
            .check_cas_preconditions(new_mutations, storage_prefix)
            .await
        {
            Ok(()) => {
                self.check_must_exist_preconditions(new_mutations, storage_prefix)
                    .await
            }
            Err(error) => Err(error),
        };
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::CasPrecondition,
            cas_precondition_start.elapsed(),
        );
        // Recorded before the error returns so a rejected batch still reports
        // what its precondition check cost — a CAS-conflict storm is exactly
        // when the operator needs this number.
        cas_result
    }

    /// Group by schema and prepare one resident delta per group.
    async fn prepare_grouped_deltas(
        &self,
        new_mutations: Vec<Mutation>,
        new_hashes: Vec<String>,
        storage_prefix: Option<&str>,
        force_durable: bool,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
        stages: &mut ResidentCommitStages,
    ) -> Result<Vec<PreparedSchemaDelta>, SchemaError> {
        // Keep each idempotency decision with the schema envelope that owns
        // its data. A detached put can pass a failed lane head and make a
        // client retry skip data that never reached storage.
        let mut hash_by_uuid: HashMap<String, String> = new_mutations
            .iter()
            .zip(new_hashes.iter())
            .map(|(mutation, hash)| (mutation.uuid.clone(), hash.clone()))
            .collect();

        // Group mutations by schema to minimize schema reloads. First-seen
        // order is the prepare order; slot gates sort independently.
        let group_start = std::time::Instant::now();
        let grouped_mutations = self.group_mutations_by_schema(new_mutations);
        Self::add_timing(timing_breakdown, "grouping", group_start.elapsed());

        let mut prepared_deltas = Vec::with_capacity(grouped_mutations.len());
        let prepare_started = std::time::Instant::now();
        for (schema_name, schema_mutations) in grouped_mutations {
            let idempotency_entries = schema_mutations
                .iter()
                .map(|mutation| {
                    let hash = hash_by_uuid
                        .remove(&mutation.uuid)
                        .expect("new mutation has a matching idempotency hash");
                    (
                        crate::schema::types::field::build_storage_key(
                            storage_prefix,
                            &format!("idem:{hash}"),
                        ),
                        mutation.uuid.clone(),
                    )
                })
                .collect();
            let owner = self
                .schema_manager
                .get_schema_metadata(&schema_name)?
                .and_then(|schema| schema.owner_app_id);
            let delta = crate::warm_admit::with_schema_owner(
                owner.as_deref(),
                self.prepare_schema_delta(
                    schema_name,
                    schema_mutations,
                    idempotency_entries,
                    storage_prefix,
                    force_durable,
                    timing_breakdown,
                ),
            )
            .await?;
            prepared_deltas.push(delta);
        }
        stages.prepare_us = crate::request_phases::duration_us(prepare_started.elapsed());
        Ok(prepared_deltas)
    }

    /// Apply gate: lock molecules, validate revisions, publish resident tips.
    /// Returns the per-schema persist completions to await.
    #[allow(clippy::too_many_arguments)]
    async fn apply_prepared_deltas(
        &self,
        prepared_deltas: &mut [PreparedSchemaDelta],
        storage_prefix: Option<&str>,
        require_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
        molecule_lock_wait: &mut std::time::Duration,
        stages: &mut ResidentCommitStages,
    ) -> Result<Vec<PersistCompletion>, SchemaError> {
        for _ in 0..APPLY_GATE_RETRIES {
            self.recheck_delta_preconditions(prepared_deltas, storage_prefix)
                .await?;
            let molecule_lock_start = std::time::Instant::now();
            let molecule_write_guards = self
                .acquire_molecule_write_lock_keys(Self::delta_molecule_lock_keys(prepared_deltas))
                .await;
            let molecule_lock_elapsed = molecule_lock_start.elapsed();
            *molecule_lock_wait += molecule_lock_elapsed;
            Self::add_timing(
                timing_breakdown,
                "  - molecule_write_locks",
                molecule_lock_elapsed,
            );
            if prepared_deltas.iter().any(|delta| {
                !self.prepared_revisions_match(&delta.prepared)
                    || !self.prepared_revisions_match(&delta.sibling_prepared)
            }) {
                drop(molecule_write_guards);
                // A concurrent same-key writer moved these slot revisions out
                // from under this batch's prepare snapshot. Reseed from fresh
                // resident state and retry — the next loop iteration's CAS
                // recheck above re-validates any surviving CAS mutation
                // against that fresh state before this attempts to apply again.
                self.reseed_stale_deltas(prepared_deltas).await?;
                continue;
            }
            let (candidates, sibling_candidates) =
                self.collect_delete_barrier_candidates(prepared_deltas, storage_prefix)?;
            let tip_guards = self
                .drop_deletes_that_win(
                    prepared_deltas,
                    &candidates,
                    &sibling_candidates,
                    require_durable,
                )
                .await?;
            let phase2_start = std::time::Instant::now();
            let outcomes = self.publish_prepared_resident_deltas(prepared_deltas);
            // Resident tips are visible from this point. Capture must stage
            // even if persist-lane wait, flush, or mixed erase later returns
            // Err — otherwise RAM shows a commit the mutation log never saw.
            #[cfg(feature = "cloud-sync")]
            crate::sync::capture::mark_logical_commit_published();
            let mut persist_completions = Vec::new();
            for (delta, outcome) in prepared_deltas.iter_mut().zip(outcomes) {
                if let Some(completion) = self.finalize_prepared_schema_delta(
                    delta,
                    &outcome,
                    timing_breakdown,
                    author_clock_barrier.clone(),
                ) {
                    persist_completions.push((delta.schema_name.clone(), completion));
                }
            }
            let publish_elapsed = phase2_start.elapsed();
            stages.publish_us = crate::request_phases::duration_us(publish_elapsed);
            Self::add_timing(
                timing_breakdown,
                "  - update_memory_serial",
                publish_elapsed,
            );
            drop(tip_guards);
            drop(molecule_write_guards);
            return Ok(persist_completions);
        }
        Err(SchemaError::InvalidData(
            "apply-gate revision retries exceeded for multi-schema batch".into(),
        ))
    }

    /// Re-validate every surviving CAS precondition against the live head on
    /// EVERY attempt, not only after a detected revision mismatch. The
    /// batch-level check ran once, before this schema group was even prepared;
    /// a concurrent same-key writer — CAS or plain — can move the head in the
    /// gap between that check and this attempt without ever tripping the
    /// revision compare (the very next prepare simply adopts the moved state
    /// as its new baseline). Re-checking before every lock acquisition is what
    /// actually closes that gap. This is a no-op for a batch with no CAS
    /// mutations (`check_cas_preconditions` skips any mutation without
    /// `expected`), so it costs nothing on the plain-write hot path. See card
    /// fold-cas-lock-scope-excludes-non-cas-same-key-writers-20260906.
    async fn recheck_delta_preconditions(
        &self,
        prepared_deltas: &[PreparedSchemaDelta],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for delta in prepared_deltas {
            self.check_cas_preconditions(&delta.schema_mutations, storage_prefix)
                .await?;
            // Same gap, same close: a concurrent delete landing between the
            // batch-level gate and this attempt would otherwise let a
            // `must_exist` update mint the phantom row it was asked to
            // refuse. Skipped for every mutation without the flag, so the
            // plain-write path pays one predicate check.
            self.check_must_exist_preconditions(&delta.schema_mutations, storage_prefix)
                .await?;
        }
        Ok(())
    }

    fn delta_molecule_lock_keys(prepared_deltas: &[PreparedSchemaDelta]) -> Vec<String> {
        let mut lock_keys = Vec::new();
        for delta in prepared_deltas {
            lock_keys.extend(Self::molecule_lock_keys_for(
                &delta.schema,
                &delta.changed_keys,
            ));
            lock_keys.extend(Self::sibling_molecule_lock_keys(&delta.sibling_updates));
        }
        lock_keys
    }

    async fn reseed_stale_deltas(
        &self,
        prepared_deltas: &mut [PreparedSchemaDelta],
    ) -> Result<(), SchemaError> {
        for delta in prepared_deltas {
            self.reseed_changed_from_resident(&mut delta.schema, &delta.changed_keys)?;
            delta.prepared = self.prepared_slot_revisions(&delta.schema, &delta.changed_keys);
            let sibling_updates = self
                .build_protein_sibling_updates(
                    &delta.schema,
                    &delta.schema_mutations,
                    &delta.mutation_key_values,
                    &delta.atom_results,
                )
                .await?;
            // Capture before the resident rebase. A sibling change
            // during either step invalidates this preparation under
            // the gates on the next attempt.
            delta.sibling_prepared = self.prepared_sibling_slot_revisions(&sibling_updates);
            delta.sibling_updates =
                self.rebase_sibling_updates_on_resident(&delta.schema, sibling_updates);
        }
        Ok(())
    }

    /// A newer Delete can win while a Put waits to publish in memory. Request
    /// and replay Puts use the same exact key gate as Delete. Collects the
    /// entry barriers and the protein sibling tips that publication must check.
    fn collect_delete_barrier_candidates(
        &self,
        prepared_deltas: &[PreparedSchemaDelta],
        storage_prefix: Option<&str>,
    ) -> Result<
        (
            Vec<crate::atom::delete_barrier::DeleteBarrier>,
            Vec<SiblingCandidate>,
        ),
        SchemaError,
    > {
        let mut candidates = Vec::new();
        let mut sibling_candidates = Vec::new();
        for (delta_index, delta) in prepared_deltas.iter().enumerate() {
            for (mutation, key_value) in delta
                .schema_mutations
                .iter()
                .zip(&delta.mutation_key_values)
            {
                let mut resolved = mutation.clone();
                resolved.key_value = key_value.clone();
                candidates.extend(super::super::super::purge::plan_normal_delete_barriers(
                    &self.db_ops,
                    &delta.schema,
                    std::slice::from_ref(&resolved),
                )?);
            }
            // Protein updates publish tips under different molecule keys.
            // Build the same final keys as the sibling persist path.
            for (sibling_index, (molecule_uuid, data, changed)) in
                delta.sibling_updates.iter().enumerate()
            {
                for key in changed {
                    let hash = key.disk_hash();
                    let range = key.disk_range();
                    let Some(entry) = data.get_atom_entry(hash, range) else {
                        continue;
                    };
                    let storage_hash = self.db_ops.atoms().storage_hash(molecule_uuid, hash)?;
                    let storage_range = self.db_ops.atoms().storage_range(molecule_uuid, range)?;
                    let base_key = crate::atom::molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        &storage_hash,
                        &storage_range,
                    );
                    let final_key =
                        crate::schema::types::field::build_storage_key(storage_prefix, &base_key);
                    sibling_candidates.push((
                        delta_index,
                        sibling_index,
                        key.clone(),
                        final_key,
                        entry.clone(),
                    ));
                }
            }
        }
        Ok((candidates, sibling_candidates))
    }

    /// Take the tip-publication locks for every candidate key and settle each
    /// against the winning Delete barriers. A request fails before ack; replay
    /// filters stale mutations and retries the remaining batch without
    /// advancing its cursor early. A protein sibling can lose even when the
    /// entry Put wins: only that sibling key is removed from memory and disk
    /// work. The returned guards must outlive publication.
    async fn drop_deletes_that_win(
        &self,
        prepared_deltas: &mut [PreparedSchemaDelta],
        candidates: &[crate::atom::delete_barrier::DeleteBarrier],
        sibling_candidates: &[SiblingCandidate],
        require_durable: bool,
    ) -> Result<Vec<tokio::sync::OwnedMutexGuard<()>>, SchemaError> {
        let mut tip_keys: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.mk_key.clone())
            .collect();
        tip_keys.extend(
            sibling_candidates
                .iter()
                .map(|(_, _, _, key, _)| key.clone()),
        );
        let tip_guards = self.db_ops.atoms().lock_tip_publications(&tip_keys).await;
        let delete_winners = self
            .db_ops
            .atoms()
            .winning_delete_barriers(&tip_keys)
            .await?;
        for candidate in candidates {
            if delete_winners
                .get(&candidate.mk_key)
                .is_some_and(|barrier| barrier.order_key() >= candidate.order_key())
            {
                return Err(if require_durable {
                    SchemaError::ReplayDeleteBarrierChanged
                } else {
                    SchemaError::InvalidData(
                        "a newer Delete won this key before Put publication".into(),
                    )
                });
            }
        }
        for (delta_index, sibling_index, changed_key, key, entry) in sibling_candidates {
            if delete_winners
                .get(key)
                .is_some_and(|barrier| barrier.blocks_tip(entry))
            {
                prepared_deltas[*delta_index].sibling_updates[*sibling_index]
                    .2
                    .remove(changed_key);
            }
        }
        for delta in prepared_deltas.iter_mut() {
            delta
                .sibling_updates
                .retain(|(_, _, changed)| !changed.is_empty());
        }
        Ok(tip_guards)
    }

    /// Insert every schema envelope before a durable caller waits. This keeps
    /// a multi-schema request from blocking its later schema groups.
    async fn await_persist_completions(
        persist_completions: Vec<PersistCompletion>,
    ) -> Result<(), SchemaError> {
        for (schema_name, completion) in persist_completions {
            let counts = completion.await.map_err(|_| {
                SchemaError::InvalidData(format!(
                    "persist lane stopped before durable completion for schema '{schema_name}'"
                ))
            })?;
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::MoleculesPersisted,
                counts.molecules_persisted,
            );
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::MoleculeStoreCommits,
                counts.molecule_store_commits,
            );
        }
        Ok(())
    }

    /// Pipeline for Create / Update mutations. Hard-erasure verbs
    /// (`Purge` / `Delete`) are split off in the public entrypoint above
    /// before reaching this method so the existing flow (atom creation,
    /// molecule advancement, inline indexing, trigger dispatch) never
    /// sees them.
    pub(in crate::fold_db_core::mutation_manager) async fn write_create_update_batch_async(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        require_durable: bool,
        author_clock_barrier: Option<super::super::author_clock::AuthorClockPersistBarrier>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        // One log for the request and the persist lane. A nested call keeps it.
        if crate::durable_flush::current().is_some() {
            return self
                .write_create_update_batch_in_log(
                    mutations,
                    storage_prefix,
                    require_durable,
                    author_clock_barrier,
                )
                .await;
        }
        let log = crate::durable_flush::BatchPlacementLog::shared();
        crate::durable_flush::scope(
            log,
            self.write_create_update_batch_in_log(
                mutations,
                storage_prefix,
                require_durable,
                author_clock_barrier,
            ),
        )
        .await
    }
}
