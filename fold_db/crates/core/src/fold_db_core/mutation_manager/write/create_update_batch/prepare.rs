//! Duplicate short-circuit, precondition gate and per-schema delta prepare.

use super::super::super::molecules::PreparedSchemaDelta;
use super::*;

impl MutationManager {
    /// Receipt for a batch whose every mutation the idempotency filter
    /// recognised as already applied.
    pub(in crate::fold_db_core::mutation_manager::write) async fn finish_all_duplicate_batch(
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
    /// guarded mutation, under the same native gate — so it shares the phase rather
    /// than minting a second name for the same number.
    pub(in crate::fold_db_core::mutation_manager::write) async fn check_batch_preconditions(
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
    pub(in crate::fold_db_core::mutation_manager::write) async fn prepare_grouped_deltas(
        &self,
        new_mutations: Vec<Mutation>,
        new_hashes: Vec<String>,
        storage_prefix: Option<&str>,
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
                .and_then(|mut schema| schema.owner_app_id.take());
            let delta = crate::warm_admit::with_schema_owner(
                owner.as_deref(),
                self.prepare_schema_delta(
                    schema_name,
                    schema_mutations,
                    idempotency_entries,
                    storage_prefix,
                    timing_breakdown,
                ),
            )
            .await?;
            prepared_deltas.push(delta);
        }
        stages.prepare_us = crate::request_phases::duration_us(prepare_started.elapsed());
        Ok(prepared_deltas)
    }
}
