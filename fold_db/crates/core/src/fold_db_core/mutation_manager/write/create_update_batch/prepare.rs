//! Duplicate short-circuit, precondition gate and per-schema record groups.

use super::*;
use crate::schema::types::KeyValue;

/// The mutations of each group, with the schema name, in stage order.
type KeyGroups = Vec<(String, Vec<Mutation>)>;

/// The keys of each schema that more than one group of the request writes.
type RepeatedKeys = HashMap<String, HashSet<KeyValue>>;

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
        // batch turned out to be all duplicates. No schema group ran.
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
            stages,
            touched_group_ids: Vec::new(),
        })
    }

    /// Phase 0.5b: Compare-and-set gate (same-node atomicity).
    ///
    /// Verify each surviving mutation's precondition against its stored
    /// record before any group stages. Already-seen duplicates were filtered
    /// out earlier and never reach this check, so a safe retry of an applied
    /// CAS-guarded mutation cannot be rejected as a conflict against a value
    /// its own earlier attempt already produced.
    ///
    /// Reported separately from the lock wait: this is storage IO (one batch
    /// read of the guarded records), not queueing, and the two have opposite
    /// remedies. The same window also carries the `must_exist` update gate and
    /// the loud erasure gate: the same class of work under the same native
    /// gate, so they share the phase rather than minting a second name for
    /// the same number.
    pub(in crate::fold_db_core::mutation_manager::write) async fn check_batch_preconditions(
        &self,
        new_mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let cas_precondition_start = std::time::Instant::now();
        let cas_result = self
            .check_record_preconditions(new_mutations, storage_prefix)
            .await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::CasPrecondition,
            cas_precondition_start.elapsed(),
        );
        // Recorded before the error returns so a rejected batch still reports
        // what its precondition check cost — a CAS-conflict storm is exactly
        // when the operator needs this number.
        cas_result
    }

    /// Stage each schema group as records before the next group reads that
    /// state. Nothing publishes until every group succeeds and the complete
    /// request owns its reservations.
    pub(in crate::fold_db_core::mutation_manager::write) async fn stage_record_groups(
        &self,
        new_mutations: Vec<Mutation>,
        new_hashes: Vec<String>,
        storage_prefix: Option<&str>,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
        stages: &mut ResidentCommitStages,
        runtime: &std::sync::Arc<crate::storage::disk_pack::PackRuntime>,
    ) -> Result<usize, SchemaError> {
        // Keep each idempotency decision with the records it describes. A
        // detached put could make a client retry skip data that never
        // reached storage.
        let mut hash_by_uuid: HashMap<String, String> = new_mutations
            .iter()
            .zip(new_hashes.iter())
            .map(|(mutation, hash)| (mutation.uuid.clone(), hash.clone()))
            .collect();

        // Group mutations by schema to minimize schema reloads. First-seen
        // order is the stage order.
        let group_start = std::time::Instant::now();
        let (grouped_mutations, repeated) =
            self.independent_key_groups(self.group_mutations_by_schema(new_mutations))?;
        Self::add_timing(timing_breakdown, "grouping", group_start.elapsed());

        let group_count = grouped_mutations.len();
        let mut prepare_elapsed = std::time::Duration::ZERO;
        let mut deferred = Vec::new();
        for (schema_name, schema_mutations) in grouped_mutations {
            let prepare_started = std::time::Instant::now();
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
            deferred.extend(
                self.stage_record_group(
                    &schema_name,
                    schema_mutations,
                    idempotency_entries,
                    storage_prefix,
                    repeated.get(&schema_name),
                    runtime,
                )
                .await?,
            );
            prepare_elapsed += prepare_started.elapsed();
        }
        // A key that two groups wrote meets the record cap with its final
        // record, not with the record between the groups.
        deferred.sort_unstable();
        deferred.dedup();
        self.db_ops.atoms().require_records_fit(&deferred).await?;
        stages.prepare_us = crate::request_phases::duration_us(prepare_elapsed);
        Ok(group_count)
    }
}

/// Independent keys prepare together. A repeated key starts the next group
/// so its unchanged fields come from the earlier private write, not the
/// request's original state. The complete request still publishes once. The
/// repeated keys of each schema come back too: their record cap waits for the
/// final record.
impl MutationManager {
    fn independent_key_groups(
        &self,
        groups: Vec<(String, Vec<Mutation>)>,
    ) -> Result<(KeyGroups, RepeatedKeys), SchemaError> {
        let mut result = Vec::new();
        let mut repeated = RepeatedKeys::new();
        for (name, mutations) in groups {
            let schema = self.schema_manager.require_schema_metadata(&name)?;
            // The group of the last write of each key.
            let mut last_group: HashMap<KeyValue, usize> = HashMap::new();
            let mut group = 0;
            let mut current = Vec::new();
            for mutation in mutations {
                let key = self.resolve_mutation_key_value(&name, &schema, &mutation)?;
                if let Some(&at) = last_group.get(&key) {
                    if at == group {
                        result.push((name.clone(), std::mem::take(&mut current)));
                        group += 1;
                    }
                    repeated
                        .entry(name.clone())
                        .or_default()
                        .insert(key.clone());
                }
                last_group.insert(key, group);
                current.push(mutation);
            }
            if !current.is_empty() {
                result.push((name, current));
            }
        }
        Ok((result, repeated))
    }
}
