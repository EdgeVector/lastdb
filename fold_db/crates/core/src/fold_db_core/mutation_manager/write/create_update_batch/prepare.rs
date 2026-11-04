//! Duplicate short-circuit, precondition gate and per-schema record groups.

use super::*;

/// The mutations of each group, with the schema name, in stage order.
type KeyGroups = Vec<(String, Vec<Mutation>)>;

impl MutationManager {
    /// Receipt for a batch whose every mutation the idempotency filter
    /// recognised as already applied.
    pub(in crate::fold_db_core::mutation_manager::write) async fn finish_all_duplicate_batch(
        &self,
        already_seen_ids: Vec<String>,
        record_revisions: std::collections::BTreeMap<String, String>,
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
        Self::report_phase_totals(timing_breakdown, start_time.elapsed());
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
            record_revisions,
            revision: None,
            operations,
            durability: Self::commit_durability(force_durable),
            stages,
        })
    }

    /// Stage each schema group as records before the next group reads that
    /// state. Nothing publishes until every group succeeds and the complete
    /// request owns its reservations.
    pub(in crate::fold_db_core::mutation_manager::write) async fn stage_record_groups(
        &self,
        new_mutations: Vec<Mutation>,
        new_hashes: Vec<String>,
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
        let grouped_mutations =
            self.independent_key_groups(self.group_mutations_by_schema(new_mutations))?;
        Self::add_timing(timing_breakdown, "grouping", group_start.elapsed());

        let group_count = grouped_mutations.len();
        let mut prepare_elapsed = std::time::Duration::ZERO;
        for (schema_name, schema_mutations) in grouped_mutations {
            let prepare_started = std::time::Instant::now();
            let idempotency_entries = schema_mutations
                .iter()
                .map(|mutation| {
                    let hash = hash_by_uuid
                        .remove(&mutation.uuid)
                        .expect("new mutation has a matching idempotency hash");
                    (
                        mutation.retry_key(),
                        lastdb_model::record_replacement::RetryReceipt {
                            intent_hash: hash,
                            revision: mutation
                                .record_stamp()
                                .expect("author clock prepared")
                                .revision(),
                        },
                    )
                })
                .collect();
            self.stage_record_group(&schema_name, schema_mutations, idempotency_entries, runtime)
                .await?;
            prepare_elapsed += prepare_started.elapsed();
        }
        stages.prepare_us = crate::request_phases::duration_us(prepare_elapsed);
        Ok(group_count)
    }
}

/// Independent records prepare together. A repeated primary key is rejected:
/// its replacement would require the revision of an unacknowledged write.
impl MutationManager {
    fn independent_key_groups(
        &self,
        groups: Vec<(String, Vec<Mutation>)>,
    ) -> Result<KeyGroups, SchemaError> {
        let mut result = Vec::new();
        for (name, mutations) in groups {
            let schema = self.schema_manager.require_schema_metadata(&name)?;
            let mut current_keys = HashSet::new();
            let mut current = Vec::new();
            for mutation in mutations {
                let key = self.resolve_mutation_key_value(&name, &schema, &mutation)?;
                if current_keys.contains(&key) {
                    return Err(SchemaError::InvalidData(
                        "a batch cannot replace the same record twice".into(),
                    ));
                }
                current_keys.insert(key);
                current.push(mutation);
            }
            if !current.is_empty() {
                result.push((name, current));
            }
        }
        Ok(result)
    }
}
