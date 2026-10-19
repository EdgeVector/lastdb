//! Chunked hard-erasure processing for one schema.

use super::hard_erasure::group_mutations_by_schema;
use super::*;

/// What a batch of hard erasures did.
pub(in crate::fold_db_core::mutation_manager) struct HardErasureOutcome {
    pub(in crate::fold_db_core::mutation_manager) ids: Vec<String>,
    /// Keys that a purge left alone, because a tip of the key is newer than
    /// the cutoff of its mutation.
    pub(in crate::fold_db_core::mutation_manager) spared: Vec<crate::schema::types::KeyValue>,
}

impl MutationManager {
    /// Dispatch hard-erasure mutations (`Purge` or `Delete`) through the erase
    /// in `purge::erase`, **grouped by schema** and cut into chunks of
    /// [`PURGE_BARRIER_CHUNK`] keys.
    ///
    /// The caller owns the native write gate through a request scope. Each
    /// chunk reads the tips of its keys, hands every tip change to the atom
    /// counter planner, and stages one batch. A body goes in that batch when
    /// its count reaches zero. No schema barrier and no molecule lock is
    /// needed: the gate orders the chunk against every writer.
    ///
    /// Ids are returned in the caller's submitted order.
    pub(in crate::fold_db_core::mutation_manager) async fn process_hard_erasures_durable(
        &self,
        erasures: Vec<Mutation>,
        options: HardErasureOptions<'_>,
    ) -> Result<HardErasureOutcome, SchemaError> {
        let ids: Vec<String> = erasures.iter().map(|m| m.uuid.clone()).collect();
        let mut spared = Vec::new();
        let (schema_order, mut by_schema) = group_mutations_by_schema(erasures);
        for schema_name in schema_order {
            let schema_purges = by_schema
                .remove(&schema_name)
                .expect("schema_order entries were sourced from by_schema keys");
            for chunk in schema_purges.chunks(PURGE_BARRIER_CHUNK) {
                spared.extend(
                    self.process_hard_erasure_chunk(&schema_name, chunk, &options)
                        .await?,
                );
            }
        }
        Ok(HardErasureOutcome { ids, spared })
    }

    /// Erase one bounded chunk and record its stats. Returns the keys that the
    /// erase spared.
    async fn process_hard_erasure_chunk(
        &self,
        schema_name: &str,
        chunk: &[Mutation],
        options: &HardErasureOptions<'_>,
    ) -> Result<Vec<crate::schema::types::KeyValue>, SchemaError> {
        let verb = options.verb;
        let caller = chunk
            .iter()
            .find_map(|m| m.erase_caller)
            .unwrap_or("mutation-pipeline");
        let started = std::time::Instant::now();
        let report = super::super::super::purge::erase_keys(
            &self.db_ops,
            &self.schema_manager,
            schema_name,
            chunk,
            super::super::super::purge::EraseOptions {
                storage_prefix: options.storage_prefix,
                verb,
                caller,
            },
        )
        .await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeCommit,
            started.elapsed(),
        );
        let report = match report {
            Ok(report) => report,
            Err(error) => {
                self.record_purge(schema_name, 0);
                return Err(error);
            }
        };
        self.record_purge(
            schema_name,
            u64::try_from(report.records_purged).unwrap_or(u64::MAX),
        );
        self.record_purge_path(
            schema_name,
            report.target_slots,
            u64::try_from(report.atom_rows_deleted).unwrap_or(u64::MAX),
        );
        info!(
            schema = %schema_name,
            records = report.records_purged,
            atoms_deleted = report.atom_rows_deleted,
            spared = report.spared_keys.len(),
            chunk_size = chunk.len(),
            verb = verb.ledger_verb(),
            policy = match options.missing {
                super::super::super::purge::PurgeMissingPolicy::Refuse => "refuse",
                super::super::super::purge::PurgeMissingPolicy::Skip => "skip",
            },
            "hard erasure committed",
        );
        Ok(report.spared_keys)
    }
}
