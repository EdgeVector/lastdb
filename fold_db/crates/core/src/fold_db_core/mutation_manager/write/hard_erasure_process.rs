//! Chunked hard-erasure processing for one schema.

use super::hard_erasure::group_mutations_by_schema;
use super::*;

impl MutationManager {
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
        // Each schema has its own molecule definitions and purge barrier.
        let (schema_order, mut by_schema) = group_mutations_by_schema(erasures);

        for schema_name in schema_order {
            let schema_purges = by_schema
                .remove(&schema_name)
                .expect("schema_order entries were sourced from by_schema keys");

            // Load definitions once for the physical Purge cutover check.
            let schema_for_purge = self
                .schema_manager
                .get_schema_metadata(&schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' not found for erasure processing"
                    ))
                })?;
            // Live Delete does not reclaim atoms. It must not wait on (or
            // advance) the compliance purge's reverse-edge history upgrade.
            let atom_ref_cutover = !options.verb.is_delete()
                && super::super::super::purge::atom_ref_cutover_ready(
                    &self.db_ops,
                    &schema_for_purge,
                )
                .await?;

            for chunk in schema_purges.chunks(PURGE_BARRIER_CHUNK) {
                self.process_hard_erasure_chunk(&schema_name, chunk, atom_ref_cutover, &options)
                    .await?;
            }
        }
        Ok(ids)
    }

    /// Remove one bounded chunk and record its stats. The envelope stages
    /// retention updates in the native transaction and delivers Search after commit.
    async fn process_hard_erasure_chunk(
        &self,
        schema_name: &str,
        chunk: &[Mutation],
        atom_ref_cutover: bool,
        options: &HardErasureOptions<'_>,
    ) -> Result<(), SchemaError> {
        let verb = options.verb;
        let missing = options.missing;
        let keys: Vec<crate::schema::types::KeyValue> =
            chunk.iter().map(|m| m.key_value.clone()).collect();

        // Both user Delete forms converge tips after their caller conditions
        // pass. Neither reclaims bodies that other protein members can share.
        // Explicit Purge retains its separate atom-reachability erasure.
        let caller = chunk
            .iter()
            .find_map(|m| m.erase_caller)
            .unwrap_or("mutation-pipeline");
        let outcome = if verb.is_delete() {
            self.converge_delete_chunk(schema_name, chunk, options.storage_prefix)
                .await?
        } else if atom_ref_cutover {
            Some(
                self.purge_chunk_by_atom_refs(schema_name, &keys, options, caller)
                    .await?,
            )
        } else {
            Some(
                self.purge_chunk_guarded(schema_name, &keys, options, caller)
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
        caller: &'static str,
    ) -> Result<(HardErasureReport, std::time::Duration), SchemaError> {
        let started = std::time::Instant::now();
        let mut acct = super::super::super::purge::PurgeCommitAccounting::default();
        let report = super::super::super::purge::purge_records_bulk(
            super::super::super::purge::PurgeScope {
                db_ops: &self.db_ops,
                schema_manager: &self.schema_manager,
                schema_name,
            },
            keys,
            options.missing,
            options.verb,
            &mut acct,
            super::super::super::purge::PurgeMode {
                reachability: super::super::super::purge::PurgeReachability::AtomRefEdges,
                precomputed_retained: None,
                caller,
            },
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
        caller: &'static str,
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
            super::super::super::purge::PurgeScope {
                db_ops: &self.db_ops,
                schema_manager: &self.schema_manager,
                schema_name,
            },
            keys,
            options.missing,
            options.verb,
            &mut acct,
            super::super::super::purge::PurgeMode {
                reachability: super::super::super::purge::PurgeReachability::GuardedComplement,
                precomputed_retained: options.precomputed_retained.clone(),
                caller,
            },
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
}

type HardErasureReport = super::super::super::purge::BulkPurgeReport;
