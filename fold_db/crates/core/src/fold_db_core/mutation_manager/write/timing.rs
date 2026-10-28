//! Per-step timing accumulation and request-phase reporting for the write pipeline.

use std::collections::HashMap;

use tracing::debug;

use super::*;

impl MutationManager {
    /// Report this pipeline run as ONE logical resident commit and its
    /// operation count.
    ///
    /// Split out so the all-duplicates early return and the applied path
    /// cannot disagree about what a commit is: both reach the gates' owner
    /// exactly once per caller batch, so both count 1.
    pub(super) fn report_resident_commit_counts(operations: &ResidentCommitOperations) {
        use crate::request_phases::{add_counter, RequestCounter};
        add_counter(RequestCounter::ResidentCommits, 1);
        add_counter(RequestCounter::ResidentOperations, operations.total());
    }

    /// Accumulates `elapsed` into the timing map under `key`.
    pub(in crate::fold_db_core::mutation_manager) fn add_timing(
        map: &mut HashMap<&str, std::time::Duration>,
        key: &'static str,
        elapsed: std::time::Duration,
    ) {
        *map.entry(key).or_insert(std::time::Duration::ZERO) += elapsed;
    }

    /// Report existing phase timers without timing the same work again.
    ///
    /// The historical `Persist` bucket receives `create_atoms_batch`, which
    /// now validates keys and builds atoms in memory. That bucket does not
    /// prove a physical write. Keep its name and arithmetic stable for callers.
    /// Molecule, schema, idempotency, restore, and other named timers retain
    /// their separate request phases. Missing timers supply no measured work.
    ///
    /// `Apply` is the residual after named buckets and molecule lock wait;
    /// it is not a direct CPU or disk measurement. Molecule lock wait has its
    /// own phase and is excluded here so it is not counted twice.
    /// Durations sum before microsecond conversion to avoid repeated truncation.
    pub(in crate::fold_db_core::mutation_manager) fn report_phase_totals(
        timing_breakdown: &HashMap<&str, std::time::Duration>,
        total_time: std::time::Duration,
        molecule_lock_wait: std::time::Duration,
    ) {
        use crate::request_phases::{add_phase, RequestPhase};

        let mut persist = std::time::Duration::ZERO;
        let mut persist_molecules = std::time::Duration::ZERO;
        let mut persist_schema = std::time::Duration::ZERO;
        let mut persist_idempotency = std::time::Duration::ZERO;
        let mut flush = std::time::Duration::ZERO;
        let mut restore_molecules = std::time::Duration::ZERO;
        let mut schema_reload = std::time::Duration::ZERO;
        let mut protein_sibling_fold = std::time::Duration::ZERO;
        let mut grouping = std::time::Duration::ZERO;
        let mut idempotency_check = std::time::Duration::ZERO;
        let mut sync_uuids = std::time::Duration::ZERO;
        let mut spawn_indexing = std::time::Duration::ZERO;
        let mut apply_memory = std::time::Duration::ZERO;
        let mut schema_load = std::time::Duration::ZERO;
        for (key, elapsed) in timing_breakdown {
            // Keys are the literal breakdown map keys — the "  - " prefix is
            // the debug log's indentation and part of the key.
            match *key {
                "  - create_atoms_batch" => persist += *elapsed,
                "  - write_molecules_batch" => persist_molecules += *elapsed,
                "schema_store" => persist_schema += *elapsed,
                "idempotency_store" => persist_idempotency += *elapsed,
                "flush" => flush += *elapsed,
                "  - restore_molecules" => restore_molecules += *elapsed,
                "schema_reload" => schema_reload += *elapsed,
                "  - protein_sibling_fold" => protein_sibling_fold += *elapsed,
                "grouping" => grouping += *elapsed,
                "idempotency_check" => idempotency_check += *elapsed,
                "sync_uuids" => sync_uuids += *elapsed,
                "spawn_indexing" => spawn_indexing += *elapsed,
                "  - update_memory_serial" => apply_memory += *elapsed,
                "schema_load" => schema_load += *elapsed,
                // "  - molecule_write_locks" folds into `molecule_lock_wait`
                // below (accumulated by the caller across schema groups, not
                // this map). "  - create_atoms_batch" already matched above.
                //
                // The deferred arm's "  - write_molecules_batch_deferred" is
                // the ONE measured key still left in the residual, and
                // deliberately: it times the `tokio::spawn` call, not the
                // durable put, which runs off-task and is already attributed
                // by the resident deferred-persist duration counter. Naming a
                // phase for a spawn would publish a bucket that reads ~0 on
                // every node and zero on any node not in write-mode over the
                // defer cap. Every OTHER key in this map is now reported.
                _ => {}
            }
        }
        let named_other = restore_molecules
            + schema_reload
            + protein_sibling_fold
            + grouping
            + idempotency_check
            + sync_uuids
            + spawn_indexing
            + apply_memory
            + schema_load;
        // The durable-store sub-steps carved out of `persist`. Subtracted from
        // the residual alongside `persist` itself so the two do not
        // double-count the same microseconds — the same reason
        // `molecule_lock_wait` is passed in and subtracted above.
        let persist_named = persist_molecules + persist_schema + persist_idempotency;
        // Saturating: the accounted buckets are measured inside `total_time`,
        // so they cannot legitimately exceed it, but clock coarseness must not
        // be able to underflow the residual.
        let apply = total_time
            .saturating_sub(persist)
            .saturating_sub(persist_named)
            .saturating_sub(flush)
            .saturating_sub(molecule_lock_wait)
            .saturating_sub(named_other);

        add_phase(RequestPhase::MoleculeGate, molecule_lock_wait);
        add_phase(RequestPhase::RestoreMolecules, restore_molecules);
        add_phase(RequestPhase::SchemaReload, schema_reload);
        add_phase(RequestPhase::ProteinSiblingFold, protein_sibling_fold);
        add_phase(RequestPhase::Grouping, grouping);
        add_phase(RequestPhase::IdempotencyCheck, idempotency_check);
        add_phase(RequestPhase::SyncUuids, sync_uuids);
        add_phase(RequestPhase::SpawnIndexing, spawn_indexing);
        add_phase(RequestPhase::Apply, apply);
        add_phase(RequestPhase::ApplyMemory, apply_memory);
        add_phase(RequestPhase::SchemaLoad, schema_load);
        add_phase(RequestPhase::Persist, persist);
        add_phase(RequestPhase::PersistMolecules, persist_molecules);
        add_phase(RequestPhase::PersistSchema, persist_schema);
        add_phase(RequestPhase::PersistIdempotency, persist_idempotency);
        add_phase(RequestPhase::Flush, flush);
    }

    /// Log a sorted timing breakdown for batch mutation phases.
    pub(in crate::fold_db_core::mutation_manager) fn log_timing_breakdown(
        timing_breakdown: &HashMap<&str, std::time::Duration>,
        total_time: std::time::Duration,
    ) {
        tracing::debug!(
            "Batch mutation timing breakdown (total: {:.2}ms):",
            total_time.as_millis()
        );
        let mut sorted_timings: Vec<_> = timing_breakdown.iter().collect();
        sorted_timings.sort_by(|a, b| b.1.cmp(a.1));
        let total_ms = total_time.as_millis() as f64;
        for (operation, duration) in sorted_timings {
            let percentage = (duration.as_millis() as f64 / total_ms) * 100.0;
            debug!(
                "  - {}: {:.2}ms ({:.1}%)",
                operation,
                duration.as_millis(),
                percentage
            );
        }
    }
}
