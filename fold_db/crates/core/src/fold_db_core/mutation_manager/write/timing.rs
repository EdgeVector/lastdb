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

    /// Lift the per-step timing breakdown into the task-local request-phase
    /// accumulator ([`crate::request_phases`]) as the apply / persist / flush
    /// split the request-ops telemetry renders.
    ///
    /// This is the "don't re-time" seam: the breakdown map already measures
    /// every step of `write_create_update_batch_async` (and was
    /// previously discarded at debug log level), so phase reporting is a
    /// bucketing of existing measurements, not a second set of timers.
    ///
    /// Bucketing:
    /// - **persist** — the RESIDUAL of the durable-store bucket: the atom
    ///   batch store inside `create_atoms_batch` (see
    ///   `prepare_atoms_and_key_values`, which batch-stores atoms as a side
    ///   effect), plus any future durable step added to this bucket without
    ///   its own phase.
    /// - **persist_molecules** / **persist_schema** / **persist_idempotency**
    ///   — the molecule store (`write_molecules_batch`), the schema persist
    ///   (`schema_store`), and the idempotency record store
    ///   (`idempotency_store`), carved out of that bucket by name.
    ///
    ///   These three are split for the same reason the `apply` residual was
    ///   carved up below. `persist` is routinely the single largest phase in
    ///   the mutation path — 32% of wall time on the kanban board's schema and
    ///   68% on `Card`, measured on the live primary 2026-08-17 — and as one
    ///   name it could not answer the only question worth asking about it:
    ///   whether that time is the irreducible cost of writing the atom bytes,
    ///   or a molecule/catalog rewrite whose size tracks the molecule's
    ///   cardinality rather than the write's. Those have different fixes; the
    ///   `bc941dbc` vs `39a0424f` split above is not readable without them.
    ///
    ///   The timers already existed here and were reachable only through the
    ///   `tracing::debug!` line below — which a running node cannot enable
    ///   (`papercut-lastdb-no-runtime-log-level-toggle`), so on the one node
    ///   whose numbers matter the decomposition was measured and unreadable.
    /// - **flush** — the optional durability flush; ~0 unless
    ///   `LASTDB_MUTATION_SYNC_FLUSH=1`, and a truncated-to-zero value stays
    ///   unset so operator surfaces hide the column.
    /// - **apply** — the RESIDUAL: everything the batch spent that no other
    ///   named bucket claims. Every step this function measures into
    ///   `timing_breakdown` now has its own phase (see below), so a non-zero
    ///   `apply` today means UNTIMED work — which is precisely what the
    ///   residual exists to keep visible.
    ///
    /// `apply` is deliberately computed as a RESIDUAL — `total` minus every
    /// other named bucket below — rather than by summing named keys. Summing
    /// made the phase totals a report on the timers that happened to exist:
    /// any awaited region inside this function with no `add_timing` around it
    /// simply VANISHED from the request-ops phase breakdown, showing up only
    /// as an unexplained gap between a request's wall time and the sum of its
    /// phases. That is not hypothetical — the Phase 3 molecule write-lock
    /// acquisition and molecule restore were both awaited and both untimed at
    /// the time, and on a cold node they accounted for the majority of a slow
    /// mutation. As a residual, a future untimed region still lands in
    /// `apply` (visible, if coarse) instead of disappearing, and the phases
    /// now sum to this function's own wall clock by construction.
    ///
    /// `restore_molecules`, `schema_reload`, `protein_sibling_fold`,
    /// `grouping`, `idempotency_check`, `sync_uuids`, `spawn_indexing`,
    /// `apply_memory`, `schema_load` and `dedupe_scan` were each measured
    /// into `timing_breakdown` (for the `tracing::debug!`-only log below) and
    /// then silently folded into the `apply` residual by the `_ => {}` arm
    /// here — measured, but not reported anywhere a live node's operator
    /// could read without turning on debug logging. Naming them as their own
    /// phases does not add a timer; it stops discarding one that already ran.
    ///
    /// The last three arrived later than the first seven, and the delay had a
    /// cost worth recording. Measured on the primary 2026-08-18, `apply` was
    /// 822.0 s of the node's slowest write key's 3022.2 s (27.2%) — its
    /// single largest phase — and ~968 ms per write on `BoardCards`
    /// (hash-range, many rows per partition) against ~1.6-2 ms per write on
    /// every point-keyed schema on the same node. A ~500x spread inside the
    /// largest bucket, with three already-running timers inside it that no
    /// surface published. `apply_memory` is the first thing to read when that
    /// spread is diagnosed: it is the only one of the three sized by the
    /// molecules the batch touches rather than by the batch.
    ///
    /// `molecule_lock_wait` is passed in because it is reported separately as
    /// [`RequestPhase::MoleculeGate`]; subtracting it keeps it out of `apply`
    /// so the two do not double-count the same microseconds. It is NOT folded
    /// into [`RequestPhase::LockWait`] — that phase is the pre-pipeline purge
    /// barrier and CAS mutex, and summing a gate that fold #1104 narrowed with
    /// two that it did not left the narrowing's effect unmeasurable on a
    /// running node.
    ///
    /// Durations are summed per bucket BEFORE the microsecond conversion so
    /// several sub-microsecond steps cannot each truncate to zero.
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
        let mut dedupe_scan = std::time::Duration::ZERO;
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
                "  - dedupe_scan" => dedupe_scan += *elapsed,
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
            + schema_load
            + dedupe_scan;
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
        add_phase(RequestPhase::DedupeScan, dedupe_scan);
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
