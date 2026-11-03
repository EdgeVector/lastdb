//! The measurable request phases.
/// One measurable request phase. Queue wait is deliberately absent: it is
/// measured on the socket worker thread (before any task exists) and joins
/// the sample via a thread-local in `lastdb_uds`, not this accumulator.
///
/// Phases are not per-kind-exclusive by construction — a variant is simply
/// unreported (and therefore zero) on a path that never records it. Mutations
/// report the write phases; queries report [`Self::Count`] and
/// [`Self::Hydrate`]; both report admission/parse/schema-resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPhase {
    /// QoS admission-gate wait around op-permit acquisition.
    AdmissionWait,
    /// Wire-body parse into typed mutation components.
    Parse,
    /// Schema name resolution.
    SchemaResolve,
    /// Mutation field validation.
    Validate,
    /// Mutation only: the persisted-head reads that verify CAS expectations
    /// inside the native request gate. Storage IO, not contention.
    CasPrecondition,
    /// Query only: the cheap exact row count read from the key index that
    /// every push-down shape performs before hydrating anything.
    Count,
    /// Query only: record materialization, end to end. The PARENT total of the
    /// three `Hydrate*` steps below, which partition it exactly — unlike
    /// [`Self::Apply`], this one is a sum rather than a residual, because
    /// every region between the two timestamps is named.
    ///
    /// Read the sub-steps to decide what to fix; read this for the whole
    /// materialization cost.
    ///
    /// Why it was carved. Measured on the primary 2026-08-18
    /// (`0.23.3-803`), `hydrate` was 92-99% of service time on EVERY top read
    /// key — 1676.4 s of 1724.3 s for `client=kanban` on `BoardCards`
    /// (97.2%), 973.6 s of 988.9 s for `client=lastgit` (98.5%). Summed over
    /// the node's top read keys it exceeded the slowest WRITE key's entire
    /// service time, while the write path carried twenty named phases and the
    /// read path carried this one. "The read is slow" was as far as any
    /// surface could take an operator.
    Hydrate,
    /// Query only: the query-executor call itself — key-index resolution,
    /// window push-down, and record-body materialization for the matched rows.
    ///
    /// The only sub-step that touches storage, so it is the one that moves
    /// with the warm set hit rate (the read receipt's `logical_warm_hits` and
    /// `logical_warm_misses`). Large here with mostly hits is in-memory
    /// materialization cost (record decode and projection), which is a
    /// different fix from large here with misses climbing — that is a
    /// placement or pruning problem.
    HydrateRecords,
    /// Query only: building the response envelopes — one `serde_json`
    /// object per returned record carrying its fields, metadata and author
    /// key.
    ///
    /// Scales with `rows x fields` and with value width, and touches no
    /// storage at all. It is separated from [`Self::HydrateRecords`] because the
    /// two answer opposite questions about the same number: whether a slow
    /// read is the store's fault or the envelope's.
    HydrateFormat,
    /// Query only: the deterministic total-order sort imposed on the formatted
    /// rows so that offset/limit pagination cannot overlap or drop rows.
    ///
    /// `O(rows log rows)` comparisons, each of which re-reads its keys out of
    /// the already-built JSON values rather than off a decorated tuple, so its
    /// constant is a JSON map lookup and not a pointer deref. Timed apart
    /// because "the sort is the cost" is a claim that should be checkable
    /// before anyone rewrites a comparator.
    HydrateSort,
    /// Mutation: the batch write's wall time less [`Self::Grouping`] and
    /// [`Self::IdempotencyCheck`] — a RESIDUAL, not a direct measurement. It
    /// covers the native request's staging, commit and durability wait.
    /// [`Self::CasPrecondition`] runs inside the same window and is not
    /// subtracted. See
    /// [`crate::fold_db_core::mutation_manager::MutationManager::report_phase_totals`].
    Apply,
    /// Mutation: grouping the batch's mutations by schema before the
    /// per-schema loop runs.
    Grouping,
    /// Mutation: the idempotency-duplicate filter pass at the top of the
    /// batch pipeline.
    IdempotencyCheck,
    /// The handler's bounded wait for background index tasks.
    IndexWait,
    /// Post-write process-local outbox recording. The durable change-feed
    /// append joins the native commit and reports no phase of its own.
    ChangeRecord,
    /// Final response envelope/error rendering after route work completes.
    ResponseEnvelope,
    /// Status route: durable backup marker evaluation.
    StatusDurability,
    /// Status route: recursive data-directory sizing.
    StatusDataDir,
    /// Status route: request-ops ring/aggregate snapshot copying.
    StatusRequestOps,
}
