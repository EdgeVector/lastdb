//! The measurable request phases.
/// One measurable request phase. Queue wait is deliberately absent: it is
/// measured on the socket worker thread (before any task exists) and joins
/// the sample via a thread-local in `lastdb_uds`, not this accumulator.
///
/// Phases are not per-kind-exclusive by construction — a variant is simply
/// unreported (and therefore zero) on a path that never records it. Mutations
/// report the write phases; queries report [`Self::Count`], [`Self::Hydrate`]
/// and [`Self::Annotate`]; both report admission/parse/schema-resolve.
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
    /// Mutation only: time blocked acquiring the Phase 3 per-`(field key,
    /// hash, range)` write gate, which serializes concurrent writers of the
    /// same field key slot across the remainder of the write (including the
    /// deferred durable persist).
    ///
    /// A large value here means writers are queueing on ONE SLOT — one
    /// `(field key, hash, range)` triple, not one hash. `e8839a4f2` narrowed
    /// the key a second time to include the range, so two rows differing only
    /// in their range no longer wait out each other's deferred durable put.
    ///
    /// The prescribed fix is therefore NOT "spread the hot key" for a workload
    /// whose keys already differ in their range — that advice was written for
    /// the hash-only gate and is stale for such a workload. Spread the key only
    /// when the hot writers genuinely share one `(hash, range)` slot.
    ///
    /// Measured on the primary 2026-08-18 (daemon `0.23.3-803`): the node's
    /// heaviest write path — `client=kanban` on `BoardCards`, a HashRange
    /// schema whose hash is low-cardinality — spent 46.9s of 2342s here
    /// (2.0%, ~90ms/write), against the 73% (~809ms/write) measured on the
    /// same schema and client under the hash-only gate. When this phase is
    /// small and the write is still slow, read [`Self::Persist`] instead.
    ///
    /// The gate-key shape is pinned by the `write_gate_key_separates_hashes_and_ranges`
    /// test in `fold_db_core::mutation_manager::write::field_keys`. That test
    /// and this paragraph are the two places a further narrowing must update
    /// together, and each names the other so neither can be changed alone —
    /// this doc stayed stale for 15 days after `e8839a4f2` because nothing
    /// linked them.
    FieldKeyGate,
    /// Mutation only: the persisted-head reads that verify CAS expectations
    /// while the batch holds the locks above. Storage IO, not contention.
    CasPrecondition,
    /// Query only: the cheap exact row count read from the key index that
    /// every push-down shape performs before hydrating anything.
    Count,
    /// Query only: row materialization, end to end. The PARENT total of the
    /// three `Hydrate*` steps below, which partition it exactly — unlike
    /// [`Self::Apply`] and [`Self::Persist`], this one is a sum rather than a
    /// residual, because every region between the two timestamps is named.
    ///
    /// It is kept as its own phase so the series that predates the carve stays
    /// comparable. Read the sub-steps to decide what to fix; read this to
    /// compare against a rollup written before 0.23.4.
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
    /// Query only: per-field merge-conflict annotation of the formatted rows.
    /// Historically a per-field-key storage walk; expected to fall to ~0 once
    /// the home-conflict index (fold #1032) reaches a node.
    Annotate,
    /// In-memory application of the write — the RESIDUAL left after every
    /// other named write phase is subtracted from the batch's wall time. See
    /// [`crate::fold_db_core::mutation_manager::MutationManager::report_phase_totals`]
    /// for why this stays a residual rather than a sum of named steps.
    Apply,
    /// Mutation: the in-memory apply loop itself
    /// (`apply_mutations_to_field_keys`) — walking this batch's record results
    /// onto the resident field keys. Carved out of the `Apply` residual for
    /// the reason [`Self::Persist`]'s sub-steps were: the timer already ran
    /// in the batch write's `timing_breakdown`, and the residual then
    /// discarded it, so the phase that shares `apply`'s NAME could not be
    /// distinguished from everything else the residual absorbs.
    ///
    /// This is the step whose cost should track rows-under-the-touched
    /// field keys rather than rows written, so a hash-range schema with many
    /// rows per partition pays here where a point-keyed schema does not.
    ApplyMemory,
    /// Mutation: loading the schema at the top of the per-schema loop, before
    /// any field key is touched. Distinct from [`Self::SchemaResolve`] (the
    /// handler's name→hash resolution) and from [`Self::SchemaReload`] (the
    /// write-back after the mutation). Large here means the catalog read is
    /// on the write's critical path.
    SchemaLoad,
    /// Mutation: schema reload back into the schema manager after a write —
    /// covers both the inline path (`mode != write` or over the defer cap)
    /// and, once surfaced from the deferred task, the write-mode background
    /// reload. Split out of the `Apply` residual because it was previously
    /// one of the untimed regions absorbed into it wholesale.
    SchemaReload,
    /// Mutation: restoring field keys missing from the resident/in-memory
    /// graph for the keys this batch touches — storage IO, not compute, but
    /// reported here because the write path already measured it as a named
    /// step before this phase existed.
    RestoreFieldKeys,
    /// Mutation: grouping the batch's mutations by schema before the
    /// per-schema loop runs.
    Grouping,
    /// Mutation: the idempotency-duplicate filter pass at the top of the
    /// batch pipeline.
    IdempotencyCheck,
    /// Mutation: syncing in-memory field key UUIDs back onto the schema
    /// before it is stored/reloaded.
    SyncUuids,
    /// Mutation: spawning the background index-mutation task (the spawn
    /// call itself, not the indexing work, which runs off-task and is
    /// deliberately not attributed here — see the task-local doc above).
    SpawnIndexing,
    /// Durable persist of the applied write, as a RESIDUAL: the durable-store
    /// time not attributed to one of the named sub-steps below. In practice
    /// that is the record batch store, plus any future durable step added to
    /// the persist bucket without its own phase — visible here, if coarse,
    /// rather than vanishing.
    ///
    /// Carved up for the same reason [`Self::Apply`] was: `persist` summed
    /// four independently-sized durable stores under one name, so an operator
    /// reading it as the largest phase in the write path could not tell an
    /// irreducible record write from a whole-field-key rewrite. The sub-timers
    /// already existed in the batch write's `timing_breakdown`; naming them
    /// does not add a timer.
    Persist,
    /// Mutation: the field key batch store (`write_field_keys_batch`). Sized by
    /// field key cardinality rather than by the write, so a schema whose
    /// field keys hold tens of thousands of keys pays here on every mutation.
    PersistFieldKeys,
    /// Mutation: persisting the schema itself after a write (`schema_store`).
    /// Near zero once the catalog's skip-if-unchanged path applies; a large
    /// value here means the catalog body is being rewritten per mutation.
    PersistSchema,
    /// Mutation: storing the idempotency record for the applied batch.
    PersistIdempotency,
    /// Explicit store durability barrier; native commits report their own phases.
    Flush,
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
