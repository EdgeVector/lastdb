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
    /// Mutation only: time blocked acquiring the per-`(schema, key)` CAS
    /// mutex in Phase 0.5, before any pipeline step runs.
    ///
    /// This is queueing, not work: a large value means same-key writers are
    /// standing in line, and the fix is on the caller's key layout (spread
    /// the hot key) rather than anywhere in the write pipeline. Batches with
    /// no CAS expectation take no per-key mutex at all and report zero here.
    ///
    /// Does NOT include either of the other two write gates:
    ///
    /// - the Phase 0 per-schema purge barrier is [`Self::PurgeBarrier`];
    /// - the Phase 3 molecule write gate is [`Self::MoleculeGate`].
    ///
    /// Both used to be folded in here. Each fold made a gate that changed
    /// indistinguishable from gates that did not — see those variants' docs.
    /// Series note for rollup readers: before the [`Self::PurgeBarrier`]
    /// split, this number also carried the purge barrier, so a `lock_wait`
    /// drop at that upgrade boundary is the split, not a regression cured.
    LockWait,
    /// Mutation purge side: time blocked before an EXCLUSIVE acquisition of
    /// the per-schema guarded-purge barrier.
    ///
    /// Ordinary writes do not acquire this barrier and report zero here. A
    /// large value identifies concurrent guarded purges with one schema name.
    /// [`Self::PurgeCommit`] measures the critical section after acquisition.
    PurgeBarrier,
    /// Mutation only, purge side: the reachability trace that decides whether
    /// there is anything to delete, run UNLOCKED ahead of the barrier.
    ///
    /// Separate from [`Self::PurgeCommit`] because this read runs outside the
    /// guarded critical section. Under `PurgeMissingPolicy::Skip`, an empty
    /// trace skips the exclusive acquisition. A large value here identifies
    /// a scan problem in the purge plan.
    PurgePlan,
    /// Mutation only, purge side: the destructive commit — snapshot→delete —
    /// performed while holding the per-schema purge barrier EXCLUSIVELY.
    ///
    /// Ordinary writes do not wait for this work. Until it was instrumented,
    /// this critical-section work was in no phase at all. On the
    /// 2026-08-09 primary, 59.5% of all `kanban` mutation wall time against
    /// the board schema was unphased, and the single slowest sample (214.3s)
    /// was 100% unphased because it was the purge request.
    /// The purge ledger on
    /// [`crate::fold_db_core::mutation_manager::PurgeStats`] already counted
    /// this critical-section time per schema; what was missing was attributing it to the
    /// REQUEST that caused it, which is the only view that says which caller
    /// to talk to.
    ///
    /// One sample sums every `PURGE_BARRIER_CHUNK` critical section in the
    /// batch. It is not the longest single hold.
    ///
    /// Reported as a RESIDUAL, for the same reason [`Self::Persist`] and
    /// [`Self::Apply`] are: the critical section summed five independently-sized steps
    /// under one name, and they do not share a fix. Measured on the primary
    /// 2026-08-18 (`0.23.3-803`), `purge_commit` was 613.1 s of the 2903.2 s
    /// this node's slowest write key spent — 21.1% — against 433 records
    /// removed in 551 purges, i.e. **0.79 records per critical section**. Which
    /// of the five was paying that could not be read off any surface. The
    /// sub-steps below carve it up; what stays here is the destructive
    /// commit's own bookkeeping (schema fetch, key-shape validation, batch
    /// dedupe, storage-slot planning, the write-ahead ledger row).
    PurgeCommit,
    /// Mutation only, purge side: materializing every field molecule of the
    /// schema (`refresh_runtime_field_molecules`) so the trace and the
    /// retention guards have live data to read.
    ///
    /// Sized by the SCHEMA, not by the batch: one `load_all_mk_records`
    /// prefix scan per field, each covering every key that field's molecule
    /// holds — live and tombstoned alike. Removing one record from a 24-field
    /// schema whose molecules carry ~19.2k keys each reads ~460k rows before
    /// it deletes anything.
    ///
    /// A large value here is the cost of asking a whole-molecule question to
    /// answer a one-key one, and its fix is the one already taken on the
    /// unlocked purge probe (retired 2026-09-04 in favor of
    /// `converge_delete_tips`'s keyed trace for the live-Delete path, which
    /// never runs this phase at all): keyed point-gets in place of the full
    /// materialization, cutting a ~3.9 s per-call scan to ~47 ms by exactly
    /// that change. This phase exists to say whether the compliance-purge
    /// destructive path is still paying what the probe stopped paying.
    ///
    /// Also reported by the request-thread complement plan, outside the
    /// exclusive hold, so it does not overlap [`Self::PurgeCommit`].
    PurgeMaterialize,
    /// Mutation only, purge side: the reachability trace
    /// (`collect_purge_trace`) — per field, the legacy `history:` event read,
    /// and per target key the `tv:` chain walk that collects the atoms this
    /// purge may delete.
    ///
    /// Distinct from [`Self::PurgeMaterialize`] because the two scale on
    /// different axes and have different remedies: materialization is
    /// `O(fields x molecule cardinality)` and is fixed by narrowing the read,
    /// while the trace is `O(fields x legacy events)` plus
    /// `O(keys x chain depth)` and is fixed by the legacy-history memo and by
    /// chain length. On a store with no legacy `history:` rows the first term
    /// is zero, so a large value here is chain depth.
    ///
    /// Also reported by the request-thread complement plan. Unreported until
    /// 2026-10-09, it was ~30% of a one-record purge request.
    PurgeTrace,
    /// Mutation only, purge side: computing the two PRESERVE sets that decide
    /// what the batch may not delete — `collect_retained_chain_atoms_*` (atoms
    /// still named by tip chains outside the batch) and `collect_live_atom_uuids`
    /// (atoms still named by a live key after the targets are removed).
    ///
    /// Both are `O(fields x records)` over the materialized schema, and both
    /// are guards: they are the reason a batch purge cannot delete a live
    /// sibling. Timed separately so that "the guards are expensive" is a
    /// statement someone can check rather than assume — the safe direction
    /// for a destructive verb is to keep them, so a fix here has to make them
    /// cheaper, never optional.
    ///
    /// The request-thread complement plan reports its chain walk here too.
    PurgeRetentionGuard,
    /// Mutation only, purge side: the durable removal itself —
    /// `remove_molecule_keys` per touched field, the atom-body reads that
    /// count destroyed file pointers, the atom-partition/locator lookups, and
    /// the single `batch_delete` of every history, tip-version, atom, locator
    /// and schema-index key.
    ///
    /// This is the irreducible part: the bytes a purge exists to remove.
    /// Sized by what the batch actually deletes, so unlike every phase above
    /// it should scale with the RECORDS purged rather than with the schema.
    /// A `purge_delete` that tracks schema size instead is the signal that
    /// something reintroduced a whole-molecule rewrite (see
    /// `AtomStore::remove_molecule_keys` for the one this replaced).
    PurgeDelete,
    /// Mutation only, purge side: closing the commit — re-storing the schema
    /// with its shrunken molecules, reloading it into the schema cache,
    /// flushing the store, and committing the delete-ledger row.
    ///
    /// Still inside the guarded purge critical section. A large value here is
    /// catalog-body rewrite plus flush cost, the same shape
    /// [`Self::PersistSchema`] reports on the ordinary write path. It should
    /// be near zero once the catalog's skip-if-unchanged path holds.
    PurgeFinalize,
    /// Mutation only: time blocked acquiring the Phase 3 per-`(molecule,
    /// hash, range)` write gate, which serializes concurrent writers of the
    /// same molecule slot across the remainder of the write (including the
    /// deferred durable persist).
    ///
    /// Split out of [`Self::LockWait`] because the two gates sit at opposite
    /// ends of the pipeline, contend for different reasons, and have
    /// different fixes: `LockWait` is a purge running against the schema (or
    /// a same-key CAS writer), while this is same-slot write concurrency.
    /// Conflating them cost a full diagnosis cycle — fold #1104 narrowed this
    /// gate from per-molecule to per-`(molecule, hash)` and the primary could
    /// not show whether it helped, because the number it reported was the sum
    /// of a gate that changed and two that did not.
    ///
    /// A large value here means writers are queueing on ONE SLOT — one
    /// `(molecule, hash, range)` triple, not one hash. `e8839a4f2` narrowed
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
    /// small and the write is still slow, read [`Self::PurgeBarrier`] /
    /// [`Self::PurgeCommit`] and [`Self::Persist`] instead: on that same
    /// sample they were 42.8% and 11.4%.
    ///
    /// The gate-key shape is pinned by the `write_gate_key_separates_hashes_and_ranges`
    /// test in `fold_db_core::mutation_manager::molecules::persist`. That test
    /// and this paragraph are the two places a further narrowing must update
    /// together, and each names the other so neither can be changed alone —
    /// this doc stayed stale for 15 days after `e8839a4f2` because nothing
    /// linked them.
    MoleculeGate,
    /// Mutation only: the persisted-head reads that verify CAS expectations
    /// while the batch holds the locks above.
    ///
    /// Split from [`Self::LockWait`] deliberately — both sit in the same
    /// previously-unphased window, but one is contention and the other is
    /// storage IO, and they have opposite fixes. Conflating them would leave
    /// the next reader with the same "which is it?" question that made this
    /// window worth instrumenting.
    CasPrecondition,
    /// Query only: the cheap exact row count read from the key index that
    /// every push-down shape performs before hydrating anything.
    Count,
    /// Query only: row materialization, end to end. The PARENT total of the
    /// four `Hydrate*` steps below, which partition it exactly — unlike
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
    /// window push-down, and atom-body materialization for the matched rows.
    ///
    /// The only sub-step that touches storage, so it is the one that moves
    /// with `cold_shard_loads` and with resident-cache hit rate. Large here
    /// with loads at zero is in-memory materialization cost (molecule walk,
    /// decrypt, value decode), which is a different fix from large here with
    /// loads climbing — that is a placement or pruning problem, and
    /// `hash_range_write_cost_tests` bounds its write-side twin.
    HydrateAtoms,
    /// Query only: building the response envelopes — `records_from_field_map`
    /// plus one `serde_json` object per row carrying its fields, metadata and
    /// author key.
    ///
    /// Scales with `rows x fields` and with value width, and touches no
    /// storage at all. It is separated from [`Self::HydrateAtoms`] because the
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
    /// Query only: numeric and field conditions over the first pass's captured
    /// values, before ordering, the predicate limit and projected-value reads.
    /// This excludes key discovery and atom fetch time.
    HydrateFilter,
    /// Query only: per-field merge-conflict annotation of the formatted rows.
    /// Historically a per-molecule storage walk; expected to fall to ~0 once
    /// the home-conflict index (fold #1032) reaches a node.
    Annotate,
    /// In-memory application of the write — the RESIDUAL left after every
    /// other named write phase is subtracted from the batch's wall time. See
    /// [`crate::fold_db_core::mutation_manager::MutationManager::report_phase_totals`]
    /// for why this stays a residual rather than a sum of named steps.
    Apply,
    /// Mutation: the in-memory apply loop itself
    /// (`apply_mutations_to_molecules`) — walking this batch's atom results
    /// onto the resident molecules. Carved out of the `Apply` residual for
    /// the reason [`Self::Persist`]'s sub-steps were: the timer already ran
    /// in the batch write's `timing_breakdown`, and the residual then
    /// discarded it, so the phase that shares `apply`'s NAME could not be
    /// distinguished from everything else the residual absorbs.
    ///
    /// This is the step whose cost should track rows-under-the-touched
    /// molecules rather than rows written, so a hash-range schema with many
    /// rows per partition pays here where a point-keyed schema does not.
    ApplyMemory,
    /// Mutation: loading the schema at the top of the per-schema loop, before
    /// any molecule is touched. Distinct from [`Self::SchemaResolve`] (the
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
    /// Mutation: restoring molecules missing from the resident/in-memory
    /// graph for the keys this batch touches — storage IO, not compute, but
    /// reported here because the write path already measured it as a named
    /// step before this phase existed.
    RestoreMolecules,
    /// Mutation: field_hash protein-sibling fold — preparing tip updates to
    /// a shared atom when the entry molecule is protein-bound.
    ProteinSiblingFold,
    /// Mutation: grouping the batch's mutations by schema before the
    /// per-schema loop runs.
    Grouping,
    /// Mutation: the idempotency-duplicate filter pass at the top of the
    /// batch pipeline.
    IdempotencyCheck,
    /// Mutation: syncing in-memory molecule UUIDs back onto the schema
    /// before it is stored/reloaded.
    SyncUuids,
    /// Mutation: spawning the background index-mutation task (the spawn
    /// call itself, not the indexing work, which runs off-task and is
    /// deliberately not attributed here — see the task-local doc above).
    SpawnIndexing,
    /// Durable persist of the applied write, as a RESIDUAL: the durable-store
    /// time not attributed to one of the named sub-steps below. In practice
    /// that is the atom batch store, plus any future durable step added to
    /// the persist bucket without its own phase — visible here, if coarse,
    /// rather than vanishing.
    ///
    /// Carved up for the same reason [`Self::Apply`] was: `persist` summed
    /// four independently-sized durable stores under one name, so an operator
    /// reading it as the largest phase in the write path could not tell an
    /// irreducible atom write from a whole-molecule rewrite. The sub-timers
    /// already existed in the batch write's `timing_breakdown`; naming them
    /// does not add a timer.
    Persist,
    /// Mutation: the molecule batch store (`write_molecules_batch`). Sized by
    /// molecule cardinality rather than by the write, so a schema whose
    /// molecules hold tens of thousands of keys pays here on every mutation.
    PersistMolecules,
    /// Mutation: persisting the schema itself after a write (`schema_store`).
    /// Near zero once the catalog's skip-if-unchanged path applies; a large
    /// value here means the catalog body is being rewritten per mutation.
    PersistSchema,
    /// Mutation: storing the idempotency record for the applied batch.
    PersistIdempotency,
    /// Explicit store flush work. Native mutation disk-sequence waits stay
    /// inside the enclosing Apply residual; the receipt includes that wait.
    Flush,
    /// Mutation: cloud-sync mutation-log work on the caller's request path.
    /// This phase contains envelope encoding and bounded queue admission.
    /// Marker, mutation-log, and cleanup storage run on the capture worker.
    ///
    /// This wraps the whole batch write from OUTSIDE
    /// `write_create_update_batch_async`, so no residual inside that function
    /// could ever reach it: the `apply` residual is bounded by that
    /// function's own wall clock, and this work happens after it returns.
    /// It was therefore in no phase at all, and showed up only as the
    /// request-level `unphased=` remainder — measured on the primary
    /// 2026-08-17, **19.2% of kanban BoardCards mutation wall time** (262 s
    /// over 151 requests, 1.74 s each) and 15.1% of `brain` mutations, while
    /// every query key on the same node reported under 1%. That mutation-only
    /// split is what pointed here.
    ///
    /// Note what this phase is NOT: it is not storage or upload time. A large
    /// value now means envelope encode cost or capture queue pressure.
    SyncCapture,
    /// The handler's bounded wait for background index tasks.
    IndexWait,
    /// Post-request process-local outbox recording only. Durable notices
    /// join the core data commit and are prepared by [`Self::ChangeNoticePrepare`].
    ChangeRecord,
    /// Mutation: read the private feed tip, encode notices, and stage their
    /// event rows and tip in the product transaction. This excludes the
    /// shared disk commit, whose wait remains in [`Self::Apply`].
    ChangeNoticePrepare,
    /// Final response envelope/error rendering after route work completes.
    ResponseEnvelope,
    /// Status route: cloud-sync health snapshot.
    StatusSync,
    /// Status route: sealed-chunk backup progress snapshot.
    StatusBackup,
    /// Status route: durable backup marker evaluation.
    StatusDurability,
    /// Status route: recursive data-directory sizing.
    StatusDataDir,
    /// Status route: request-ops ring/aggregate snapshot copying.
    StatusRequestOps,
}
