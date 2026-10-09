//! Task-local accumulation of per-request phase timings.
//!
//! The request-ops telemetry (`lastdb_node::request_telemetry::PhaseTimings`)
//! wants to know *where inside the node* a request spent its time. For a
//! mutation that is QoS admission wait, parse, schema resolve, validate,
//! write-gate `lock_wait`, `molecule_gate`, `cas_precondition`, apply,
//! persist, flush, cloud-sync capture,
//! background index wait, change recording, and response envelope rendering.
//! For a query it is admission wait, parse, schema resolve, the index
//! `count`, row `hydrate`, and conflict `annotate`. For status it is the
//! snapshot components that can otherwise hide synchronous ops-surface work:
//! sync health, backup progress, durability marker evaluation, data-dir sizing,
//! and request-ops snapshot copying.
//! Those phases are measured in three different
//! crates (`lastdb_node` parses, `lastdb_host` resolves/validates/admits,
//! this crate applies/persists), so threading a collector parameter through
//! every signature would churn public APIs for a diagnostic.
//!
//! Instead, the socket executor wraps request dispatch in
//! [`run_with_phases`] and every recording site calls [`add_phase`], which
//! silently no-ops when no scope is present (background jobs, tests, the
//! desktop node's non-instrumented surfaces). Precedent:
//! [`crate::user_context::run_with_user`], which propagates the request's
//! user id over exactly the same task-local seam.
//!
//! `tokio::spawn` does NOT inherit task-locals, which is load-bearing here:
//! spawned background work (e.g. index embedding) must not attribute its
//! time to whatever request happens to be in scope. Only the awaited
//! request path accumulates.
//!
//! # Counts, alongside the timings
//!
//! A phase total answers "how long", never "how much work". That gap is not
//! academic: measured on the primary 2026-08-17, `persist_molecules` for
//! kanban BoardCards mutations read 7.80 s/req at 15:0xZ and 2.27 s/req at
//! 17:1xZ — a 3.4x move with NO change to that code path in the running
//! binary (fold #1546, which restructures it, is not an ancestor of the
//! running build). Node state alone moves the wall clock by more than the
//! optimisation being evaluated is expected to save, so a before/after read
//! of the phase cannot attribute anything.
//!
//! [`RequestCounter`] is the companion: a small set of *counts of work
//! issued*, accumulated over the same task-local seam. A count is immune to
//! IO contention, page-cache state and concurrent load, so it answers
//! structural questions ("did this mutation issue one durable molecule
//! commit or twenty-four?") that the clock cannot answer at all.

use std::cell::RefCell;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// A bounded estimate of distinct molecule-tip keys in resolved field results.
/// Only register ranks survive; no molecule or record key is retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TipKeySketch {
    registers: [u8; 1024],
}

impl Default for TipKeySketch {
    fn default() -> Self {
        Self {
            registers: [0; 1024],
        }
    }
}

impl TipKeySketch {
    const PRECISION: u32 = 10;

    pub fn insert(&mut self, molecule_uuid: &str, hash: &str, range: &str) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (molecule_uuid, hash, range).hash(&mut hasher);
        let fingerprint = hasher.finish();
        let index = (fingerprint >> (64 - Self::PRECISION)) as usize;
        let remainder = fingerprint << Self::PRECISION;
        let rank = (remainder.leading_zeros() + 1).min(65 - Self::PRECISION) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    pub fn merge(&mut self, other: &Self) {
        for (left, right) in self.registers.iter_mut().zip(other.registers.iter()) {
            *left = (*left).max(*right);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.registers.iter().all(|rank| *rank == 0)
    }

    /// HyperLogLog estimate; linear correction keeps small key sets useful.
    pub fn estimate(&self) -> u64 {
        let m = self.registers.len() as f64;
        let zeroes = self.registers.iter().filter(|rank| **rank == 0).count();
        if zeroes == self.registers.len() {
            return 0;
        }
        let sum: f64 = self
            .registers
            .iter()
            .map(|rank| 2f64.powi(-i32::from(*rank)))
            .sum();
        let raw = 0.720540758 * m * m / sum;
        let estimate = if raw <= 2.5 * m && zeroes > 0 {
            m * (m / zeroes as f64).ln()
        } else {
            raw
        };
        estimate.round() as u64
    }
}

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
    /// Query only: the post-hoc value-filter retain applied after the sort.
    ///
    /// Normally zero — the executor's `apply_value_filters` already dropped
    /// every row whose concrete value failed — so a non-zero reading here is
    /// itself information: it means rows survived the executor's filter and
    /// were dropped only after being hydrated, formatted and sorted.
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
    /// Mutation: the optional per-molecule write-dedupe pass
    /// (`LASTDB_WRITE_DEDUPE`, off by default) that drops field writes whose
    /// value already equals the stored tip. Zero when the feature is off —
    /// which is itself the reading worth having, since a zero here means the
    /// downstream savings it exists to produce are not being taken.
    DedupeScan,
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
    /// Store flush (near zero unless `LASTDB_MUTATION_SYNC_FLUSH=1`).
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
    /// Post-write process-local outbox recording only. Until
    /// `lastdb-change-record-is-a-third-of-all-write-time-20260803`, this
    /// also covered the durable change-feed append below; that work is now
    /// [`Self::ChangeRecordLockWait`] / [`Self::ChangeRecordWrite`], so a drop
    /// here at the upgrade boundary is the split, not a regression cured.
    ChangeRecord,
    /// Mutation only: time blocked acquiring the change feed's single global
    /// `tip` mutex, which every successful mutation on the node passes
    /// through to append its durable change-feed event — the one lock this
    /// node has that is not scoped to a schema, key, or molecule.
    ///
    /// A large value here means writers are queueing behind EVERY OTHER
    /// writer on the node, not a same-key or same-schema peer: measured
    /// 2026-08-03, this queueing was 33% of all mutation wall time. Split out
    /// so the queueing (this phase) is distinguishable from the storage IO
    /// done while holding the lock ([`Self::ChangeRecordWrite`]) — the two
    /// have different fixes, same as [`Self::LockWait`] vs
    /// [`Self::MoleculeGate`].
    ChangeRecordLockWait,
    /// Mutation only: the durable change-feed batch write (event + tip) done
    /// while holding the mutex above. Storage IO, not queueing.
    ChangeRecordWrite,
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

/// Accumulated per-phase totals for one request, in microseconds.
///
/// Fields are microseconds because several phases are sub-millisecond and
/// would read as constant zeroes in ms. Zero means "not reported": every
/// consumer treats an all-zero set as absent, so recording sites never need
/// a "was anything measured" side channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestPhaseTotals {
    pub admission_wait_us: u64,
    pub parse_us: u64,
    pub schema_resolve_us: u64,
    pub validate_us: u64,
    pub lock_wait_us: u64,
    pub purge_barrier_us: u64,
    pub purge_plan_us: u64,
    pub purge_commit_us: u64,
    pub purge_materialize_us: u64,
    pub purge_trace_us: u64,
    pub purge_retention_guard_us: u64,
    pub purge_delete_us: u64,
    pub purge_finalize_us: u64,
    pub molecule_gate_us: u64,
    pub cas_precondition_us: u64,
    pub count_us: u64,
    pub hydrate_us: u64,
    pub hydrate_atoms_us: u64,
    pub hydrate_format_us: u64,
    pub hydrate_sort_us: u64,
    pub hydrate_filter_us: u64,
    pub annotate_us: u64,
    pub apply_us: u64,
    pub apply_memory_us: u64,
    pub schema_load_us: u64,
    pub dedupe_scan_us: u64,
    pub schema_reload_us: u64,
    pub restore_molecules_us: u64,
    pub protein_sibling_fold_us: u64,
    pub grouping_us: u64,
    pub idempotency_check_us: u64,
    pub sync_uuids_us: u64,
    pub spawn_indexing_us: u64,
    pub persist_us: u64,
    pub persist_molecules_us: u64,
    pub persist_schema_us: u64,
    pub persist_idempotency_us: u64,
    pub flush_us: u64,
    pub sync_capture_us: u64,
    pub index_wait_us: u64,
    pub change_record_us: u64,
    pub change_record_lock_wait_us: u64,
    pub change_record_write_us: u64,
    pub response_envelope_us: u64,
    pub status_sync_us: u64,
    pub status_backup_us: u64,
    pub status_durability_us: u64,
    pub status_data_dir_us: u64,
    pub status_request_ops_us: u64,
}

/// One countable unit of request work — "how much", where [`RequestPhase`] is
/// "how long".
///
/// Deliberately small. A counter earns a place here only when a wall-clock
/// phase already exists for the same work AND that phase is known to move
/// more from node state than from the code being evaluated. Every variant is
/// counted at the site that issues the work, so it is exact for the request
/// rather than a store-wide gauge sampled around it (contrast
/// `OpSample::cold_shard_loads`, which is store-wide and therefore charged
/// for concurrent callers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestCounter {
    /// Unanchored product reads rejected by this request before group access.
    PartitionReadRejections,
    /// Explicit startup/admin physical passes issued by this request.
    AllGroupWalks,
    /// Field molecules this request handed to the durable store.
    ///
    /// The denominator `persist_molecules_us` never had. One mutation touches
    /// one molecule PER FIELD, so this is the schema's modified-field count,
    /// not a constant: measured on the primary 2026-08-17, 55 of 195
    /// mutations stored 24 apiece (BoardCards' 24 fields) against a mean of
    /// 11.98 and a median of 7. Without it, `persist_molecules_us / count` is
    /// a per-request figure whose unit of work is unknown, and two windows
    /// with different field mixes are not comparable.
    MoleculesPersisted,
    /// Awaited durable molecule-store operations this request issued.
    ///
    /// Counted at each call boundary in the molecule persist path — one per
    /// shared batch commit, one per single-molecule commit, and one more for
    /// the delete half of a full rewrite. This is the number the batch path
    /// exists to reduce: with every molecule eligible it is 1 regardless of
    /// field count, and with none eligible it is at least
    /// [`Self::MoleculesPersisted`].
    ///
    /// Read as a RATIO against `MoleculesPersisted`: `24/24` is the
    /// per-molecule path, `24/1` is the batch path, and anything between
    /// names how many molecules fell out of the batch's eligibility window
    /// (absent header, or a full order snapshot rather than an append tail).
    /// That ratio is what makes the restructuring verifiable on a live node
    /// without a wall-clock A/B the node's own variance would swamp.
    MoleculeStoreCommits,
    /// Logical resident commits this request published — one per caller batch
    /// that reached the apply gates, not one per mutation or per schema.
    ///
    /// The number the one-resident-commit design is defined by. A Brain write
    /// carrying a primary record plus eight exact projections is CORRECT at 1
    /// and is the old serial repair path at 9, and no wall-clock phase can
    /// tell those apart: both spend their time in the same buckets, and the
    /// serial shape is often the *faster* one per commit while being nine
    /// times the work. Read with [`Self::ResidentOperations`].
    ///
    /// A count rather than a phase on purpose — see
    /// [`crate::fold_db_core::mutation_manager::receipt`] for why the resident
    /// stage CLOCKS cannot join [`RequestPhaseTotals`] without corrupting the
    /// `unattributed` remainder, and why the counts can.
    ResidentCommits,
    /// Operations carried by those commits: created + updated + deleted +
    /// recognised-duplicate rows.
    ///
    /// The denominator for [`Self::ResidentCommits`]. `9/1` is one resident
    /// commit carrying a primary record and eight projections; `9/9` is nine
    /// separate commits doing the same work, which is the shape the design
    /// replaces. Idempotency duplicates are counted here because a batch that
    /// was entirely duplicates still cost the node an idempotency scan, and
    /// omitting them would report that request as having issued no work at all.
    ResidentOperations,
}

/// Accumulated per-request work counts.
///
/// Zero means "not reported", exactly as for [`RequestPhaseTotals`]: consumers
/// treat an all-zero set as absent, so recording sites need no "was anything
/// counted" side channel. `Copy` for the same reason the phase set is — the
/// telemetry ring clones whole samples repeatedly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestCounts {
    pub partition_read_rejections: u64,
    pub all_group_walks: u64,
    pub molecules_persisted: u64,
    pub molecule_store_commits: u64,
    pub resident_commits: u64,
    pub resident_operations: u64,
}

impl RequestCounts {
    /// True when nothing was counted, so surfaces can omit the set entirely
    /// and stay byte-identical to pre-counter output.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Everything one request accumulates: the phase clock and the work counts.
///
/// ONE task-local cell rather than two, for two reasons. It makes it
/// impossible for a request path to be inside one scope and outside the other
/// — [`add_counter`] would then report a structural zero indistinguishable
/// from "this path issues no durable commits". And nesting a second
/// `task_local::scope` around request dispatch pushed the compiler past its
/// query-depth limit on two `lastdb_node` integration tests (`local_watch_admission`,
/// `mutation_phase_instrumentation`): each `scope` wraps the whole request
/// future in another generic future type, and that future is already ~130
/// layers deep. Widening the payload costs no type depth at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RequestMetrics {
    phases: RequestPhaseTotals,
    counts: RequestCounts,
    tip_keys: TipKeySketch,
}

tokio::task_local! {
    static REQUEST_METRICS: RefCell<RequestMetrics>;
}

/// Saturating `Duration` → whole microseconds for phase fields.
#[must_use]
pub fn duration_us(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// Run `f` with fresh phase and count accumulators in scope and return its
/// output alongside whatever the recording sites accumulated.
///
/// One scope covers both — see [`RequestMetrics`] for why that is a
/// correctness property and not just tidiness.
pub async fn run_with_phases<F>(f: F) -> (F::Output, RequestPhaseTotals, RequestCounts)
where
    F: Future,
{
    let (output, phases, counts, _) = run_with_phases_and_keys(f).await;
    (output, phases, counts)
}

/// Return the request's distinct-tip sketch beside its existing metrics.
pub async fn run_with_phases_and_keys<F>(
    f: F,
) -> (F::Output, RequestPhaseTotals, RequestCounts, TipKeySketch)
where
    F: Future,
{
    REQUEST_METRICS
        .scope(RefCell::new(RequestMetrics::default()), async move {
            let output = f.await;
            let metrics = REQUEST_METRICS.with(|cell| cell.borrow().clone());
            (output, metrics.phases, metrics.counts, metrics.tip_keys)
        })
        .await
}

/// Count one logical molecule tip returned by a field read in a socket request.
pub fn record_tip_key(molecule_uuid: &str, hash: &str, range: &str) {
    let _ = REQUEST_METRICS.try_with(|cell| {
        cell.borrow_mut()
            .tip_keys
            .insert(molecule_uuid, hash, range);
    });
}

/// Accumulate `n` into `counter` for the request in scope. Outside a
/// [`run_with_phases`] scope this is a silent no-op, for the same reason
/// [`add_phase_us`] is: recording sites sit deep in the storage path and must
/// not care whether their caller is an instrumented socket request.
pub fn add_counter(counter: RequestCounter, n: u64) {
    // Adding zero is the identity; skip the task-local access entirely so a
    // no-op mutation (nothing modified) costs nothing.
    if n == 0 {
        return;
    }
    let _ = REQUEST_METRICS.try_with(|cell| {
        let mut metrics = cell.borrow_mut();
        let slot = match counter {
            RequestCounter::PartitionReadRejections => {
                &mut metrics.counts.partition_read_rejections
            }
            RequestCounter::AllGroupWalks => &mut metrics.counts.all_group_walks,
            RequestCounter::MoleculesPersisted => &mut metrics.counts.molecules_persisted,
            RequestCounter::MoleculeStoreCommits => &mut metrics.counts.molecule_store_commits,
            RequestCounter::ResidentCommits => &mut metrics.counts.resident_commits,
            RequestCounter::ResidentOperations => &mut metrics.counts.resident_operations,
        };
        *slot = slot.saturating_add(n);
    });
}

/// Accumulate `elapsed` into `phase` for the request in scope. Outside a
/// [`run_with_phases`] scope this is a silent no-op — recording sites never
/// need to know whether the caller is an instrumented socket request.
pub fn add_phase(phase: RequestPhase, elapsed: Duration) {
    add_phase_us(phase, duration_us(elapsed));
}

/// [`add_phase`] with a pre-converted microsecond value. Saturating: a
/// request cannot wrap its own phase counter no matter how long it runs.
pub fn add_phase_us(phase: RequestPhase, us: u64) {
    // Sub-microsecond measurements truncate to 0 and adding 0 is the
    // identity, but skipping the task-local access entirely keeps the
    // common "phase too fast to matter" case free.
    if us == 0 {
        return;
    }
    let _ = REQUEST_METRICS.try_with(|cell| {
        let mut metrics = cell.borrow_mut();
        let totals = &mut metrics.phases;
        let slot = match phase {
            RequestPhase::AdmissionWait => &mut totals.admission_wait_us,
            RequestPhase::Parse => &mut totals.parse_us,
            RequestPhase::SchemaResolve => &mut totals.schema_resolve_us,
            RequestPhase::Validate => &mut totals.validate_us,
            RequestPhase::LockWait => &mut totals.lock_wait_us,
            RequestPhase::PurgeBarrier => &mut totals.purge_barrier_us,
            RequestPhase::PurgePlan => &mut totals.purge_plan_us,
            RequestPhase::PurgeCommit => &mut totals.purge_commit_us,
            RequestPhase::PurgeMaterialize => &mut totals.purge_materialize_us,
            RequestPhase::PurgeTrace => &mut totals.purge_trace_us,
            RequestPhase::PurgeRetentionGuard => &mut totals.purge_retention_guard_us,
            RequestPhase::PurgeDelete => &mut totals.purge_delete_us,
            RequestPhase::PurgeFinalize => &mut totals.purge_finalize_us,
            RequestPhase::MoleculeGate => &mut totals.molecule_gate_us,
            RequestPhase::CasPrecondition => &mut totals.cas_precondition_us,
            RequestPhase::Count => &mut totals.count_us,
            RequestPhase::Hydrate => &mut totals.hydrate_us,
            RequestPhase::HydrateAtoms => &mut totals.hydrate_atoms_us,
            RequestPhase::HydrateFormat => &mut totals.hydrate_format_us,
            RequestPhase::HydrateSort => &mut totals.hydrate_sort_us,
            RequestPhase::HydrateFilter => &mut totals.hydrate_filter_us,
            RequestPhase::Annotate => &mut totals.annotate_us,
            RequestPhase::Apply => &mut totals.apply_us,
            RequestPhase::ApplyMemory => &mut totals.apply_memory_us,
            RequestPhase::SchemaLoad => &mut totals.schema_load_us,
            RequestPhase::DedupeScan => &mut totals.dedupe_scan_us,
            RequestPhase::SchemaReload => &mut totals.schema_reload_us,
            RequestPhase::RestoreMolecules => &mut totals.restore_molecules_us,
            RequestPhase::ProteinSiblingFold => &mut totals.protein_sibling_fold_us,
            RequestPhase::Grouping => &mut totals.grouping_us,
            RequestPhase::IdempotencyCheck => &mut totals.idempotency_check_us,
            RequestPhase::SyncUuids => &mut totals.sync_uuids_us,
            RequestPhase::SpawnIndexing => &mut totals.spawn_indexing_us,
            RequestPhase::Persist => &mut totals.persist_us,
            RequestPhase::PersistMolecules => &mut totals.persist_molecules_us,
            RequestPhase::PersistSchema => &mut totals.persist_schema_us,
            RequestPhase::PersistIdempotency => &mut totals.persist_idempotency_us,
            RequestPhase::Flush => &mut totals.flush_us,
            RequestPhase::SyncCapture => &mut totals.sync_capture_us,
            RequestPhase::IndexWait => &mut totals.index_wait_us,
            RequestPhase::ChangeRecord => &mut totals.change_record_us,
            RequestPhase::ChangeRecordLockWait => &mut totals.change_record_lock_wait_us,
            RequestPhase::ChangeRecordWrite => &mut totals.change_record_write_us,
            RequestPhase::ResponseEnvelope => &mut totals.response_envelope_us,
            RequestPhase::StatusSync => &mut totals.status_sync_us,
            RequestPhase::StatusBackup => &mut totals.status_backup_us,
            RequestPhase::StatusDurability => &mut totals.status_durability_us,
            RequestPhase::StatusDataDir => &mut totals.status_data_dir_us,
            RequestPhase::StatusRequestOps => &mut totals.status_request_ops_us,
        };
        *slot = slot.saturating_add(us);
    });
}

/// Transfer purge work from a persist-lane task to the request that awaited it.
/// Ordinary asynchronous deletes do not call this function.
pub(crate) fn add_purge_totals(totals: &RequestPhaseTotals) {
    for (phase, us) in [
        (RequestPhase::PurgeBarrier, totals.purge_barrier_us),
        (RequestPhase::PurgePlan, totals.purge_plan_us),
        (RequestPhase::PurgeCommit, totals.purge_commit_us),
        (RequestPhase::PurgeMaterialize, totals.purge_materialize_us),
        (RequestPhase::PurgeTrace, totals.purge_trace_us),
        (
            RequestPhase::PurgeRetentionGuard,
            totals.purge_retention_guard_us,
        ),
        (RequestPhase::PurgeDelete, totals.purge_delete_us),
        (RequestPhase::PurgeFinalize, totals.purge_finalize_us),
    ] {
        add_phase_us(phase, us);
    }
}
