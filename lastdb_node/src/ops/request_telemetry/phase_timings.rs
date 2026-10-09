use super::*;

/// Number of phases in [`PhaseTimings`] — see [`PhaseTimings::PHASE_NAMES`].
pub(super) const PHASE_COUNT: usize = 50;

/// Per-phase timing breakdown for one request, in microseconds.
///
/// The model half of mutation phase observability: once the mutation path
/// reports them, these fields answer *where inside the node* a slow request
/// spent its time — waiting in the socket queue, waiting at the QoS
/// admission gate, queueing at a write gate (`lock_wait`), or doing parse /
/// schema-resolve / validate / cas-precondition / apply /
/// persist / flush / index-wait / change-record / response-envelope work.
/// This struct is the shared vocabulary; the recording sites land separately.
///
/// Invariants downstream code depends on:
///
/// - **Microseconds** (`_us`): several phases are sub-millisecond and would
///   read as constant zeroes in ms.
/// - **Fixed-size numerics only** — snapshots clone the whole sample ring
///   repeatedly, so a phase set must stay a cheap `Copy`.
/// - **All-zero means absent, not measured-as-zero**: serde omits empty
///   sets (and zero fields within a set), and [`Self::detail`] renders the
///   empty string, so surfaces stay byte-identical until a phase is
///   actually reported.
/// - On aggregates phases are **field-wise sums, never maxes** — the
///   durable rollup's delta writer subtracts consecutive cumulative
///   snapshots, and only sums survive subtraction honestly
///   ([`Self::saturating_sub`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseTimings {
    /// Socket-queue wait before a worker picked the connection up.
    ///
    /// **Outside the request's wall clock.** `execute_data_route` reads this
    /// from the worker pool's thread-local and only THEN starts the
    /// `Instant` that becomes `duration_ms`, so this phase measures time
    /// that elapsed before the interval every other phase divides up. It is
    /// therefore excluded from [`Self::within_wall_us`] — see that method
    /// for what happens when it is not.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub queue_wait_us: u64,
    /// QoS admission-gate wait — time spent queued behind the write
    /// governor, previously invisible and charged as handler work.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub admission_wait_us: u64,
    /// Request body parse.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub parse_us: u64,
    /// Schema resolution.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub schema_resolve_us: u64,
    /// Mutation validation.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub validate_us: u64,
    /// Mutation: time blocked acquiring the per-`(schema, key)` CAS mutex
    /// before any pipeline step runs. Queueing behind same-key writers, not
    /// work — the remedy is the caller's key layout. A batch carrying no CAS
    /// expectation takes no per-key mutex and reports zero here.
    ///
    /// Series note for rollup readers: before the `purge_barrier_us` split
    /// this number also carried the per-schema purge barrier, so a
    /// `lock_wait` drop at that upgrade boundary is the split, not a
    /// regression cured.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub lock_wait_us: u64,
    /// Mutation, purge side: time blocked before an EXCLUSIVE acquisition of
    /// the per-schema guarded-purge barrier.
    ///
    /// Ordinary writes do not acquire this barrier and report zero here. A
    /// large value identifies concurrent guarded purges with one schema name.
    /// `purge_commit_us` measures the critical section after acquisition.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_barrier_us: u64,
    /// Mutation, purge side: the unlocked reachability trace that decides
    /// whether there is anything to delete. A read, paid by this caller
    /// alone; under the `Skip` policy a trace that finds nothing skips the
    /// exclusive acquire entirely. Large here means a scan problem.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_plan_us: u64,
    /// Mutation, purge side: the destructive snapshot→delete performed while
    /// holding the per-schema purge barrier EXCLUSIVELY. Ordinary writes do
    /// not wait for this work. Summed across the batch's chunks, this is the
    /// total critical-section time, not the longest single section.
    ///
    /// Series note for rollup readers: rows written before 0.23.3 read back
    /// as 0 here while carrying the same work inside the request's
    /// unattributed remainder, so an `apply`/unphased drop at that boundary
    /// is the split, not a regression cured.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_commit_us: u64,
    /// Mutation, purge side: materializing every field molecule of the schema
    /// before the trace and the guards read it. Sized by the schema
    /// (`fields x molecule cardinality`), not by the records being removed —
    /// a one-record purge of a wide, high-cardinality schema pays the same as
    /// a thousand-record one. Large here is a read-width problem, and the
    /// unlocked probe already shows what narrowing it is worth.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_materialize_us: u64,
    /// Mutation, purge side: the reachability trace under the barrier —
    /// legacy `history:` events per field plus the `tv:` chain walk per target
    /// key. On a store with no legacy rows this is chain depth alone.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_trace_us: u64,
    /// Mutation, purge side: the two preserve sets (retained chain atoms and
    /// live atom uuids) that keep a batch from deleting a live sibling. Both
    /// `O(fields x records)`. A guard, so the fix is to make it cheaper, never
    /// to drop it.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_retention_guard_us: u64,
    /// Mutation, purge side: the durable removal — per-field molecule-key
    /// removal, file-pointer accounting reads, and the single `batch_delete`.
    /// The only purge phase that should scale with RECORDS PURGED rather than
    /// with schema size.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_delete_us: u64,
    /// Mutation, purge side: schema re-store, schema-cache reload, flush and
    /// delete-ledger commit inside the guarded purge critical section.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub purge_finalize_us: u64,
    /// Mutation: time blocked acquiring the Phase 3 per-`(molecule, hash)`
    /// write gate, held across the rest of the write including the deferred
    /// durable persist.
    ///
    /// Reported apart from `lock_wait_us` since 0.23.2: the two gates sit at
    /// opposite ends of the pipeline and have different remedies, and summing
    /// them hid whether narrowing this one (fold #1104) helped. Series note
    /// for rollup readers — before this split, this gate's time was included
    /// in `lock_wait_us`, so a `lock_wait` drop at the upgrade boundary is
    /// the split, not a regression cured.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub molecule_gate_us: u64,
    /// Mutation: persisted-head reads verifying CAS expectations while the
    /// batch holds those locks. Storage IO; split from `lock_wait_us`
    /// because contention and read cost have opposite fixes.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub cas_precondition_us: u64,
    /// Query: the exact row count read from the key index ahead of the
    /// push-down. Large relative to `hydrate` means the partition is being
    /// counted more expensively than the page is being served.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub count_us: u64,
    /// Query: row materialization end to end — the PARENT total that the four
    /// `hydrate_*` fields below partition exactly. Kept so rollup rows written
    /// before 0.23.4 stay comparable with rows written after; read the
    /// sub-steps to decide what to fix.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub hydrate_us: u64,
    /// Query: the executor call — key-index resolution, window push-down and
    /// atom-body materialization. The only hydrate sub-step that touches
    /// storage, so it is the one that tracks `sum_cold_shard_loads`.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub hydrate_atoms_us: u64,
    /// Query: response-envelope build, `O(rows x fields)` and storage-free.
    /// Large here with `hydrate_atoms` small means the read is cheap and the
    /// rendering is not.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub hydrate_format_us: u64,
    /// Query: the deterministic total-order sort that makes offset/limit
    /// pagination non-overlapping. `O(rows log rows)` JSON-keyed comparisons.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub hydrate_sort_us: u64,
    /// Query: post-sort value-filter retain. Normally zero; non-zero means
    /// rows were hydrated, formatted and sorted before being dropped.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub hydrate_filter_us: u64,
    /// Query: per-field merge-conflict annotation of the formatted rows.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub annotate_us: u64,
    /// In-memory application of the write — the RESIDUAL left after every
    /// other named write phase below is subtracted from the batch's wall
    /// time, not a sum of named steps. See
    /// `fold_db::request_phases::RequestPhase::Apply`.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub apply_us: u64,
    /// Mutation: the in-memory apply loop itself — walking the batch's atom
    /// results onto the resident molecules. Sized by the molecules touched
    /// rather than by the rows written, so read this first when one schema's
    /// `apply` cost is orders of magnitude above another's on the same node.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub apply_memory_us: u64,
    /// Mutation: loading the schema at the top of the per-schema loop.
    /// Distinct from `schema_resolve` (the handler's name lookup) and
    /// `schema_reload` (the write-back afterwards).
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub schema_load_us: u64,
    /// Mutation: the optional write-dedupe pass (`LASTDB_WRITE_DEDUPE`, off
    /// by default) that drops field writes already equal to the stored tip.
    /// Zero means the feature is off and its downstream savings are not taken.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub dedupe_scan_us: u64,
    /// Mutation: the idempotency-duplicate filter pass at the top of the
    /// batch pipeline. Previously folded into `apply_us` unreported outside
    /// debug logging.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub idempotency_check_us: u64,
    /// Mutation: grouping the batch's mutations by schema.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub grouping_us: u64,
    /// Mutation: restoring molecules missing from the resident graph for the
    /// keys this batch touches. Storage IO.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub restore_molecules_us: u64,
    /// Mutation: field_hash protein-sibling fold — preparing tip updates to
    /// a shared atom when the entry molecule is protein-bound.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub protein_sibling_fold_us: u64,
    /// Mutation: spawning the background index-mutation task (the spawn call
    /// itself, not the off-task indexing work).
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub spawn_indexing_us: u64,
    /// Mutation: syncing in-memory molecule UUIDs onto the schema before it
    /// is stored/reloaded.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub sync_uuids_us: u64,
    /// Mutation: schema reload back into the schema manager after a write.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub schema_reload_us: u64,
    /// Durable persist of the applied write, as a residual: the durable-store
    /// time not attributed to one of the three named sub-steps below. In
    /// practice the atom batch store.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub persist_us: u64,
    /// Mutation: the molecule batch store. Sized by molecule cardinality, not
    /// by the write, so a schema whose molecules hold tens of thousands of
    /// keys pays here on every mutation.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub persist_molecules_us: u64,
    /// Mutation: persisting the schema after a write. Near zero once the
    /// catalog's skip-if-unchanged path applies.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub persist_schema_us: u64,
    /// Mutation: storing the idempotency record for the applied batch.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub persist_idempotency_us: u64,
    /// Store flush. Near zero on default builds (background flusher);
    /// meaningful only under synchronous-flush configurations.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub flush_us: u64,
    /// Mutation: cloud-sync mutation-log capture on the request path —
    /// envelope encode plus the post-commit durable pin-log append. NOT
    /// upload time: `record_op` returns once the op is durable locally and
    /// admitted to the bounded upload queue, and the network cycle runs
    /// off-task.
    ///
    /// Absent before 0.23.3: this work wraps the batch write from outside
    /// `write_create_update_batch_async`, so no residual inside that function
    /// could reach it and it landed in no phase at all. It surfaced only as
    /// the request-level `unphased=` remainder — 19.2% of kanban BoardCards
    /// mutation wall time on the primary, 2026-08-17, against under 1% on
    /// every query key. A rollup row from an older binary reads 0 here; that
    /// is unmigrated, not measured-as-zero.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub sync_capture_us: u64,
    /// The handler's bounded wait for background index tasks — NOT the
    /// off-thread index/embedding work itself, which never blocks the
    /// request.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub index_wait_us: u64,
    /// Post-write process-local outbox recording only. Since 0.23.2 the
    /// durable change-feed append below is `change_record_lock_wait_us` /
    /// `change_record_write_us`; a drop here at the upgrade boundary is the
    /// split, not a regression cured.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub change_record_us: u64,
    /// Mutation: time blocked acquiring the change-feed's single global `tip`
    /// mutex — the one lock on the node not scoped to a schema, key, or
    /// molecule. Every successful mutation passes through it, so a large
    /// value here is every writer on the node queueing behind every other
    /// one, not a same-key or same-schema peer.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub change_record_lock_wait_us: u64,
    /// Mutation: the durable change-feed batch write (event + tip) done while
    /// holding the mutex above. Storage IO, split from
    /// `change_record_lock_wait_us` because contention and write cost have
    /// different fixes.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub change_record_write_us: u64,
    /// Final response envelope/error rendering after route work completes.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub response_envelope_us: u64,
    /// Status route: cloud-sync health snapshot.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub status_sync_us: u64,
    /// Status route: sealed-chunk backup progress snapshot.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub status_backup_us: u64,
    /// Status route: durable backup marker evaluation.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub status_durability_us: u64,
    /// Status route: recursive data-directory sizing.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub status_data_dir_us: u64,
    /// Status route: request-ops ring/aggregate snapshot copying.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub status_request_ops_us: u64,
}

impl PhaseTimings {
    /// Stable phase names in pipeline order. This is the wire vocabulary:
    /// serde fields are `<name>_us` and durable rollup fields derive from
    /// these names — never rename or remove one.
    ///
    /// A NEW name may be inserted in pipeline order rather than appended:
    /// every consumer keys off the name, not the position (serde renders
    /// named fields, `SELF_METRIC_FIELDS` lists `sum_<name>_us` and migrates
    /// additively, and `named_us` zips this array with `as_array`). Keeping
    /// the order meaningful is what makes a rendered `phases[…]` line read as
    /// the pipeline it describes.
    pub const PHASE_NAMES: [&'static str; PHASE_COUNT] = [
        "queue_wait",
        "admission_wait",
        "parse",
        "schema_resolve",
        "validate",
        "lock_wait",
        "purge_barrier",
        "purge_plan",
        "purge_commit",
        "purge_materialize",
        "purge_trace",
        "purge_retention_guard",
        "purge_delete",
        "purge_finalize",
        "molecule_gate",
        "cas_precondition",
        "count",
        "hydrate",
        "hydrate_atoms",
        "hydrate_format",
        "hydrate_sort",
        "hydrate_filter",
        "annotate",
        "apply",
        "apply_memory",
        "schema_load",
        "dedupe_scan",
        "idempotency_check",
        "grouping",
        "restore_molecules",
        "protein_sibling_fold",
        "spawn_indexing",
        "sync_uuids",
        "schema_reload",
        "persist",
        "persist_molecules",
        "persist_schema",
        "persist_idempotency",
        "flush",
        "sync_capture",
        "index_wait",
        "change_record",
        "change_record_lock_wait",
        "change_record_write",
        "response_envelope",
        "status_sync",
        "status_backup",
        "status_durability",
        "status_data_dir",
        "status_request_ops",
    ];

    pub(super) fn as_array(&self) -> [u64; PHASE_COUNT] {
        [
            self.queue_wait_us,
            self.admission_wait_us,
            self.parse_us,
            self.schema_resolve_us,
            self.validate_us,
            self.lock_wait_us,
            self.purge_barrier_us,
            self.purge_plan_us,
            self.purge_commit_us,
            self.purge_materialize_us,
            self.purge_trace_us,
            self.purge_retention_guard_us,
            self.purge_delete_us,
            self.purge_finalize_us,
            self.molecule_gate_us,
            self.cas_precondition_us,
            self.count_us,
            self.hydrate_us,
            self.hydrate_atoms_us,
            self.hydrate_format_us,
            self.hydrate_sort_us,
            self.hydrate_filter_us,
            self.annotate_us,
            self.apply_us,
            self.apply_memory_us,
            self.schema_load_us,
            self.dedupe_scan_us,
            self.idempotency_check_us,
            self.grouping_us,
            self.restore_molecules_us,
            self.protein_sibling_fold_us,
            self.spawn_indexing_us,
            self.sync_uuids_us,
            self.schema_reload_us,
            self.persist_us,
            self.persist_molecules_us,
            self.persist_schema_us,
            self.persist_idempotency_us,
            self.flush_us,
            self.sync_capture_us,
            self.index_wait_us,
            self.change_record_us,
            self.change_record_lock_wait_us,
            self.change_record_write_us,
            self.response_envelope_us,
            self.status_sync_us,
            self.status_backup_us,
            self.status_durability_us,
            self.status_data_dir_us,
            self.status_request_ops_us,
        ]
    }

    pub(super) fn from_array(values: [u64; PHASE_COUNT]) -> Self {
        Self {
            queue_wait_us: values[0],
            admission_wait_us: values[1],
            parse_us: values[2],
            schema_resolve_us: values[3],
            validate_us: values[4],
            lock_wait_us: values[5],
            purge_barrier_us: values[6],
            purge_plan_us: values[7],
            purge_commit_us: values[8],
            purge_materialize_us: values[9],
            purge_trace_us: values[10],
            purge_retention_guard_us: values[11],
            purge_delete_us: values[12],
            purge_finalize_us: values[13],
            molecule_gate_us: values[14],
            cas_precondition_us: values[15],
            count_us: values[16],
            hydrate_us: values[17],
            hydrate_atoms_us: values[18],
            hydrate_format_us: values[19],
            hydrate_sort_us: values[20],
            hydrate_filter_us: values[21],
            annotate_us: values[22],
            apply_us: values[23],
            apply_memory_us: values[24],
            schema_load_us: values[25],
            dedupe_scan_us: values[26],
            idempotency_check_us: values[27],
            grouping_us: values[28],
            restore_molecules_us: values[29],
            protein_sibling_fold_us: values[30],
            spawn_indexing_us: values[31],
            sync_uuids_us: values[32],
            schema_reload_us: values[33],
            persist_us: values[34],
            persist_molecules_us: values[35],
            persist_schema_us: values[36],
            persist_idempotency_us: values[37],
            flush_us: values[38],
            sync_capture_us: values[39],
            index_wait_us: values[40],
            change_record_us: values[41],
            change_record_lock_wait_us: values[42],
            change_record_write_us: values[43],
            response_envelope_us: values[44],
            status_sync_us: values[45],
            status_backup_us: values[46],
            status_durability_us: values[47],
            status_data_dir_us: values[48],
            status_request_ops_us: values[49],
        }
    }

    /// `true` when no phase was reported — the state every surface treats
    /// as absent rather than as a measurement of zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_array().iter().all(|v| *v == 0)
    }

    /// Flat fold over all [`PHASE_COUNT`] fields, INCLUDING pre-service waits
    /// AND `hydrate_us`, which its four `hydrate_*` children partition. On a
    /// post-0.23.4 query row that parent is therefore counted twice, so this
    /// sum is ~2x the real phase cost.
    ///
    /// It is the WRONG number to rank keys by — use [`Self::disjoint_us`] —
    /// and the WRONG number to subtract from `duration_ms` — use
    /// [`Self::within_wall_us`]. It is kept because the wire vocabulary is
    /// a flat 50-field array and a caller that genuinely wants the raw fold
    /// (a serialization round-trip check, for one) should not have to
    /// reimplement it.
    #[must_use]
    pub fn total_us(&self) -> u64 {
        self.as_array()
            .iter()
            .fold(0u64, |acc, v| acc.saturating_add(*v))
    }

    /// Phase time counted ONCE — [`Self::total_us`] less every parent phase
    /// whose children reported. This is the caller-observed cost and the
    /// right number to rank keys by.
    ///
    /// `hydrate_us` is the node's one declared PARENT: it is charged over the
    /// whole materialization span while `hydrate_atoms` / `hydrate_format` /
    /// `hydrate_sort` / `hydrate_filter` partition that same span exactly
    /// (asserted by `lastdb_node/tests/mutation_phase_instrumentation.rs`,
    /// `query_records_count_and_hydrate_phases`). Every other phase surface
    /// in this node treats its buckets as disjoint — `apply` and `persist`
    /// are residuals for precisely this reason — so summing the parent beside
    /// its own parts inflated every query row while every mutation row was
    /// ranked honestly.
    ///
    /// Measured on the primary 2026-08-18: all ten rendered `Request phases`
    /// rows were mis-ordered, query rows inflated 1.98-2.00x, and a
    /// `lastgit query` key printed at #3 belonged at #7 — below four separate
    /// mutation keys that `Top by total time`, which ranks by real
    /// `duration_ms`, placed above it. One command's output contradicted
    /// itself and whichever table was read first was the one acted on.
    ///
    /// The subtraction is GUARDED on the children, which handles both eras
    /// with no schema change: a rollup row written before the 0.23.4 carve
    /// carries `hydrate` with zero children and stays whole; a row written
    /// after it carries the children and drops the parent. The parent itself
    /// is never rewritten — `handlers.rs` keeps it so rollup series written
    /// before the carve stay comparable, and that argument is sound.
    #[must_use]
    pub fn disjoint_us(&self) -> u64 {
        self.total_us().saturating_sub(self.double_counted_us())
    }

    /// Phase time that [`Self::total_us`] counts twice: the sum of every
    /// parent phase whose children reported. Zero on mutation rows, on
    /// pre-carve rollup rows, and on any query that reported `hydrate`
    /// without a sub-step.
    #[must_use]
    pub(super) fn double_counted_us(&self) -> u64 {
        let hydrate_children = self
            .hydrate_atoms_us
            .saturating_add(self.hydrate_format_us)
            .saturating_add(self.hydrate_sort_us)
            .saturating_add(self.hydrate_filter_us);
        if hydrate_children == 0 {
            0
        } else {
            self.hydrate_us
        }
    }

    /// Phase time that falls INSIDE the request's `duration_ms` interval and
    /// is counted ONCE — [`Self::disjoint_us`] less the phases measured before
    /// the wall clock starts. Today that pre-service set is `queue_wait_us`
    /// alone.
    ///
    /// This is the only sound left-hand side for a residual. `duration_ms`
    /// starts after the UDS worker dequeues the job, so subtracting a sum
    /// that contains `queue_wait_us` yields a residual too negative by
    /// exactly the queue wait — and `residual_detail` renders any negative
    /// residual as `over=`, whose documented meaning is over-attribution by
    /// a BATCHED caller. A perfectly instrumented, perfectly unbatched query
    /// on a contended node was therefore libelled as over-attributed, with
    /// the size of the libel set by how contended the node was.
    ///
    /// Measured on the primary 2026-08-17, `client=kanban kind=query` on
    /// the live board schema: `hydrate` + `annotate` summed to 591.89 s
    /// against a 591.79 s wall clock — closed to 0.02% — and the row still
    /// printed `over=359228ms (60.7%)`, which was `queue_wait` and nothing
    /// else. Four of the ten rendered rows read `over=` for that reason.
    ///
    /// Built on `disjoint_us`, not `total_us`, for the same reason one step
    /// down: `hydrate_us` lies inside the wall clock but so do the four
    /// children that partition it, and counting the span twice made the
    /// residual too negative by a whole `hydrate`. With the queue-wait fix
    /// shipped and this one not, the primary printed `over=` at 96-100% on
    /// every one of the ten rendered rows (2026-09-07) — the same libel as
    /// before, now sourced from the double-count instead of the queue wait.
    #[must_use]
    pub fn within_wall_us(&self) -> u64 {
        self.disjoint_us().saturating_sub(self.queue_wait_us)
    }

    /// Stable `(name, value)` pairs in [`Self::PHASE_NAMES`] order.
    #[must_use]
    pub fn named_us(&self) -> [(&'static str, u64); PHASE_COUNT] {
        let values = self.as_array();
        std::array::from_fn(|i| (Self::PHASE_NAMES[i], values[i]))
    }

    /// Fold one request's phases into an aggregate: field-wise saturating
    /// SUM. Aggregates must stay delta-able, so no max/last semantics here.
    pub fn accumulate(&mut self, sample: Self) {
        let mut values = self.as_array();
        for (acc, v) in values.iter_mut().zip(sample.as_array()) {
            *acc = acc.saturating_add(v);
        }
        *self = Self::from_array(values);
    }

    /// Field-wise saturating subtraction — the interval delta the rollup
    /// writer persists between two consecutive cumulative snapshots.
    #[must_use]
    pub fn saturating_sub(&self, prev: Self) -> Self {
        let mut values = self.as_array();
        for (v, p) in values.iter_mut().zip(prev.as_array()) {
            *v = v.saturating_sub(p);
        }
        Self::from_array(values)
    }

    /// Rendered breakdown like ` phases[apply=3400us persist=90us]`. Zero
    /// phases are omitted and an empty set renders as the empty string, so
    /// lines for requests that reported nothing stay byte-identical to what
    /// operators already read.
    #[must_use]
    pub fn detail(&self) -> String {
        let parts: Vec<String> = self
            .named_us()
            .iter()
            .filter(|(_, v)| *v > 0)
            .map(|(name, v)| format!("{name}={v}us"))
            .collect();
        if parts.is_empty() {
            return String::new();
        }
        format!(" phases[{}]", parts.join(" "))
    }
}
// lint:file-size-ok moved verbatim from request_telemetry.rs; cohesive unit, split further in a later pass
