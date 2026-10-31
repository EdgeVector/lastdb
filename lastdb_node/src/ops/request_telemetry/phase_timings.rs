use super::*;

// Declare each phase once so serde fields, display names, and array slots cannot
// drift apart. The invocation order is the existing wire/rollup order.
macro_rules! phase_timings {
    ($(#[$struct_doc:meta])* struct $type:ident {
        $($(#[$field_doc:meta])* $field:ident => $name:literal,)*
    }) => {
        /// Number of phases in [`PhaseTimings`].
        pub(super) const PHASE_COUNT: usize = [$(stringify!($field)),*].len();

        $(#[$struct_doc])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $type {
            $(
                $(#[$field_doc])*
                #[serde(default, skip_serializing_if = "is_zero_u64")]
                pub $field: u64,
            )*
        }

        impl $type {
            /// Stable pipeline names; serde fields are `<name>_us` and durable
            /// rollup fields are `sum_<name>_us`. Keep names and order stable.
            pub const PHASE_NAMES: [&'static str; PHASE_COUNT] = [$($name),*];

            /// Read durable interval sums; absent or non-u64 fields mean no measurement.
            pub fn from_rollup_fields(fields: &serde_json::Value) -> Self {
                Self { $($field: fields.get(concat!("sum_", $name, "_us"))
                    .and_then(serde_json::Value::as_u64).unwrap_or(0)),* }
            }

            pub(super) fn as_array(&self) -> [u64; PHASE_COUNT] {
                [$(self.$field),*]
            }

            pub(super) fn from_array(values: [u64; PHASE_COUNT]) -> Self {
                let [$($field),*] = values;
                Self { $($field),* }
            }
        }
    };
}

phase_timings! {
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

    struct PhaseTimings {
        /// Socket-queue wait before a worker picked the connection up.
        ///
        /// **Outside the request's wall clock.** `execute_data_route` reads this
        /// from the worker pool's thread-local and only THEN starts the
        /// `Instant` that becomes `duration_ms`, so this phase measures time
        /// that elapsed before the interval every other phase divides up. It is
        /// therefore excluded from [`Self::within_wall_us`] — see that method
        /// for what happens when it is not.
        queue_wait_us => "queue_wait",
        /// QoS admission-gate wait — time spent queued behind the write
        /// governor, previously invisible and charged as handler work.
        admission_wait_us => "admission_wait",
        /// Request body parse.
        parse_us => "parse",
        /// Schema resolution.
        schema_resolve_us => "schema_resolve",
        /// Mutation validation.
        validate_us => "validate",
        /// Mutation: time blocked acquiring the per-`(schema, key)` CAS mutex
        /// before any pipeline step runs. Queueing behind same-key writers, not
        /// work — the remedy is the caller's key layout. A batch carrying no CAS
        /// expectation takes no per-key mutex and reports zero here.
        ///
        /// Series note for rollup readers: before the `purge_barrier_us` split
        /// this number also carried the per-schema purge barrier, so a
        /// `lock_wait` drop at that upgrade boundary is the split, not a
        /// regression cured.
        lock_wait_us => "lock_wait",
        /// Mutation, purge side: time blocked before an EXCLUSIVE acquisition of
        /// the per-schema guarded-purge barrier.
        ///
        /// Ordinary writes do not acquire this barrier and report zero here. A
        /// large value identifies concurrent guarded purges with one schema name.
        /// `purge_commit_us` measures the critical section after acquisition.
        purge_barrier_us => "purge_barrier",
        /// Mutation, purge side: the unlocked reachability trace that decides
        /// whether there is anything to delete. A read, paid by this caller
        /// alone; under the `Skip` policy a trace that finds nothing skips the
        /// exclusive acquire entirely. Large here means a scan problem.
        purge_plan_us => "purge_plan",
        /// Mutation, purge side: the destructive snapshot→delete performed while
        /// holding the per-schema purge barrier EXCLUSIVELY. Ordinary writes do
        /// not wait for this work. Summed across the batch's chunks, this is the
        /// total critical-section time, not the longest single section.
        ///
        /// Series note for rollup readers: rows written before 0.23.3 read back
        /// as 0 here while carrying the same work inside the request's
        /// unattributed remainder, so an `apply`/unphased drop at that boundary
        /// is the split, not a regression cured.
        purge_commit_us => "purge_commit",
        /// Mutation, purge side: materializing every field molecule of the schema
        /// before the trace and the guards read it. Sized by the schema
        /// (`fields x molecule cardinality`), not by the records being removed —
        /// a one-record purge of a wide, high-cardinality schema pays the same as
        /// a thousand-record one. Large here is a read-width problem, and the
        /// unlocked probe already shows what narrowing it is worth.
        purge_materialize_us => "purge_materialize",
        /// Mutation, purge side: the reachability trace under the barrier —
        /// legacy `history:` events per field plus the `tv:` chain walk per target
        /// key. On a store with no legacy rows this is chain depth alone.
        purge_trace_us => "purge_trace",
        /// Mutation, purge side: the two preserve sets (retained chain atoms and
        /// live atom uuids) that keep a batch from deleting a live sibling. Both
        /// `O(fields x records)`. A guard, so the fix is to make it cheaper, never
        /// to drop it.
        purge_retention_guard_us => "purge_retention_guard",
        /// Mutation, purge side: the durable removal — per-field molecule-key
        /// removal, file-pointer accounting reads, and the single `batch_delete`.
        /// The only purge phase that should scale with RECORDS PURGED rather than
        /// with schema size.
        purge_delete_us => "purge_delete",
        /// Mutation, purge side: schema re-store, schema-cache reload, flush and
        /// delete-ledger commit inside the guarded purge critical section.
        purge_finalize_us => "purge_finalize",
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
        molecule_gate_us => "molecule_gate",
        /// Mutation: persisted-head reads verifying CAS expectations while the
        /// batch holds those locks. Storage IO; split from `lock_wait_us`
        /// because contention and read cost have opposite fixes.
        cas_precondition_us => "cas_precondition",
        /// Query: the exact row count read from the key index ahead of the
        /// push-down. Large relative to `hydrate` means the partition is being
        /// counted more expensively than the page is being served.
        count_us => "count",
        /// Query: row materialization end to end — the PARENT total that the four
        /// `hydrate_*` fields below partition exactly. Kept so rollup rows written
        /// before 0.23.4 stay comparable with rows written after; read the
        /// sub-steps to decide what to fix.
        hydrate_us => "hydrate",
        /// Query: the executor call — key-index resolution, window push-down and
        /// atom-body materialization. The only hydrate sub-step that touches
        /// storage.
        hydrate_atoms_us => "hydrate_atoms",
        /// Query: response-envelope build, `O(rows x fields)` and storage-free.
        /// Large here with `hydrate_atoms` small means the read is cheap and the
        /// rendering is not.
        hydrate_format_us => "hydrate_format",
        /// Query: the deterministic total-order sort that makes offset/limit
        /// pagination non-overlapping. `O(rows log rows)` JSON-keyed comparisons.
        hydrate_sort_us => "hydrate_sort",
        /// Query: post-sort value-filter retain. Normally zero; non-zero means
        /// rows were hydrated, formatted and sorted before being dropped.
        hydrate_filter_us => "hydrate_filter",
        /// Query: per-field merge-conflict annotation of the formatted rows.
        annotate_us => "annotate",
        /// In-memory application of the write — the RESIDUAL left after every
        /// other named write phase below is subtracted from the batch's wall
        /// time, not a sum of named steps. See
        /// `fold_db::request_phases::RequestPhase::Apply`.
        apply_us => "apply",
        /// Mutation: the in-memory apply loop itself — walking the batch's atom
        /// results onto the resident molecules. Sized by the molecules touched
        /// rather than by the rows written, so read this first when one schema's
        /// `apply` cost is orders of magnitude above another's on the same node.
        apply_memory_us => "apply_memory",
        /// Mutation: loading the schema at the top of the per-schema loop.
        /// Distinct from `schema_resolve` (the handler's name lookup) and
        /// `schema_reload` (the write-back afterwards).
        schema_load_us => "schema_load",
        /// Mutation: the idempotency-duplicate filter pass at the top of the
        /// batch pipeline. Previously folded into `apply_us` unreported outside
        /// debug logging.
        idempotency_check_us => "idempotency_check",
        /// Mutation: grouping the batch's mutations by schema.
        grouping_us => "grouping",
        /// Mutation: restoring molecules missing from the resident graph for the
        /// keys this batch touches. Storage IO.
        restore_molecules_us => "restore_molecules",
        /// Mutation: spawning the background index-mutation task (the spawn call
        /// itself, not the off-task indexing work).
        spawn_indexing_us => "spawn_indexing",
        /// Mutation: syncing in-memory molecule UUIDs onto the schema before it
        /// is stored/reloaded.
        sync_uuids_us => "sync_uuids",
        /// Mutation: schema reload back into the schema manager after a write.
        schema_reload_us => "schema_reload",
        /// Durable persist of the applied write, as a residual: the durable-store
        /// time not attributed to one of the three named sub-steps below. In
        /// practice the atom batch store.
        persist_us => "persist",
        /// Mutation: the molecule batch store. Sized by molecule cardinality, not
        /// by the write, so a schema whose molecules hold tens of thousands of
        /// keys pays here on every mutation.
        persist_molecules_us => "persist_molecules",
        /// Mutation: persisting the schema after a write. Near zero once the
        /// catalog's skip-if-unchanged path applies.
        persist_schema_us => "persist_schema",
        /// Mutation: storing the idempotency record for the applied batch.
        persist_idempotency_us => "persist_idempotency",
        /// Store flush. Near zero on default builds (background flusher);
        /// meaningful only under synchronous-flush configurations.
        flush_us => "flush",
        /// The handler's bounded wait for background index tasks — NOT the
        /// off-thread index/embedding work itself, which never blocks the
        /// request.
        index_wait_us => "index_wait",
        /// Post-write process-local outbox recording only. Since 0.23.2 the
        /// durable change-feed append below is `change_record_lock_wait_us` /
        /// `change_record_write_us`; a drop here at the upgrade boundary is the
        /// split, not a regression cured.
        change_record_us => "change_record",
        /// Mutation: time blocked acquiring the change-feed's single global `tip`
        /// mutex — the one lock on the node not scoped to a schema, key, or
        /// molecule. Every successful mutation passes through it, so a large
        /// value here is every writer on the node queueing behind every other
        /// one, not a same-key or same-schema peer.
        change_record_lock_wait_us => "change_record_lock_wait",
        /// Mutation: the durable change-feed batch write (event + tip) done while
        /// holding the mutex above. Storage IO, split from
        /// `change_record_lock_wait_us` because contention and write cost have
        /// different fixes.
        change_record_write_us => "change_record_write",
        /// Final response envelope/error rendering after route work completes.
        response_envelope_us => "response_envelope",
        /// Status route: sealed-chunk backup progress snapshot.
        status_backup_us => "status_backup",
        /// Status route: durable backup marker evaluation.
        status_durability_us => "status_durability",
        /// Status route: recursive data-directory sizing.
        status_data_dir_us => "status_data_dir",
        /// Status route: request-ops ring/aggregate snapshot copying.
        status_request_ops_us => "status_request_ops",
    }
}

mod math;
