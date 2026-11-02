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
    /// admission gate, queueing at a write gate (`field_key_gate`), or doing parse /
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
        /// Mutation: time blocked acquiring the Phase 3 per-`(field key, hash)`
        /// write gate, held across the rest of the write including the deferred
        /// durable persist.
        field_key_gate_us => "field_key_gate",
        /// Mutation: persisted-head reads verifying CAS expectations while the
        /// batch holds those locks. Storage IO, not contention.
        cas_precondition_us => "cas_precondition",
        /// Query: the exact row count read from the key index ahead of the
        /// push-down. Large relative to `hydrate` means the partition is being
        /// counted more expensively than the page is being served.
        count_us => "count",
        /// Query: row materialization end to end — the PARENT total that the three
        /// `hydrate_*` fields below partition exactly. Kept so rollup rows written
        /// before 0.23.4 stay comparable with rows written after; read the
        /// sub-steps to decide what to fix.
        hydrate_us => "hydrate",
        /// Query: the executor call — key-index resolution, window push-down and
        /// record-body materialization. The only hydrate sub-step that touches
        /// storage.
        hydrate_records_us => "hydrate_records",
        /// Query: response-envelope build, `O(rows x fields)` and storage-free.
        /// Large here with `hydrate_records` small means the read is cheap and the
        /// rendering is not.
        hydrate_format_us => "hydrate_format",
        /// Query: the deterministic total-order sort that makes offset/limit
        /// pagination non-overlapping. `O(rows log rows)` JSON-keyed comparisons.
        hydrate_sort_us => "hydrate_sort",
        /// Query: per-field merge-conflict annotation of the formatted rows.
        annotate_us => "annotate",
        /// In-memory application of the write — the RESIDUAL left after every
        /// other named write phase below is subtracted from the batch's wall
        /// time, not a sum of named steps. See
        /// `fold_db::request_phases::RequestPhase::Apply`.
        apply_us => "apply",
        /// Mutation: the in-memory apply loop itself — walking the batch's record
        /// results onto the resident field keys. Sized by the field keys touched
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
        /// Mutation: restoring field keys missing from the resident graph for the
        /// keys this batch touches. Storage IO.
        restore_field_keys_us => "restore_field_keys",
        /// Mutation: spawning the background index-mutation task (the spawn call
        /// itself, not the off-task indexing work).
        spawn_indexing_us => "spawn_indexing",
        /// Mutation: syncing in-memory field key UUIDs onto the schema before it
        /// is stored/reloaded.
        sync_uuids_us => "sync_uuids",
        /// Mutation: schema reload back into the schema manager after a write.
        schema_reload_us => "schema_reload",
        /// Durable persist of the applied write, as a residual: the durable-store
        /// time not attributed to one of the three named sub-steps below. In
        /// practice the record batch store.
        persist_us => "persist",
        /// Mutation: the field key batch store. Sized by field key cardinality, not
        /// by the write, so a schema whose field keys hold tens of thousands of
        /// keys pays here on every mutation.
        persist_field_keys_us => "persist_field_keys",
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
        /// Post-write process-local outbox recording. The durable change-feed
        /// append joins the native commit and reports no phase of its own.
        change_record_us => "change_record",
        /// Final response envelope/error rendering after route work completes.
        response_envelope_us => "response_envelope",
        /// Status route: durable backup marker evaluation.
        status_durability_us => "status_durability",
        /// Status route: recursive data-directory sizing.
        status_data_dir_us => "status_data_dir",
        /// Status route: request-ops ring/aggregate snapshot copying.
        status_request_ops_us => "status_request_ops",
    }
}

mod math;
