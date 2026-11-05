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
            /// Stable pipeline names. Serde fields are `<name>_us`. Keep names
            /// and order stable.
            pub const PHASE_NAMES: [&'static str; PHASE_COUNT] = [$($name),*];

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
    /// admission gate, or doing parse / schema-resolve / validate /
    /// cas-precondition / grouping / apply / index-wait /
    /// change-record / response-envelope work.
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
    /// - On aggregates phases are **field-wise sums, never maxes**. Only sums
    ///   survive subtraction honestly ([`Self::saturating_sub`]).

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
        /// Mutation: persisted-head reads verifying CAS expectations inside the
        /// native request gate. Storage IO, not contention.
        cas_precondition_us => "cas_precondition",
        /// Query: the exact row count read from the key index ahead of the
        /// push-down. Large relative to `hydrate` means the partition is being
        /// counted more expensively than the page is being served.
        count_us => "count",
        /// Query: record materialization end to end — the PARENT total that the
        /// three `hydrate_*` fields below partition exactly. Read the sub-steps
        /// to decide what to fix.
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
        /// Mutation: the batch write's wall time less `grouping` —
        /// a residual, not a sum of named steps. See
        /// `fold_db::request_phases::RequestPhase::Apply`.
        apply_us => "apply",
        /// Mutation: grouping the batch's mutations by schema.
        grouping_us => "grouping",
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
