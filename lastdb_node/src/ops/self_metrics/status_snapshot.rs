use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub sampled_at: u64,
    pub process_start_ts: u64,
    pub uptime_secs: u64,
    pub rss_bytes: Option<u64>,
    /// Where this daemon's stdout and stderr are actually going.
    ///
    /// Same class of fact as pid, version and uptime, and the one the node is
    /// the only honest source for — see [`LogStreamHealth`].
    pub logs: LogStreamHealth,
    /// Physical memory footprint — what the OS actually bills this process for.
    ///
    /// This is the honest "how much memory am I using" number and it leads the
    /// status line. `rss_bytes` is the `ps`-style resident size, which on macOS
    /// **excludes compressed pages** and understated the primary by 6.7x on
    /// 2026-08-03 (status said 1.39 GiB; `footprint -p` said 9,547 MB). Three
    /// separate operator runs read the RSS line that day and concluded the node
    /// was comfortable.
    ///
    /// `None` on platforms with no footprint accounting (everything but macOS
    /// today), where RSS is not misleading in the same way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phys_footprint_bytes: Option<u64>,
    /// Lifetime peak of [`Self::phys_footprint_bytes`] for this process.
    ///
    /// The peak is the part a point-in-time reading cannot show: the primary
    /// was sitting at 9.5 GB against a 12 GiB ceiling it had *already touched*.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phys_footprint_peak_bytes: Option<u64>,
    /// The ceiling `lastdbd-memory-guard` restarts this process at
    /// (`LASTDBD_RSS_LIMIT_MB`), in bytes.
    ///
    /// Which gauge it is a ceiling *on* is [`Self::memory_guard_metric`], and
    /// the two are only readable together. This field carried a doc comment
    /// asserting `ps -o rss=` for a month after the guard cut over to
    /// phys_footprint, which is how the status line came to name the wrong one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
    /// The gauge [`Self::memory_limit_bytes`] is enforced on.
    ///
    /// Without it that limit is unreadable: a consumer sees one ceiling and two
    /// gauges 1.4-6.7x apart, with nothing saying which pair is the restart
    /// distance. Not persisted into the self-metric series — it is a constant
    /// for the life of the process, and a constant string on a per-sample row
    /// is storage spent to learn nothing.
    pub memory_guard_metric: MemoryGuardMetric,
    /// Runtime occupancy against the process memory budget.
    ///
    /// Every value is an in-process counter or immutable boot-time budget. No
    /// catalog, row, or per-group scan is performed on the status path.
    pub memory_budget: MemoryBudgetHealth,
    pub cpu_percent: Option<f64>,
    /// Allocated disk under the configured **node home** (not only `data/`).
    ///
    /// This is the primary operator gauge for "is stuck backup eating disk?":
    /// it includes sibling trees such as `backup-cut-freeze/`, `bin/`, pack CAS,
    /// and anything else under [`crate::host::Host::home`]. Historically status
    /// reported only [`Self::data_dir_bytes`] while docs said "node home", which
    /// understated the primary by ~half when a freeze tree was present
    /// (measured 2026-08-06: 11.83 GiB data vs 23.05 GiB `du` of home).
    ///
    /// `None` (field omitted) means no walk has completed since this process
    /// started — the walk is running now and the number lands on a later
    /// status. It is deliberately absent rather than `0`: see
    /// [`crate::ops::gauge`], "a missing field becomes `Unavailable`, never
    /// `Measured(0)`". A `0` here would read as an empty store to an operator
    /// checking disk pressure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home_bytes: Option<u64>,
    /// Directory walks this process has completed for the node home.
    ///
    /// Published because the single-flight property — N concurrent cold
    /// callers cost ONE walk — is invisible in `status_data_dir`, which
    /// measures wait rather than work. Expect `1` shortly after a restart no
    /// matter how many probes arrive at once, then one more per TTL expiry
    /// under continued polling.
    pub home_size_walks: u64,
    /// Absolute path of this node's home directory.
    pub home: String,
    /// Allocated disk under the data directory only (`Host::data_dir`).
    ///
    /// Secondary gauge: store payload without sibling trees under the node home.
    /// Do **not** treat this as total node-home usage — use [`Self::home_bytes`].
    ///
    /// Same absent-not-zero contract as [`Self::home_bytes`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_bytes: Option<u64>,
    /// Directory walks this process has completed for the data dir.
    /// See [`Self::home_size_walks`].
    pub data_dir_size_walks: u64,
    /// Absolute path of this node's data directory.
    ///
    /// HTTP / non-socket clients need this to place durable node-relative
    /// artifacts (pack CAS, workdirs, …) without guessing `$TMPDIR`.
    pub data_dir: String,
    /// Redacted explicit V2 job status; no legacy marker or automatic coverage claim.
    pub durability: DurabilityHealth,
    pub sampler: SamplerStatus,
    pub qos: QosHealth,
    /// UDS worker pool (workers + queue backpressure).
    pub uds: UdsPoolHealth,
    /// Blocking long-poll watchers — the pool occupancy `qos` cannot see.
    pub watchers: WatchersHealth,
    /// In-process request/op offender ranking (self-reported clients).
    pub request_ops: crate::request_telemetry::RequestTelemetrySnapshot,
    /// Write-path product limits (record size fence, …) — always present so
    /// operators and agents see them without reading docs.
    pub limits: LimitsHealth,
    /// Warm set record counts and lookups.
    pub resident: ResidentHealth,
    /// Daemon build identity + the phase vocabulary it knows, so a client can
    /// detect that it is about to render a truncated breakdown.
    pub build: BuildHealth,
    /// Configured node-local TTL policies plus process-lifetime sweep state.
    pub local_retention: LocalRetentionHealth,
    /// The reference verifier: the last run of `lastdb db verify-refs` and a
    /// run in progress.
    pub verify_refs: fold_db::db_operations::VerifySummary,
    /// The blob counters: totals, the dwell queue at the last pass, counter
    /// faults.
    pub blob_counters: fold_db::db_operations::BlobCounterReport,
    /// The retention window of kept conflict records, the janitor's last run
    /// and the queue depth.
    pub conflict_retention: fold_db::db_operations::ConflictRetentionReport,
    /// Process-lifetime effectiveness of plaintext compression before sealing.
    ///
    /// All-zero counters mean nothing sealed yet. The enabled bit separately
    /// distinguishes a cold process from the compression kill switch being
    /// engaged.
    pub at_rest_compression: AtRestCompressionHealth,
}
