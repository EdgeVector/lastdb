use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub sampled_at: u64,
    pub process_start_ts: u64,
    pub uptime_secs: u64,
    pub rss_bytes: Option<u64>,
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
    /// Where this daemon's stdout and stderr are actually going.
    ///
    /// Same class of fact as pid, version and uptime, and the one the node is
    /// the only honest source for — see [`LogStreamHealth`].
    #[serde(default)]
    pub logs: LogStreamHealth,
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
    #[serde(default)]
    pub memory_guard_metric: MemoryGuardMetric,
    /// Runtime occupancy against the process memory budget.
    ///
    /// Every value is an in-process counter or immutable boot-time budget. No
    /// catalog, row, or per-group scan is performed on the status path.
    #[serde(default)]
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
    #[serde(default)]
    pub home_size_walks: u64,
    /// Absolute path of this node's home directory, when disclosed.
    ///
    /// Same disclosure contract as [`Self::data_dir`]: omitted only when
    /// [`Self::home_path_disclosed`] is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub home: Option<String>,
    /// Whether this response discloses the host-local node-home path.
    #[serde(default)]
    pub home_path_disclosed: bool,
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
    #[serde(default)]
    pub data_dir_size_walks: u64,
    /// Absolute path of this node's data directory, when disclosed.
    ///
    /// HTTP / non-socket clients need this to place durable node-relative
    /// artifacts (pack CAS, workdirs, …) without guessing `$TMPDIR`. Omitted
    /// only when [`Self::data_dir_path_disclosed`] is false — never silently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
    /// Whether this response discloses the host-local data-dir path.
    ///
    /// Always present so a client can tell "old binary / field missing" from
    /// "path withheld by policy". When false, do not invent a fallback path
    /// for durable node-relative artifacts.
    #[serde(default)]
    pub data_dir_path_disclosed: bool,
    pub sync: SyncHealth,
    /// Sealed-chunk cloud backup progress (percent / ETA). Omitted fields when
    /// backup is disabled; `show_progress` is false when fully caught up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupProgressHealth>,
    /// Cloud backup billable footprint: referenced keep-set vs listed total.
    ///
    /// Cached (GC / background list); never computed inline on the status path.
    /// When present, operators can read "9 GiB tip / 25 GiB billed" without a
    /// separate `backup-gc` dry-run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_storage: Option<BackupStorageHealth>,
    /// Age of the last committed backup manifest, read from the durable marker.
    ///
    /// Always present — unlike `backup`, which is engine state and therefore
    /// absent exactly when backup is switched off and nobody is watching.
    #[serde(default)]
    pub durability: DurabilityHealth,
    pub sampler: SamplerStatus,
    pub qos: QosHealth,
    /// UDS worker pool (workers + queue backpressure).
    #[serde(default)]
    pub uds: UdsPoolHealth,
    /// Blocking long-poll watchers — the pool occupancy `qos` cannot see.
    #[serde(default)]
    pub watchers: WatchersHealth,
    /// In-process request/op offender ranking (self-reported clients).
    #[serde(default)]
    pub request_ops: crate::request_telemetry::RequestTelemetrySnapshot,
    /// Write-path product limits (atom size fence, …) — always present so
    /// operators and agents see them without reading docs.
    #[serde(default)]
    pub limits: LimitsHealth,
    /// Read cost: cold shard loads + warm-set residency vs budget. `None` on
    /// backends that do not use hash groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_cost: Option<ReadCostHealth>,
    /// Resident graph counters (T0 memory hits, rehydrates, deferred persists).
    #[serde(default)]
    pub resident: ResidentHealth,
    /// Dual-read residue counters (tip-plane legacy fallthrough). Process-lifetime;
    /// see `fold_db::storage::DualReadMetricsSnapshot` / design PR-5.
    #[serde(default)]
    pub dual_read: DualReadHealth,
    /// Daemon build identity + the phase vocabulary it knows, so a client can
    /// detect that it is about to render a truncated breakdown. Defaults empty
    /// when read from a daemon predating this field.
    #[serde(default)]
    pub build: BuildHealth,
    /// Read-path integrity: rows dropped from query results because their tip
    /// pointed at an atom that would not resolve.
    #[serde(default)]
    pub integrity: IntegrityHealth,
    /// Atom reverse-edge cutover bytes and audit gauges.
    ///
    /// Physical bytes are exact collection gauges. Count-derived fields stay
    /// unavailable until a bounded audit or backfill publishes them. Status
    /// never scans either edge plane.
    #[serde(default)]
    pub atom_ref_edges: AtomRefEdgeHealth,
    /// File-blob durability: fetches the remote CAS proved absent, split by
    /// whether this node had recorded those bytes as uploaded.
    #[serde(default)]
    pub file_blob: FileBlobHealth,
    /// Last locator-only tip population probe for this process (if any).
    ///
    /// Never computed on the status path — run `lastdb db probe-locator-only`
    /// (or `POST /api/db/probe-locator-only`) to refresh. Absent means "never
    /// probed since process start", not "population is zero".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_only: Option<LocatorOnlyHealth>,
    /// Per-schema purge accounting (cumulative since process start).
    ///
    /// This ledger reports purge counts, guarded critical-section time, and
    /// path work. Consumers delta the cumulative counters themselves, as they
    /// do for request-ops.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub purge_stats: HashMap<String, fold_db::fold_db_core::PurgeStats>,
    /// Configured node-local TTL policies plus process-lifetime sweep state.
    #[serde(default)]
    pub local_retention: LocalRetentionHealth,
    /// Molecule write-gate HOLD accounting — the other end of the
    /// `molecule_gate` request phase, which measures only the wait.
    ///
    /// Defaults to all-zero when read from a daemon predating this field. That
    /// is indistinguishable from a node that took no gates, so the renderer
    /// suppresses the line entirely at `count == 0` rather than printing
    /// "0 holds" and inviting the reader to conclude the gate is never taken.
    #[serde(default)]
    pub molecule_gate: MoleculeGateHealth,
    /// Process-lifetime effectiveness of plaintext compression before sealing.
    ///
    /// `None` means the serving daemon predates this status field. A current
    /// daemon always publishes `Some`, including all-zero counters, so clients
    /// can distinguish "nothing sealed yet" from "unsupported". The enabled
    /// bit separately distinguishes a cold process from the compression kill
    /// switch being engaged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_rest_compression: Option<AtRestCompressionHealth>,
    /// Effective codec policy and supported read formats.
    ///
    /// Published to indicate which formats this node can write and read,
    /// and to gate unsupported openers. `None` means predates this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec_policy: Option<CodecPolicyHealth>,
}
