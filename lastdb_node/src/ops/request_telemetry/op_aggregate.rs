use super::*;

/// One completed request sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpSample {
    pub ts_ms: u64,
    pub client: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub kind: OpKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub duration_ms: u64,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    pub body_bytes: u64,
    /// Cold hash-group shard loads observed while this request was in flight.
    ///
    /// **Observed during, not necessarily caused by.** The underlying counter
    /// is store-wide, so under concurrency a request is charged for loads other
    /// requests triggered in the same window. That makes it a good ranking
    /// signal over many samples and a bad forensic claim about one sample —
    /// read it the way you would read a load average.
    ///
    /// `None` on backends without hash groups.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_shard_loads: Option<u64>,
    /// Unanchored product reads this request caused the store to reject.
    /// Exact across async/blocking boundaries; concurrent callers are excluded.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub partition_read_rejections: u64,
    /// Explicit startup/admin physical passes this request issued.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub all_group_walks: u64,
    /// Per-phase timing breakdown (microseconds). Empty means the handler
    /// reported no phases; serde omits it so pre-phase status JSON is
    /// unchanged. Fixed-size `Copy` by design — see [`PhaseTimings`].
    #[serde(default, skip_serializing_if = "PhaseTimings::is_empty")]
    pub phases: PhaseTimings,
    /// Field molecules this request handed to the durable store.
    ///
    /// The denominator `phases.persist_molecules_us` lacks. Request-scoped and
    /// exact — unlike [`Self::cold_shard_loads`], this is counted at the site
    /// that issues the work, so it is never charged for a concurrent caller.
    /// Zero (and omitted) on every non-mutating request.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub molecules_persisted: u64,
    /// Awaited durable molecule-store operations this request issued.
    ///
    /// Read against [`Self::molecules_persisted`] as a ratio: `24/24` is the
    /// per-molecule store path, `24/1` is the shared batch commit. A wall-clock
    /// read of `persist_molecules_us` cannot separate those two — it moved 3.4x
    /// on the primary in two hours with no change to the code path at all —
    /// which is why the count exists.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub molecule_store_commits: u64,
    /// Logical resident commits this request published — one per caller batch
    /// that reached the apply gates.
    ///
    /// Zero (and omitted) on every non-mutating request, and on a row written
    /// by a binary before this counter existed. That is unmigrated, not
    /// measured-as-zero.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub resident_commits: u64,
    /// Operations carried by those commits: created + updated + deleted +
    /// recognised-duplicate rows.
    ///
    /// Read against `resident_commits` as a ratio. `9/1` is one resident
    /// commit carrying a primary record and eight exact projections; `9/9` is
    /// the serial per-projection repair path doing the same work as nine
    /// separate commits. No wall-clock phase separates those two — both spend
    /// their time in the same buckets, and the serial shape can be the faster
    /// one per commit while being nine times the work — which is why the pair
    /// exists.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub resident_operations: u64,
    /// UDS worker-pool jobs accepted and not yet finished when this sample was
    /// recorded, including the current request when it ran through the pool.
    ///
    /// Paired with `phases.queue_wait_us`: a slow point read with
    /// `uds_in_flight <= uds_workers` was not sitting behind a full worker
    /// pool, while `uds_in_flight > uds_workers` names real queue pressure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uds_in_flight: Option<usize>,
    /// Configured UDS worker threads at sample time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uds_workers: Option<usize>,
    /// Configured UDS queue capacity at sample time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uds_queue_capacity: Option<usize>,
    /// HTTP method + path without query/fragment (e.g. `GET /api/schemas`).
    ///
    /// Forensic detail for ONE sample, never an aggregation key: several
    /// routes carry caller data in the path (`/api/atom/<id>`,
    /// `/api/history/<key>`, `/api/schema/<name>`,
    /// `/api/app/blob/cas/sha256/<digest>`), so keying on it would mint a
    /// telemetry key per atom, per key, per schema and per digest. Use
    /// [`Self::route`] for identity. Omitted when not recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Bounded label for the route the router matched (the `DataRoute`
    /// variant name, e.g. `DbCompactOrderLog`).
    ///
    /// This is the identity [`OpKind`] throws away. `kind` is a lossy
    /// projection of the route — `OpKind::Other` is the fallback arm over
    /// roughly sixty `/api/db/*` admin routes, and none of them reports a
    /// schema — so a `(client, kind, schema)` key renders the node's heaviest
    /// admin work as one unidentifiable row. Measured on the primary
    /// 2026-10-06T14:4xZ: `lastdb / other / -` was #1 by total time at
    /// 13h14m of a 14h uptime, and the actual cause (one
    /// `lastdb db compact-order-log --retention-seconds 0 --execute`, running
    /// 13h37m) was readable in `ps` and nowhere in `lastdb ops`.
    ///
    /// Omitted on a sample recorded by a binary before this field existed,
    /// and on direct in-process calls that bypass the route dispatcher. That
    /// is unrecorded, not "no route".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    /// Kernel peer pid from the UDS connection, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_pid: Option<i32>,
    /// Best-effort short process name for [`Self::peer_pid`] at sample time
    /// (e.g. `curl`, `node`, `lastdb`). May be absent if the process exited
    /// before the name lookup or the platform cannot resolve it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_comm: Option<String>,
}

/// Aggregate counters for one (client, kind, schema) key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpAggregate {
    pub client: String,
    pub kind: OpKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Route identity for a key the schema cannot identify — see
    /// [`OpAggregate::key`] for why it participates only then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    pub count: u64,
    pub sum_ms: u64,
    pub max_ms: u64,
    pub last_ts_ms: u64,
    pub error_count: u64,
    /// Total cold shard loads observed across this key's requests. Ranking
    /// signal — see [`OpSample::cold_shard_loads`] for the attribution caveat.
    #[serde(default)]
    pub sum_cold_shard_loads: u64,
    /// Total request body bytes observed across this key's requests.
    ///
    /// For mutations / mutation batches / file-blob writes this is the best
    /// cheap proxy for "how many bytes did this caller ask the node to
    /// absorb" — not net disk growth (tips/indexes/history amplify), but the
    /// ranking that answers "who is stuffing the node" when time and count
    /// alone cannot. Zero-hide on the ranking table when every key is 0, so
    /// a quiet window stays byte-identical to pre-field output.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub sum_body_bytes: u64,
    /// Largest single request body on this key. Like `max_ms`, this is a
    /// running process max (not an interval max) — useful to spot one fat
    /// write hiding inside a large sum.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub max_body_bytes: u64,
    /// Failed requests broken down by HTTP status.
    ///
    /// `error_count` alone tells an operator that writes failed but not
    /// whether they were rejected (413 too large), conflicted (409), or the
    /// node faulted (5xx) — and the sample ring holding the status is only
    /// [`DEFAULT_RING_CAP`] deep, so by the time anyone reads `lastdb ops`
    /// the evidence is usually gone. This survives for the aggregate's life.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub error_statuses: BTreeMap<u16, u64>,
    /// Failures whose status did not fit within [`MAX_ERROR_STATUS_KEYS`].
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub error_statuses_overflow: u64,
    /// Status of the most recent failure on this key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_status: Option<u16>,
    /// When the most recent failure happened — an undated error count hides
    /// how stale it is, which is how an hours-old transient gets diagnosed as
    /// a live outage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_ts_ms: Option<u64>,
    /// Field-wise SUM of phase timings across this key's requests
    /// (microseconds). Sums, never maxes: the durable rollup's delta writer
    /// subtracts consecutive snapshots, and a cumulatively-copied max
    /// cannot be delta'd.
    #[serde(default, skip_serializing_if = "PhaseTimings::is_empty")]
    pub phase_sums: PhaseTimings,
    /// Number of samples that reported a non-empty phase set — the honest
    /// denominator for per-phase averages while phase coverage is partial.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub phase_count: u64,
    /// Wall-clock sum (ms) over ONLY the samples counted by `phase_count`.
    ///
    /// The numerator that makes the phase table's residual honest. `sum_ms`
    /// spans all `count` requests while `phase_sums` spans only the phased
    /// ones, so `sum_ms - phase_sums.total_us()` silently mixes two
    /// populations and overstates the unattributed remainder whenever phase
    /// coverage is partial. This field is the matching wall clock for the
    /// phased population, so [`Self::unphased_us`] compares like with like
    /// no matter what fraction reported phases.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub phased_sum_ms: u64,
    /// Total field molecules this key's requests handed to the durable store.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub sum_molecules_persisted: u64,
    /// Total awaited durable molecule-store operations this key's requests
    /// issued. Divide [`Self::sum_molecules_persisted`] by this for the
    /// molecules-per-commit ratio — the contention-free way to see whether the
    /// batch commit path is being taken, and how often eligibility rejects a
    /// molecule from it.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub sum_molecule_store_commits: u64,
    /// Logical resident commits published across this window's requests.
    pub sum_resident_commits: u64,
    /// Operations those commits carried. Divide by
    /// [`Self::sum_resident_commits`] for the window's operations-per-commit.
    pub sum_resident_operations: u64,
}

impl OpAggregate {
    /// Wall time this key's phased requests spent in NO reported phase, in
    /// microseconds. `None` when nothing reported phases (there is no
    /// comparison to make — not a residual of zero).
    ///
    /// Compares against [`PhaseTimings::within_wall_us`], not `total_us` —
    /// phases measured before the wall clock starts (socket-queue wait) are
    /// not part of the interval being divided up, and including them made
    /// every contended key report a spurious `over=`.
    ///
    /// Signed on purpose. Phases accumulate PER BATCH ITEM (`add_phase` sums
    /// into the request's totals, see
    /// `lastdb_host::handlers::execute_mutations_batch`), so a multi-item
    /// batch legitimately reports MORE phase time than the one request's wall
    /// clock. Clamping that to zero would hide the over-attribution and make
    /// a batch-heavy key look perfectly accounted; a negative value is the
    /// signal that the key's traffic is batched, and it also keeps the
    /// positive residual readable as the conservative floor it is.
    #[must_use]
    pub fn unphased_us(&self) -> Option<i64> {
        if self.phase_count == 0 || self.phase_sums.is_empty() {
            return None;
        }
        // No recorded wall clock for the phased population means there is
        // nothing to subtract FROM. Two ways to land here, both of which must
        // render nothing rather than a residual:
        //
        // - a durable rollup row written before `phased_sum_ms` shipped, which
        //   reads back as 0 — subtracting phases from it would report the
        //   whole phase sum as `over=`, libelling an unmigrated row as
        //   over-attributed;
        // - a key whose phased requests all completed sub-millisecond, where
        //   the true residual is under the 1 ms resolution anyway.
        if self.phased_sum_ms == 0 {
            return None;
        }
        let wall_us = i128::from(self.phased_sum_ms).saturating_mul(1_000);
        // `within_wall_us`, not `total_us`: `phased_sum_ms` sums `duration_ms`,
        // which begins after the UDS dequeue, so socket-queue wait is not
        // inside the interval being divided up.
        let phased_us = i128::from(self.phase_sums.within_wall_us());
        Some(
            (wall_us - phased_us)
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                .try_into()
                .unwrap_or(0),
        )
    }
}

/// Render a residual as the ` unphased=…` / ` over=…` token the phase
/// surfaces append after `phases[…]`.
///
/// Milliseconds with a percentage: the residual is the number an operator
/// acts on, and at these magnitudes (seconds per call) microseconds read as
/// noise. `wall_us == 0` yields no percentage rather than a division by zero.
pub(super) fn residual_detail(unphased_us: i64, wall_us: u64) -> String {
    let pct = if wall_us == 0 {
        String::new()
    } else {
        #[allow(clippy::cast_precision_loss)]
        let ratio = (unphased_us.unsigned_abs() as f64 / wall_us as f64) * 100.0;
        format!(" ({ratio:.1}%)")
    };
    if unphased_us < 0 {
        // Over-attribution: batched requests report each item's phases
        // against one request's wall clock. Named differently so it can
        // never be misread as unaccounted time.
        format!(" over={}ms{pct}", unphased_us.unsigned_abs() / 1_000)
    } else {
        format!(" unphased={}ms{pct}", unphased_us / 1_000)
    }
}

pub(super) fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

/// Fold one failure into a bounded status breakdown.
///
/// Shared by [`OpAggregate`] and [`AppVerbAggregate`] so the two surfaces
/// cannot drift apart on the cap or the overflow accounting.
pub(super) fn record_error_status(
    statuses: &mut BTreeMap<u16, u64>,
    overflow: &mut u64,
    status: u16,
    ts_ms: u64,
    last_status: &mut Option<u16>,
    last_ts_ms: &mut Option<u64>,
) {
    let has_room = statuses.len() < MAX_ERROR_STATUS_KEYS;
    match statuses.get_mut(&status) {
        Some(n) => *n = n.saturating_add(1),
        None if has_room => {
            statuses.insert(status, 1);
        }
        None => *overflow = overflow.saturating_add(1),
    }
    // Samples arrive in completion order, but a merge across aggregates can
    // present them out of order — keep the genuinely newest.
    if last_ts_ms.is_none_or(|prev| ts_ms >= prev) {
        *last_status = Some(status);
        *last_ts_ms = Some(ts_ms);
    }
}

impl OpAggregate {
    /// Identity for one ranked row.
    ///
    /// The route participates **only when the row has no schema**, which is
    /// the rule [`OpSample::route`] exists to serve: a schema-bearing row is
    /// already identified by the work it touched, and a schema-less one has
    /// nothing but its route. Collapsing the schema-less rows is not a
    /// cosmetic loss — `OpKind::Other` is the fallback over roughly sixty
    /// `/api/db/*` admin routes and reports no schema, so before the route
    /// joined this key every one of them ranked as a single
    /// `<client> / other / -` row.
    ///
    /// Keying on the route rather than on [`OpSample::path`] is what keeps
    /// the key space bounded; see that field's note on the parameterised
    /// paths.
    pub(super) fn key(
        client: &str,
        kind: OpKind,
        schema: Option<&str>,
        route: Option<&str>,
    ) -> String {
        let schema = schema.unwrap_or("");
        let route = if schema.is_empty() {
            route.unwrap_or("")
        } else {
            ""
        };
        format!("{}\0{}\0{}\0{}", client, kind.as_str(), schema, route)
    }

    /// The route this row keeps as identity, or `None` when the schema is the
    /// identity. Never the route of one arbitrary sample out of several.
    pub(super) fn identity_route(schema: Option<&str>, route: Option<&str>) -> Option<String> {
        if schema.is_some_and(|s| !s.is_empty()) {
            return None;
        }
        route
            .filter(|r| !r.is_empty())
            .map(std::string::ToString::to_string)
    }

    pub fn avg_ms(&self) -> u64 {
        self.sum_ms.checked_div(self.count).unwrap_or(0)
    }

    /// Cold shard loads observed per call on this key.
    ///
    /// **Not a per-caller read cost.** [`OpSample::cold_shard_loads`] is a
    /// store-wide counter sampled around each request, so a call is charged for
    /// every load concurrent callers triggered while it ran. This ratio
    /// therefore scales with how LONG the key's calls are, and a key whose
    /// calls are slow for reasons that have nothing to do with reading — lock
    /// wait, queueing, a large sort — ranks at the top of it.
    ///
    /// Read [`Self::cold_shard_loads_per_service_second`] beside it. When the
    /// per-second figure is flat across keys whose per-call figures differ by
    /// orders of magnitude, the per-call column is measuring duration and the
    /// node has one store-wide load rate, not one heavy caller.
    pub fn avg_cold_shard_loads(&self) -> u64 {
        self.sum_cold_shard_loads
            .checked_div(self.count)
            .unwrap_or(0)
    }

    /// Cold shard loads per SECOND of this key's service time.
    ///
    /// The duration-normalised companion to [`Self::avg_cold_shard_loads`].
    /// Because the underlying counter is store-wide, a key that merely runs
    /// long accumulates loads in proportion to its own duration; dividing them
    /// out leaves the store-wide load rate during that key's windows. Keys that
    /// genuinely differ in read cost separate here; keys that differ only in
    /// duration converge.
    pub fn cold_shard_loads_per_service_second(&self) -> u64 {
        self.sum_cold_shard_loads
            .saturating_mul(1000)
            .checked_div(self.sum_ms)
            .unwrap_or(0)
    }

    /// Mean request body size on this key.
    pub fn avg_body_bytes(&self) -> u64 {
        self.sum_body_bytes.checked_div(self.count).unwrap_or(0)
    }

    /// Rendered error breakdown, empty when this key never failed.
    pub fn error_detail(&self, now_ms: u64) -> String {
        error_detail(
            self.error_count,
            &self.error_statuses,
            self.error_statuses_overflow,
            self.last_error_status,
            self.last_error_ts_ms,
            now_ms,
        )
    }
}

/// Aggregate counters for one app/client and request verb, across schemas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppVerbAggregate {
    pub client: String,
    pub kind: OpKind,
    pub count: u64,
    pub sum_ms: u64,
    pub max_ms: u64,
    pub error_count: u64,
    /// Total request body bytes across schemas for this app/verb.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub sum_body_bytes: u64,
    /// Largest single request body across schemas for this app/verb.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub max_body_bytes: u64,
    /// Failed requests broken down by HTTP status — see
    /// [`OpAggregate::error_statuses`].
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub error_statuses: BTreeMap<u16, u64>,
    /// Failures whose status did not fit within [`MAX_ERROR_STATUS_KEYS`].
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub error_statuses_overflow: u64,
    /// Status of the most recent failure for this app/verb.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_status: Option<u16>,
    /// When the most recent failure happened.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_ts_ms: Option<u64>,
    /// p95 over samples still present in the recent ring.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_ms: Option<u64>,
    #[serde(default)]
    pub recent_count: u64,
}

impl AppVerbAggregate {
    pub(super) fn key(client: &str, kind: OpKind) -> String {
        format!("{}\0{}", client, kind.as_str())
    }

    pub(super) fn avg_ms(&self) -> u64 {
        self.sum_ms.checked_div(self.count).unwrap_or(0)
    }

    /// Rendered error breakdown, empty when this app/verb never failed.
    pub fn error_detail(&self, now_ms: u64) -> String {
        error_detail(
            self.error_count,
            &self.error_statuses,
            self.error_statuses_overflow,
            self.last_error_status,
            self.last_error_ts_ms,
            now_ms,
        )
    }
}

/// Fold one [`OpAggregate`]'s error breakdown into an app/verb rollup.
///
/// Keeps the same cap as the per-schema map: statuses that no longer fit are
/// added to overflow rather than dropped, so
/// `sum(error_statuses) + error_statuses_overflow == error_count` holds on the
/// rollup exactly as it does on the source aggregates.
pub(super) fn merge_error_statuses(rollup: &mut AppVerbAggregate, src: &OpAggregate) {
    for (status, count) in &src.error_statuses {
        let has_room = rollup.error_statuses.len() < MAX_ERROR_STATUS_KEYS;
        match rollup.error_statuses.get_mut(status) {
            Some(n) => *n = n.saturating_add(*count),
            None if has_room => {
                rollup.error_statuses.insert(*status, *count);
            }
            None => {
                rollup.error_statuses_overflow =
                    rollup.error_statuses_overflow.saturating_add(*count);
            }
        }
    }
    rollup.error_statuses_overflow = rollup
        .error_statuses_overflow
        .saturating_add(src.error_statuses_overflow);

    if let (Some(status), Some(ts_ms)) = (src.last_error_status, src.last_error_ts_ms) {
        if rollup.last_error_ts_ms.is_none_or(|prev| ts_ms >= prev) {
            rollup.last_error_status = Some(status);
            rollup.last_error_ts_ms = Some(ts_ms);
        }
    }
}

/// Render an error breakdown as ` [413x17 409x1 last=413 7m ago]`, or an empty
/// string when the key never failed — so healthy lines are byte-identical to
/// what operators already read.
pub(super) fn error_detail(
    error_count: u64,
    statuses: &BTreeMap<u16, u64>,
    overflow: u64,
    last_status: Option<u16>,
    last_ts_ms: Option<u64>,
    now_ms: u64,
) -> String {
    if error_count == 0 {
        return String::new();
    }
    let mut parts: Vec<(u16, u64)> = statuses.iter().map(|(s, n)| (*s, *n)).collect();
    parts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut detail: Vec<String> = parts.iter().map(|(s, n)| format!("{s}x{n}")).collect();
    if overflow > 0 {
        detail.push(format!("other x{overflow}"));
    }
    if let (Some(status), Some(ts_ms)) = (last_status, last_ts_ms) {
        detail.push(format!("last={status} {}", human_age(now_ms, ts_ms)));
    }
    if detail.is_empty() {
        return String::new();
    }
    format!(" [{}]", detail.join(" "))
}
// lint:file-size-ok moved verbatim from request_telemetry.rs; cohesive unit, split further in a later pass
