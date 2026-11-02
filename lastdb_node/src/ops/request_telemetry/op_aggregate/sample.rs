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
    /// Per-phase timing breakdown (microseconds). Empty means the handler
    /// reported no phases; serde omits it so pre-phase status JSON is
    /// unchanged. Fixed-size `Copy` by design — see [`PhaseTimings`].
    #[serde(default, skip_serializing_if = "PhaseTimings::is_empty")]
    pub phases: PhaseTimings,
    /// Field keys this request handed to the durable store.
    ///
    /// The denominator `phases.persist_field_keys_us` lacks. Request-scoped and
    /// exact: this is counted at the site that issues the work, so it is never
    /// charged for a concurrent caller.
    /// Zero (and omitted) on every non-mutating request.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub field_keys_persisted: u64,
    /// Awaited durable field-key-store operations this request issued.
    ///
    /// Read against [`Self::field_keys_persisted`] as a ratio: `24/24` is the
    /// per-field-key store path, `24/1` is the shared batch commit. A wall-clock
    /// read of `persist_field_keys_us` cannot separate those two — it moved 3.4x
    /// on the primary in two hours with no change to the code path at all —
    /// which is why the count exists.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub field_key_store_commits: u64,
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
    /// routes carry caller data in the path (`/api/schema/<name>`,
    /// `/api/app/blob/cas/sha256/<digest>`), so keying on it would mint a
    /// telemetry key per schema and per digest. Use
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
