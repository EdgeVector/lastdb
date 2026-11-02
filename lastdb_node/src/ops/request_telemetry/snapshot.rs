use super::*;

/// Snapshot exported on `/api/status` and `lastdb ops`.
///
/// On the **cheap health** path (`GET /api/status` without `?recent=1`), only
/// [`Self::sample_count`] and [`Self::ring_capacity`] are serialized — the
/// forensic ring and ranking tables are cleared via
/// [`Self::for_cheap_health`]. Pass `?recent=1` or `?forensics=1` (or call
/// `lastdb ops`) for the full payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestTelemetrySnapshot {
    /// Newest-first ring of recent samples (capped). Forensic; opt-in on status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent: Vec<OpSample>,
    /// Slowest individual samples currently in the ring. Forensic; opt-in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_by_duration: Vec<OpSample>,
    /// Aggregates ranked by total time spent (`sum_ms`). Forensic; opt-in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_by_total_ms: Vec<OpAggregate>,
    /// Aggregates ranked by call count. Forensic; opt-in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_by_count: Vec<OpAggregate>,
    /// Aggregates ranked by total request body bytes — who is stuffing the
    /// node (mutation / batch / file-blob write volume), not who is slow.
    ///
    /// Idle long-poll wait is excluded (body is zero there anyway). The table
    /// is omitted entirely when every work key has `sum_body_bytes == 0`.
    /// Forensic; opt-in on status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub top_by_body_bytes: Vec<OpAggregate>,
    /// Aggregates by app/client and verb, across schemas — **work only**.
    ///
    /// Idle long-poll kinds ([`OpKind::is_idle_wait`]) are excluded here so the
    /// compact `lastdb status` "App / verb latency" block (and
    /// `lastdb ops --by-app`) cannot rank a sleeping `local_watch` as if it
    /// were store latency. Those watches live in [`Self::idle_wait`].
    /// Forensic; opt-in on status (use `lastdb ops --by-app`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub app_verb: Vec<AppVerbAggregate>,
    /// Idle long-poll wait ([`OpKind::is_idle_wait`]), ranked by `sum_ms` and
    /// held OUT of `top_by_total_ms` / `top_by_duration` / `app_verb` so a
    /// sleeping watcher cannot outrank real query work. Reported separately,
    /// never dropped on the forensic path. Opt-in on status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub idle_wait: Vec<OpAggregate>,
    pub sample_count: u64,
    pub ring_capacity: usize,
}

impl RequestTelemetrySnapshot {
    /// Drop the forensic ring and ranking tables; keep only the cheap scalars.
    ///
    /// Used by default `/api/status` so fleet health probes stay small. The
    /// in-process ring is still fully recorded — this only affects serialization.
    #[must_use]
    pub fn for_cheap_health(&self) -> Self {
        Self {
            recent: Vec::new(),
            top_by_duration: Vec::new(),
            top_by_total_ms: Vec::new(),
            top_by_count: Vec::new(),
            top_by_body_bytes: Vec::new(),
            app_verb: Vec::new(),
            idle_wait: Vec::new(),
            sample_count: self.sample_count,
            ring_capacity: self.ring_capacity,
        }
    }
}

/// Durable rollup query result rendered by `lastdb ops --since`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestOpsRollupSnapshot {
    /// Aggregates merged across persisted sampler rollup rows.
    /// Excludes [`OpKind::is_idle_wait`] — see [`Self::idle_wait`].
    #[serde(default)]
    pub top_by_total_ms: Vec<OpAggregate>,
    /// Aggregates ranked by call count across persisted sampler rollup rows.
    /// Call count is honest for every kind, so idle wait stays in this one.
    #[serde(default)]
    pub top_by_count: Vec<OpAggregate>,
    /// Aggregates ranked by total request body bytes across the window.
    /// Idle wait is excluded; empty when every work key wrote 0 body bytes.
    #[serde(default)]
    pub top_by_body_bytes: Vec<OpAggregate>,
    /// Idle long-poll wait, held out of `top_by_total_ms` for the same reason
    /// as [`RequestTelemetrySnapshot::idle_wait`]: sleeping is not consuming.
    #[serde(default)]
    pub idle_wait: Vec<OpAggregate>,
    #[serde(default)]
    pub row_count: usize,
    #[serde(default)]
    pub since_ms: u64,
    #[serde(default)]
    pub until_ms: u64,
}

impl RequestOpsRollupSnapshot {
    /// True when the window rendered nothing. Says only that no rows came
    /// back — NOT that no traffic happened. A failing sampler produces an
    /// empty window indistinguishable from an idle one, which is why
    /// `lastdb ops --since` cross-checks `sampler.last_error` before letting
    /// a reader conclude "quiet period".
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.top_by_total_ms.is_empty() && self.idle_wait.is_empty()
    }
}
