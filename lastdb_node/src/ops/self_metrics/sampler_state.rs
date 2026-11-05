use super::*;

/// Operator-visible sampler vitals: when the last sample landed, and the last
/// error if that sample failed. The numbers `lastdb status` prints come from
/// the published [`StatusSnapshot`], not from a LastDB row.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SamplerStatus {
    pub last_sample_at: Option<u64>,
    pub last_error: Option<String>,
}

/// Failed passes in a row before the TTL sweep waits longer than one sampler tick.
///
/// The first `MAX_DRAIN_ATTEMPTS - 1` failures start the next pass on the next
/// tick. From this count, the sweep doubles the wait. The name stays because
/// `ttl_backoff` already keys off it.
pub(super) const MAX_DRAIN_ATTEMPTS: u32 = 3;

#[derive(Debug, Default)]
pub struct SamplerRuntimeState {
    pub(super) inner: Mutex<SamplerStatus>,
    /// Node-local TTL sweeper bookkeeping.
    pub(super) ttl: Mutex<TtlSweepState>,
    /// Last complete status snapshot published by the sampler. `/api/status`
    /// clones this value and never admits a group.
    pub(super) last_status: Mutex<Option<std::sync::Arc<StatusSnapshot>>>,
}

#[derive(Debug, Default)]
pub(super) struct TtlSweepState {
    pub(super) settled: bool,
    pub(super) failures: u32,
    pub(super) in_flight: bool,
    pub(super) passes: u32,
    pub(super) rows_reaped: u64,
    pub(super) last_sweep_unix_secs: Option<u64>,
    pub(super) policies: HashMap<String, fold_db::schema::SchemaRetentionPolicy>,
    pub(super) policy_error: Option<String>,
    pub(super) schema_failures: u64,
    pub(super) last_pass: super::ttl_pass::TtlPassReport,
    /// No new pass starts before this instant (`ttl_backoff`).
    pub(super) retry_at: Option<Instant>,
    /// The same moment in unix seconds, for the status.
    pub(super) next_attempt_unix_secs: Option<u64>,
}

/// Node-local retention policy and sweep state exposed by `/api/status`.
///
/// Policies come from the reserved retention prefix in `schema_states`; this
/// is a bounded operator read over configured series, never a product scan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalRetentionHealth {
    pub enabled: bool,
    pub policies: HashMap<String, fold_db::schema::SchemaRetentionPolicy>,
    pub passes: u32,
    pub rows_reaped: u64,
    pub last_sweep_unix_secs: Option<u64>,
    pub failures: u32,
    pub in_flight: bool,
    pub settled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_error: Option<String>,
    /// Schemas skipped for an error, summed over passes (process counter).
    pub schema_failures: u64,
    pub last_pass: super::ttl_pass::TtlPassReport,
    /// Set while the sweep waits after repeated failed passes: the unix second
    /// at which the next pass may start. `failures` counts the failed passes in
    /// a row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_unix_secs: Option<u64>,
}

impl SamplerRuntimeState {
    pub fn background_writes_in_flight(&self) -> bool {
        self.ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
    }

    pub fn publish_status(&self, snapshot: StatusSnapshot) {
        *self
            .last_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(std::sync::Arc::new(snapshot));
    }

    pub fn published_status(&self) -> Option<StatusSnapshot> {
        self.last_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|snapshot| snapshot.as_ref().clone())
    }

    pub(crate) fn end_ttl_sweep(&self) {
        self.ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight = false;
    }

    pub(crate) fn settle_ttl_sweep(&self) {
        self.ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settled = true;
    }

    pub(crate) fn cache_retention_policies(
        &self,
        policies: HashMap<String, fold_db::schema::SchemaRetentionPolicy>,
    ) {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ttl.policies = policies;
        ttl.policy_error = None;
    }

    pub(crate) fn cache_retention_policy_error(&self, error: String) {
        self.ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .policy_error = Some(error);
    }

    pub(crate) fn local_retention_health(&self, enabled: bool) -> LocalRetentionHealth {
        let ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        LocalRetentionHealth {
            enabled,
            policies: ttl.policies.clone(),
            passes: ttl.passes,
            rows_reaped: ttl.rows_reaped,
            last_sweep_unix_secs: ttl.last_sweep_unix_secs,
            failures: ttl.failures,
            in_flight: ttl.in_flight,
            settled: ttl.settled,
            policy_error: ttl.policy_error.clone(),
            schema_failures: ttl.schema_failures,
            last_pass: ttl.last_pass.clone(),
            next_attempt_unix_secs: ttl.next_attempt_unix_secs,
        }
    }
}

impl SamplerRuntimeState {
    pub fn snapshot(&self) -> SamplerStatus {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn record_success(&self, sampled_at: u64) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.last_sample_at = Some(sampled_at);
        inner.last_error = None;
    }

    pub(crate) fn record_error(&self, error: String) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_error = Some(error);
    }
}
