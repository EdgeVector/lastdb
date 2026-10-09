use super::*;

/// Operator-visible sampler vitals. `sample_count` / `retention_max_samples`
/// are wire-transparent gauges (bare u64 / null).
#[derive(Debug, Clone)]
pub struct SamplerStatus {
    pub last_sample_at: Option<u64>,
    pub last_error: Option<String>,
    pub sample_count: Gauge,
    pub retention_max_samples: Gauge,
    /// Whether this daemon persists telemetry into LastDB at all
    /// (`LASTDB_SELF_METRICS_TO_DB`, default **false**).
    ///
    /// A config fact, not a sampling outcome — without it, "sampled recently
    /// and no error" reads as *healthy and durable* when the durable sink was
    /// never switched on. `lastdb ops --since` needs the distinction to tell a
    /// genuinely idle window from one whose rollups were never written.
    ///
    /// `Option`, not `bool`, and `serde(default)` so an older daemon that does
    /// not send the field reads as `None` — *unknown* — rather than as a
    /// confident `false`. A reader that turned "the daemon did not say" into
    /// "the sink is off" would be making exactly the unearned assertion this
    /// field exists to stop.
    pub db_write_enabled: Option<bool>,
}

impl Default for SamplerStatus {
    fn default() -> Self {
        Self {
            last_sample_at: None,
            last_error: None,
            sample_count: Gauge::field_not_served(Unit::Named("sample(s)"), GAUGE_PROCESS_LIFETIME),
            retention_max_samples: Gauge::field_not_served(Unit::Named("sample(s)"), GAUGE_INSTANT),
            db_write_enabled: None,
        }
    }
}

impl Serialize for SamplerStatus {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("SamplerStatus", 5)?;
        s.serialize_field("last_sample_at", &self.last_sample_at)?;
        s.serialize_field("last_error", &self.last_error)?;
        s.serialize_field("sample_count", &wire_u64(&self.sample_count))?;
        s.serialize_field(
            "retention_max_samples",
            &wire_u64(&self.retention_max_samples),
        )?;
        s.serialize_field("db_write_enabled", &self.db_write_enabled)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for SamplerStatus {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            last_sample_at: Option<u64>,
            #[serde(default)]
            last_error: Option<String>,
            #[serde(default)]
            sample_count: Option<u64>,
            #[serde(default)]
            retention_max_samples: Option<u64>,
            #[serde(default)]
            db_write_enabled: Option<bool>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            last_sample_at: raw.last_sample_at,
            last_error: raw.last_error,
            sample_count: g_lifetime_wire(raw.sample_count, Unit::Named("sample(s)")),
            retention_max_samples: g_instant_wire(
                raw.retention_max_samples,
                Unit::Named("sample(s)"),
            ),
            db_write_enabled: raw.db_write_enabled,
        })
    }
}

/// Consecutive failures after which the orphaned-telemetry drain stops trying.
///
/// A drain that fails is a full plane scan that failed, so an unbounded retry
/// is a full plane scan every sample interval, forever, on a node that is
/// already unhealthy. Three is enough to ride out a transient timeout and few
/// enough that a genuinely broken plane costs three passes, not thousands.
pub(super) const MAX_DRAIN_ATTEMPTS: u32 = 3;

#[derive(Debug, Default)]
pub struct SamplerRuntimeState {
    pub(super) inner: Mutex<SamplerStatus>,
    /// Last complete sync-engine snapshot. The sampler publishes it; the
    /// request path only clones it and never waits on sync worker locks.
    pub(super) sync: Mutex<Option<CachedSyncHealth>>,
    pub(super) request_ops_rollup: Mutex<HashMap<String, crate::request_telemetry::OpAggregate>>,
    /// Orphaned-telemetry drain bookkeeping. Per-`Host`, not a process global,
    /// so a test's drain state cannot leak into the next test.
    pub(super) drain: Mutex<DrainState>,
    /// Node-local TTL sweeper bookkeeping. Independent of the telemetry
    /// count-cap drain so a settled telemetry plane cannot silence expiry.
    pub(super) ttl: Mutex<TtlSweepState>,
    /// Last atom reverse-edge health read by its background owner task.
    /// Status clones this value and never reads durable atom-ref records.
    pub(super) atom_ref_edges: Mutex<Option<AtomRefEdgeHealth>>,
    /// Last complete status snapshot published by the sampler. `/api/status`
    /// clones this value and never admits a group.
    pub(super) last_status: Mutex<Option<std::sync::Arc<StatusSnapshot>>>,
}

#[derive(Debug, Default)]
pub(super) struct DrainState {
    pub(super) settled: bool,
    pub(super) failures: u32,
    pub(super) in_flight: bool,
}

#[derive(Debug, Clone)]
pub(super) struct CachedSyncHealth {
    pub(super) health: SyncHealth,
    pub(super) updated_at: Instant,
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
}

impl SamplerRuntimeState {
    pub fn background_writes_in_flight(&self) -> bool {
        self.drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            || self
                .ttl
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

    pub(super) fn cache_sync_health(&self, health: SyncHealth) {
        *self
            .sync
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(CachedSyncHealth {
            health,
            updated_at: Instant::now(),
        });
    }

    pub(super) fn cached_sync_health(&self) -> Option<(SyncHealth, Duration)> {
        self.sync
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|cached| (cached.health.clone(), cached.updated_at.elapsed()))
    }

    pub(super) fn drain_settled(&self) -> bool {
        self.drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settled
    }

    /// Claim the right to run a drain. `false` means one is already running, or
    /// there is nothing left to do — either way, do not start another.
    pub(super) fn begin_drain(&self) -> bool {
        let mut drain = self
            .drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if drain.settled || drain.in_flight {
            return false;
        }
        drain.in_flight = true;
        true
    }

    pub(super) fn end_drain(&self) {
        self.drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight = false;
    }

    pub(super) fn settle_drain(&self) {
        self.drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .settled = true;
    }

    pub(super) fn record_drain_failure(&self) {
        let mut drain = self
            .drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drain.failures += 1;
        if drain.failures >= MAX_DRAIN_ATTEMPTS {
            drain.settled = true;
            tracing::warn!(
                target: "lastdbd::self_metrics",
                failures = drain.failures,
                "orphaned-telemetry drain giving up for this process; restart the daemon to retry"
            );
        }
    }

    pub(crate) fn begin_ttl_sweep(&self) -> bool {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A completed empty pass only describes the last observed cutoff. A
        // later write can age past that cutoff, or an operator can add a new
        // policy, without restarting the daemon. Keep the single-flight guard,
        // but let the next sampler tick check the bounded policy and key paths.
        if ttl.in_flight || ttl.failures >= MAX_DRAIN_ATTEMPTS {
            return false;
        }
        ttl.settled = false;
        ttl.in_flight = true;
        true
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

    pub(crate) fn record_ttl_sweep_failure(&self) {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ttl.failures += 1;
        if ttl.failures >= MAX_DRAIN_ATTEMPTS {
            ttl.settled = true;
            tracing::warn!(
                target: "lastdbd::self_metrics",
                failures = ttl.failures,
                "ttl sweep giving up for this process; restart the daemon to retry"
            );
        }
    }

    pub(crate) fn mark_ttl_sweep_pass(&self, removed: usize, now: u64) {
        let mut ttl = self
            .ttl
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ttl.passes = ttl.passes.saturating_add(1);
        ttl.rows_reaped = ttl
            .rows_reaped
            .saturating_add(u64::try_from(removed).unwrap_or(u64::MAX));
        ttl.last_sweep_unix_secs = Some(now);
        ttl.failures = 0;
        if removed == 0 {
            ttl.settled = true;
        }
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
        }
    }

    pub(crate) fn cache_atom_ref_edge_health(&self, health: AtomRefEdgeHealth) {
        *self
            .atom_ref_edges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(health);
    }

    pub(super) fn atom_ref_edge_health(&self) -> Option<AtomRefEdgeHealth> {
        self.atom_ref_edges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl SamplerRuntimeState {
    pub fn snapshot(&self) -> SamplerStatus {
        let mut status = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // Read the sink setting here rather than storing it at tick time: it
        // is configuration, so it must be reportable before the first sample
        // and must not be stale if the daemon was started without the flag.
        status.db_write_enabled = Some(self_metrics_db_write_enabled());
        status
    }

    pub(super) fn record_success(&self, sampled_at: u64, sample_count: usize, cap: usize) {
        *self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = SamplerStatus {
            last_sample_at: Some(sampled_at),
            last_error: None,
            sample_count: g_lifetime(sample_count as u64, Unit::Named("sample(s)")),
            retention_max_samples: g_instant(cap as u64, Unit::Named("sample(s)")),
            // Overwritten by `snapshot()` from the live setting; stored state
            // never carries the sink flag so it cannot go stale here.
            db_write_enabled: None,
        };
    }

    pub(super) fn record_error(&self, error: String, cap: usize) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.last_error = Some(error);
        inner.retention_max_samples = g_instant(cap as u64, Unit::Named("sample(s)"));
    }

    pub(super) fn request_ops_rollup_delta(
        &self,
        snapshot: &crate::request_telemetry::RequestTelemetrySnapshot,
    ) -> Vec<crate::request_telemetry::OpAggregate> {
        let current = request_ops_rollup_aggregates(snapshot);
        let mut previous = self
            .request_ops_rollup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut deltas = Vec::new();
        for aggregate in current {
            let key = request_ops_rollup_key(&aggregate);
            let delta = match previous.get(&key) {
                Some(prev) => crate::request_telemetry::OpAggregate {
                    client: aggregate.client.clone(),
                    kind: aggregate.kind,
                    schema: aggregate.schema.clone(),
                    // Identity, not a counter: carried, never subtracted.
                    route: aggregate.route.clone(),
                    count: aggregate.count.saturating_sub(prev.count),
                    sum_ms: aggregate.sum_ms.saturating_sub(prev.sum_ms),
                    max_ms: aggregate.max_ms,
                    last_ts_ms: aggregate.last_ts_ms,
                    error_count: aggregate.error_count.saturating_sub(prev.error_count),
                    sum_cold_shard_loads: aggregate
                        .sum_cold_shard_loads
                        .saturating_sub(prev.sum_cold_shard_loads),
                    sum_body_bytes: aggregate.sum_body_bytes.saturating_sub(prev.sum_body_bytes),
                    // Same as max_ms: running max, not an interval delta.
                    max_body_bytes: aggregate.max_body_bytes,
                    // The durable rollup row persists `error_count` only, so a
                    // per-status delta has nowhere to land. Left empty rather
                    // than copied from the cumulative aggregate, which would
                    // overstate this interval's failures.
                    error_statuses: std::collections::BTreeMap::new(),
                    error_statuses_overflow: 0,
                    last_error_status: None,
                    last_error_ts_ms: None,
                    // Phase sums are delta-able by construction (sums, never
                    // maxes), so the interval delta the rollup row persists
                    // is honest.
                    phase_sums: aggregate.phase_sums.saturating_sub(prev.phase_sums),
                    phase_count: aggregate.phase_count.saturating_sub(prev.phase_count),
                    // A cumulative sum, so it deltas honestly alongside the
                    // phase sums it is the denominator's wall clock for.
                    phased_sum_ms: aggregate.phased_sum_ms.saturating_sub(prev.phased_sum_ms),
                    // Cumulative sums, so they delta honestly — and they must
                    // be delta'd rather than copied, because the ratio between
                    // them is the whole reading: a cumulative pair carried into
                    // an interval row would average this interval's write shape
                    // together with every interval before it.
                    sum_molecules_persisted: aggregate
                        .sum_molecules_persisted
                        .saturating_sub(prev.sum_molecules_persisted),
                    sum_molecule_store_commits: aggregate
                        .sum_molecule_store_commits
                        .saturating_sub(prev.sum_molecule_store_commits),
                    // Same reasoning as the molecule pair above: the ratio
                    // between these two IS the reading, so a cumulative pair
                    // carried into an interval row would average this
                    // interval's commit shape with every interval before it.
                    sum_resident_commits: aggregate
                        .sum_resident_commits
                        .saturating_sub(prev.sum_resident_commits),
                    sum_resident_operations: aggregate
                        .sum_resident_operations
                        .saturating_sub(prev.sum_resident_operations),
                },
                None => aggregate.clone(),
            };
            // Known limitation (inherited): baselines are never pruned. An
            // aggregate key evicted from the in-memory table and later
            // recreated restarts its cumulative counters at zero, so every
            // saturating_sub against the stale baseline reads 0 — the bucket
            // under-reports until its counters pass the old baseline again.
            previous.insert(key, aggregate);
            if delta.count > 0 || delta.sum_ms > 0 || delta.error_count > 0 {
                deltas.push(delta);
            }
        }
        deltas
    }
}

// lint:file-size-ok moved verbatim from self_metrics.rs; cohesive unit, split further in a later pass
