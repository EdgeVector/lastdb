use super::*;

#[derive(Debug)]
pub(super) struct Inner {
    pub(super) ring: VecDeque<OpSample>,
    pub(super) ring_cap: usize,
    pub(super) top_n: usize,
    pub(super) aggregates: HashMap<String, OpAggregate>,
    pub(super) sample_count: u64,
    pub(super) key_use: KeyUseWindow,
}

impl Inner {
    pub(super) fn new(ring_cap: usize, top_n: usize) -> Self {
        Self {
            ring: VecDeque::with_capacity(ring_cap),
            ring_cap: ring_cap.max(1),
            top_n: top_n.max(1),
            aggregates: HashMap::new(),
            sample_count: 0,
            key_use: KeyUseWindow::default(),
        }
    }

    pub(super) fn record(&mut self, sample: OpSample) {
        self.sample_count = self.sample_count.saturating_add(1);

        let key = OpAggregate::key(
            &sample.client,
            sample.kind,
            sample.schema.as_deref(),
            sample.route.as_deref(),
        );
        let agg = self.aggregates.entry(key).or_insert_with(|| OpAggregate {
            client: sample.client.clone(),
            kind: sample.kind,
            schema: sample.schema.clone(),
            route: OpAggregate::identity_route(sample.schema.as_deref(), sample.route.as_deref()),
            count: 0,
            sum_ms: 0,
            max_ms: 0,
            last_ts_ms: 0,
            error_count: 0,
            sum_cold_shard_loads: 0,
            sum_body_bytes: 0,
            max_body_bytes: 0,
            error_statuses: BTreeMap::new(),
            error_statuses_overflow: 0,
            last_error_status: None,
            last_error_ts_ms: None,
            phase_sums: PhaseTimings::default(),
            phase_count: 0,
            phased_sum_ms: 0,
            sum_molecules_persisted: 0,
            sum_molecule_store_commits: 0,
            sum_resident_commits: 0,
            sum_resident_operations: 0,
        });
        agg.count = agg.count.saturating_add(1);
        agg.sum_ms = agg.sum_ms.saturating_add(sample.duration_ms);
        agg.max_ms = agg.max_ms.max(sample.duration_ms);
        agg.last_ts_ms = sample.ts_ms;
        agg.sum_cold_shard_loads = agg
            .sum_cold_shard_loads
            .saturating_add(sample.cold_shard_loads.unwrap_or(0));
        agg.sum_body_bytes = agg.sum_body_bytes.saturating_add(sample.body_bytes);
        agg.max_body_bytes = agg.max_body_bytes.max(sample.body_bytes);
        agg.sum_molecules_persisted = agg
            .sum_molecules_persisted
            .saturating_add(sample.molecules_persisted);
        agg.sum_molecule_store_commits = agg
            .sum_molecule_store_commits
            .saturating_add(sample.molecule_store_commits);
        agg.sum_resident_commits = agg
            .sum_resident_commits
            .saturating_add(sample.resident_commits);
        agg.sum_resident_operations = agg
            .sum_resident_operations
            .saturating_add(sample.resident_operations);
        // Constant-time numeric sums only — this runs under the global lock
        // on every completion, next to an O(n) eviction scan that is already
        // the budget ceiling for this path.
        agg.phase_sums.accumulate(sample.phases);
        if !sample.phases.is_empty() {
            agg.phase_count = agg.phase_count.saturating_add(1);
            // Same guard as `phase_count`, so the two always describe the
            // same set of requests — that pairing is what makes the residual
            // in `unphased_us` a like-for-like comparison.
            agg.phased_sum_ms = agg.phased_sum_ms.saturating_add(sample.duration_ms);
        }
        if sample.status >= 400 {
            agg.error_count = agg.error_count.saturating_add(1);
            record_error_status(
                &mut agg.error_statuses,
                &mut agg.error_statuses_overflow,
                sample.status,
                sample.ts_ms,
                &mut agg.last_error_status,
                &mut agg.last_error_ts_ms,
            );
        }

        // Bound aggregate map growth under label cardinality attacks.
        if self.aggregates.len() > MAX_AGGREGATE_KEYS {
            // Drop the aggregate with the oldest last_ts_ms.
            if let Some(evict) = self
                .aggregates
                .iter()
                .min_by_key(|(_, a)| a.last_ts_ms)
                .map(|(k, _)| k.clone())
            {
                self.aggregates.remove(&evict);
            }
        }

        self.ring.push_back(sample);
        while self.ring.len() > self.ring_cap {
            self.ring.pop_front();
        }
    }

    pub(super) fn snapshot(&self) -> RequestTelemetrySnapshot {
        let recent: Vec<OpSample> = self.ring.iter().rev().cloned().collect();

        // `recent` keeps every sample — full fidelity. The RANKINGS drop idle
        // wait, because "slowest" and "most total time" are read as "biggest
        // consumer", and a sleeping long-poll consumes nothing.
        let mut by_duration: Vec<OpSample> = self
            .ring
            .iter()
            .filter(|s| !s.kind.is_idle_wait())
            .cloned()
            .collect();
        by_duration.sort_by(|a, b| {
            b.duration_ms
                .cmp(&a.duration_ms)
                .then_with(|| b.ts_ms.cmp(&a.ts_ms))
        });
        by_duration.truncate(self.top_n);

        // Only the TIME-based rankings exclude idle wait. `top_by_count` below
        // ranks call count, which is honest for a watcher — a chatty poller is
        // genuinely chatty — so it keeps every kind.
        let mut aggregates: Vec<OpAggregate> = self.aggregates.values().cloned().collect();
        let (mut idle_wait, work): (Vec<OpAggregate>, Vec<OpAggregate>) = aggregates
            .iter()
            .cloned()
            .partition(|a| a.kind.is_idle_wait());
        idle_wait.sort_by(|a, b| b.sum_ms.cmp(&a.sum_ms).then_with(|| b.count.cmp(&a.count)));
        idle_wait.truncate(self.top_n);

        let mut top_by_total_ms = work.clone();
        top_by_total_ms.sort_by(|a, b| {
            b.sum_ms
                .cmp(&a.sum_ms)
                .then_with(|| b.max_ms.cmp(&a.max_ms))
                .then_with(|| b.count.cmp(&a.count))
        });
        top_by_total_ms.truncate(self.top_n);

        let mut top_by_body_bytes = work.clone();
        top_by_body_bytes.sort_by(|a, b| {
            b.sum_body_bytes
                .cmp(&a.sum_body_bytes)
                .then_with(|| b.max_body_bytes.cmp(&a.max_body_bytes))
                .then_with(|| b.count.cmp(&a.count))
                .then_with(|| a.client.cmp(&b.client))
        });
        top_by_body_bytes.truncate(self.top_n);
        // Hide the table when nothing reported a body — same precedent as
        // cold-load ranking so quiet windows stay byte-identical.
        if top_by_body_bytes
            .first()
            .is_none_or(|a| a.sum_body_bytes == 0)
        {
            top_by_body_bytes.clear();
        }

        let mut top_by_cold_shard_loads = work;
        top_by_cold_shard_loads.sort_by(|a, b| {
            b.sum_cold_shard_loads
                .cmp(&a.sum_cold_shard_loads)
                .then_with(|| b.sum_ms.cmp(&a.sum_ms))
                .then_with(|| b.count.cmp(&a.count))
        });
        top_by_cold_shard_loads.truncate(self.top_n);
        // Nothing to rank when the backend reports no loads at all — keep the
        // table absent rather than printing a column of zeroes.
        if top_by_cold_shard_loads
            .first()
            .is_none_or(|a| a.sum_cold_shard_loads == 0)
        {
            top_by_cold_shard_loads.clear();
        }

        aggregates.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| b.sum_ms.cmp(&a.sum_ms))
                .then_with(|| b.max_ms.cmp(&a.max_ms))
        });
        aggregates.truncate(self.top_n);

        let app_verb = self.app_verb_rollups();

        RequestTelemetrySnapshot {
            recent,
            top_by_duration: by_duration,
            top_by_total_ms,
            top_by_count: aggregates,
            top_by_cold_shard_loads,
            top_by_body_bytes,
            app_verb,
            idle_wait,
            sample_count: self.sample_count,
            ring_capacity: self.ring_cap,
            persist_lanes: Vec::new(),
            key_use: self.key_use.snapshot(unix_millis()),
        }
    }

    pub(super) fn app_verb_rollups(&self) -> Vec<AppVerbAggregate> {
        // Work-only: idle long-poll duration is client-requested sleep, not
        // latency. Ranking it by sum_ms in the compact "App / verb latency"
        // table is the misread that put `lastgit local_watch` at #2 on status.
        let mut recent_durations: HashMap<String, Vec<u64>> = HashMap::new();
        for sample in &self.ring {
            if sample.kind.is_idle_wait() {
                continue;
            }
            recent_durations
                .entry(AppVerbAggregate::key(&sample.client, sample.kind))
                .or_default()
                .push(sample.duration_ms);
        }

        let mut by_app_verb: HashMap<String, AppVerbAggregate> = HashMap::new();
        for aggregate in self.aggregates.values() {
            if aggregate.kind.is_idle_wait() {
                continue;
            }
            let key = AppVerbAggregate::key(&aggregate.client, aggregate.kind);
            let rollup = by_app_verb.entry(key).or_insert_with(|| AppVerbAggregate {
                client: aggregate.client.clone(),
                kind: aggregate.kind,
                count: 0,
                sum_ms: 0,
                max_ms: 0,
                error_count: 0,
                sum_body_bytes: 0,
                max_body_bytes: 0,
                error_statuses: BTreeMap::new(),
                error_statuses_overflow: 0,
                last_error_status: None,
                last_error_ts_ms: None,
                p95_ms: None,
                recent_count: 0,
            });
            rollup.count = rollup.count.saturating_add(aggregate.count);
            rollup.sum_ms = rollup.sum_ms.saturating_add(aggregate.sum_ms);
            rollup.max_ms = rollup.max_ms.max(aggregate.max_ms);
            rollup.error_count = rollup.error_count.saturating_add(aggregate.error_count);
            rollup.sum_body_bytes = rollup
                .sum_body_bytes
                .saturating_add(aggregate.sum_body_bytes);
            rollup.max_body_bytes = rollup.max_body_bytes.max(aggregate.max_body_bytes);
            merge_error_statuses(rollup, aggregate);
        }

        for (key, rollup) in &mut by_app_verb {
            if let Some(mut durations) = recent_durations.remove(key) {
                rollup.recent_count = durations.len() as u64;
                rollup.p95_ms = percentile_95_ms(&mut durations);
            }
        }

        let mut rollups: Vec<AppVerbAggregate> = by_app_verb.into_values().collect();
        rollups.sort_by(|a, b| {
            b.sum_ms
                .cmp(&a.sum_ms)
                .then_with(|| b.max_ms.cmp(&a.max_ms))
                .then_with(|| b.count.cmp(&a.count))
                .then_with(|| a.client.cmp(&b.client))
                .then_with(|| a.kind.as_str().cmp(b.kind.as_str()))
        });
        rollups
    }
}

/// Process-wide request telemetry runtime (shared via [`std::sync::Arc`] on Host).
#[derive(Debug)]
pub struct Runtime {
    pub(super) inner: Mutex<Inner>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(DEFAULT_RING_CAP, DEFAULT_TOP_N)
    }
}

impl Runtime {
    pub fn new(ring_cap: usize, top_n: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::new(ring_cap, top_n)),
        }
    }

    pub fn record(&self, sample: OpSample) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(sample);
    }

    pub fn record_with_tip_keys(&self, sample: OpSample, keys: &TipKeySketch) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .key_use
            .record(sample.ts_ms, &sample.client, sample.schema.as_deref(), keys);
        inner.record(sample);
    }

    pub fn snapshot(&self) -> RequestTelemetrySnapshot {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
    }
}
