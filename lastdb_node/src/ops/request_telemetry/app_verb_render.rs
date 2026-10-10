use super::*;

/// Human-readable app/client by verb latency rollup for `lastdb ops --by-app`
/// and the compact header of `lastdb status` / `lastdb ops`.
///
/// Work verbs only. Idle long-poll wait is rendered separately (see
/// [`snapshot_lines`]) so duration is never misread as store latency.
///
/// **Population honesty:** `count` / `avg` / `max` are process-lifetime
/// aggregates; `p95` is computed only over samples still in the recent ring
/// (`recent_count`). Field order stays `count … avg … p95 … max … err` so
/// external parsers (ops-terminal) keep working; the trailing `[pop: …]`
/// annotation makes the two statistical populations un-mixable.
pub fn app_verb_lines(snap: &RequestTelemetrySnapshot) -> Vec<String> {
    let mut lines = vec![
        "App / verb latency (idle excluded; count/avg/max=lifetime process totals; p95=recent ring):"
            .to_string(),
    ];
    // Defensive filter: older snapshots / hand-built fixtures may still mix
    // idle kinds into `app_verb`. The live rollup already drops them.
    let work: Vec<&AppVerbAggregate> = snap
        .app_verb
        .iter()
        .filter(|a| !a.kind.is_idle_wait())
        .collect();
    if work.is_empty() {
        lines.push("  (no app/verb work aggregates yet)".to_string());
        return lines;
    }

    let now = unix_millis();
    for (i, a) in work.into_iter().enumerate() {
        let p95 = a
            .p95_ms
            .map_or_else(|| "-".to_string(), |ms| format!("{ms}ms"));
        let body = if a.sum_body_bytes > 0 {
            format!(
                " body_sum={} body_max={}",
                human_bytes(a.sum_body_bytes),
                human_bytes(a.max_body_bytes)
            )
        } else {
            String::new()
        };
        lines.push(format!(
            "  {}. app={} verb={} count={} avg={}ms p95={} max={}ms err={}{}{} [pop: count/avg/max=lifetime; p95=ring n={}]",
            i + 1,
            a.client,
            a.kind.as_str(),
            a.count,
            a.avg_ms(),
            p95,
            a.max_ms,
            a.error_count,
            a.error_detail(now),
            body,
            a.recent_count,
        ));
    }
    lines
}

/// Aligned app/verb summary for the default human `lastdb ops` view.
///
/// Keep [`app_verb_lines`] as the stable key-value surface for `--by-app` and
/// its existing ops-terminal parser. The default view is for people and uses
/// columns plus an explicit population note.
pub(super) fn app_verb_table_lines(snap: &RequestTelemetrySnapshot) -> Vec<String> {
    let work: Vec<&AppVerbAggregate> = snap
        .app_verb
        .iter()
        .filter(|a| !a.kind.is_idle_wait())
        .take(10)
        .collect();
    let mut lines = vec![
        "App / verb latency — idle excluded".to_string(),
        "  Lifetime: calls, avg, max, errors, body. Recent ring: p95 and N.".to_string(),
    ];
    if work.is_empty() {
        lines.push("  (no app/verb work aggregates yet)".to_string());
        return lines;
    }

    let now = unix_millis();
    let rows: Vec<Vec<String>> = work
        .iter()
        .enumerate()
        .map(|(i, a)| {
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                human_count(a.count),
                human_duration_ms(a.avg_ms()),
                a.p95_ms.map_or_else(|| "-".to_string(), human_duration_ms),
                human_count(a.recent_count),
                human_duration_ms(a.max_ms),
                human_count(a.error_count),
                if a.sum_body_bytes == 0 {
                    "-".to_string()
                } else {
                    human_bytes(a.sum_body_bytes)
                },
                error_cell(&a.error_detail(now)),
            ]
        })
        .collect();
    lines.extend(aligned_table(
        &[
            "#",
            "APP",
            "VERB",
            "CALLS",
            "AVG",
            "P95",
            "N",
            "MAX",
            "ERR",
            "BODY",
            "ERROR DETAIL",
        ],
        &rows,
        &[
            true, false, false, true, true, true, true, true, true, true, false,
        ],
    ));
    lines
}

/// One (client, kind, schema) cold-load rollup over samples still in the ring.
#[derive(Debug, Clone)]
pub(super) struct RingColdLoadRow {
    pub(super) client: String,
    pub(super) kind: OpKind,
    pub(super) schema: Option<String>,
    pub(super) route: Option<String>,
    pub(super) sum_loads: u64,
    pub(super) count: u64,
    pub(super) sum_ms: u64,
}

impl RingColdLoadRow {
    pub(super) fn loads_per_call(&self) -> u64 {
        self.sum_loads.checked_div(self.count).unwrap_or(0)
    }

    /// Cold shard loads per second of service time on this key.
    ///
    /// See [`OpAggregate::cold_shard_loads_per_service_second`] — the ring
    /// window carries the same store-wide attribution as the lifetime table,
    /// so it needs the same normaliser to be comparable across keys.
    pub(super) fn loads_per_service_second(&self) -> u64 {
        self.sum_loads
            .saturating_mul(1000)
            .checked_div(self.sum_ms)
            .unwrap_or(0)
    }
}

/// Rank cold shard loads from the **recent ring only** — the live read-cost
/// signal agents should act on. Lifetime `top_by_cold_shard_loads` is a
/// separate table.
pub(super) fn ring_cold_load_rows(snap: &RequestTelemetrySnapshot) -> Vec<RingColdLoadRow> {
    use std::collections::HashMap;

    let mut by_key: HashMap<String, RingColdLoadRow> = HashMap::new();
    for sample in &snap.recent {
        if sample.kind.is_idle_wait() {
            continue;
        }
        let Some(loads) = sample.cold_shard_loads else {
            continue;
        };
        // Include zeros so a post-burst quiet ring can honestly report
        // loads/call_ring=0 against a still-high lifetime average.
        let key = OpAggregate::key(
            &sample.client,
            sample.kind,
            sample.schema.as_deref(),
            sample.route.as_deref(),
        );
        let row = by_key.entry(key).or_insert_with(|| RingColdLoadRow {
            client: sample.client.clone(),
            kind: sample.kind,
            schema: sample.schema.clone(),
            route: OpAggregate::identity_route(sample.schema.as_deref(), sample.route.as_deref()),
            sum_loads: 0,
            count: 0,
            sum_ms: 0,
        });
        row.sum_loads = row.sum_loads.saturating_add(loads);
        row.count = row.count.saturating_add(1);
        row.sum_ms = row.sum_ms.saturating_add(sample.duration_ms);
    }

    let mut rows: Vec<RingColdLoadRow> = by_key.into_values().collect();
    // Hide the table when every ring sample reports zero loads (or none
    // reported the field) — same zero-hide precedent as the lifetime table.
    if rows.iter().all(|r| r.sum_loads == 0) {
        // Keep rows that observed the field with zeros only when *some*
        // lifetime cold-load ranking still exists: that is the exact
        // misread this card prevents (lifetime ≫ 0, ring = 0).
        if snap.top_by_cold_shard_loads.is_empty() {
            return Vec::new();
        }
        // Show zero-load ring rows so the disagree is visible.
    } else {
        rows.retain(|r| r.sum_loads > 0);
    }
    rows.sort_by(|a, b| {
        b.sum_loads
            .cmp(&a.sum_loads)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| a.client.cmp(&b.client))
    });
    rows.truncate(10);
    rows
}

pub(super) fn percentile_95_ms(durations: &mut [u64]) -> Option<u64> {
    if durations.is_empty() {
        return None;
    }
    durations.sort_unstable();
    let rank = (durations.len().saturating_mul(95).saturating_add(99)) / 100;
    let idx = rank.saturating_sub(1).min(durations.len() - 1);
    Some(durations[idx])
}
