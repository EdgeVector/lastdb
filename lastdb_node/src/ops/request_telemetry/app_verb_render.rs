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

pub(super) fn percentile_95_ms(durations: &mut [u64]) -> Option<u64> {
    if durations.is_empty() {
        return None;
    }
    durations.sort_unstable();
    let rank = (durations.len().saturating_mul(95).saturating_add(99)) / 100;
    let idx = rank.saturating_sub(1).min(durations.len() - 1);
    Some(durations[idx])
}
