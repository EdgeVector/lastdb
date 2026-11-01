use super::*;

mod phase_lines;
pub(crate) use phase_lines::*;

/// Persist-lane occupancy lines for `lastdb status` / `lastdb ops`.
#[must_use]
pub fn persist_lane_lines(snap: &RequestTelemetrySnapshot) -> Vec<String> {
    if snap.persist_lanes.is_empty() {
        return Vec::new();
    }
    snap.persist_lanes
        .iter()
        .map(|row| {
            let schema = if row.schema.is_empty() {
                "none"
            } else {
                row.schema.as_str()
            };
            format!(
                "Persist lane: schema={schema} reserved_bytes={} fair_share_bytes={} \
                 queued_entries={} reserved_entries={} oldest_age_ms={} \
                 refuse_bytes={} refuse_entries={} refuse_unhealthy={} \
                 unhealthy_lanes={} quarantined={} breaker_trips={}",
                row.reserved_bytes,
                row.fair_share_bytes,
                row.queued_entries,
                row.reserved_entries,
                row.oldest_age_ms,
                row.refuse_bytes,
                row.refuse_entries,
                row.refuse_unhealthy,
                row.unhealthy_lanes,
                row.quarantined,
                row.breaker_trips
            )
        })
        .collect()
}

/// Human-readable lines for `lastdb ops` / status extensions.
pub fn snapshot_lines(snap: &RequestTelemetrySnapshot) -> Vec<String> {
    snapshot_lines_with_schema_labels(snap, &SchemaLabels::new())
}

/// Human-readable request telemetry with optional schema catalog labels.
///
/// The status JSON keeps the runtime schema `name` as its stable key. The
/// `lastdb ops` CLI supplies a best-effort `name -> descriptive_name` map so
/// terminal rows show the product name plus an eight-character stable ID.
pub fn snapshot_lines_with_schema_labels(
    snap: &RequestTelemetrySnapshot,
    labels: &SchemaLabels,
) -> Vec<String> {
    let mut lines = vec![format!(
        "Request ops: {} lifetime samples | recent ring {}/{}",
        human_count(snap.sample_count),
        snap.recent.len().min(snap.ring_capacity),
        snap.ring_capacity
    )];
    lines.extend(persist_lane_lines(snap));

    if snap.top_by_total_ms.is_empty() && snap.idle_wait.is_empty() {
        // Cheap `/api/status` keeps sample_count but strips the forensic
        // tables — do not claim "no traffic" when the ring was deliberately
        // omitted (use `lastdb ops` / `?recent=1` for rankings).
        if snap.sample_count > 0 && snap.recent.is_empty() {
            lines.push(
                "  (forensic ring omitted — use `lastdb ops` or GET /api/status?recent=1)"
                    .to_string(),
            );
        } else {
            lines.push("  (no samples yet — traffic will appear here)".to_string());
        }
        return lines;
    }

    lines.extend(app_verb_table_lines(snap));

    // `key_use` stays in the JSON for its readers. The record model keeps it
    // empty, so the text output has no table for it.
    lines.extend(top_total_time_lines(snap, labels));
    lines.extend(body_volume_lines(snap, labels));
    lines.extend(idle_wait_lines(snap));
    lines.extend(slowest_request_lines(snap, labels));

    // LAST on purpose: `lastdb status` prints only the first few of these
    // lines, so a new table above the app/verb block would silently push
    // the offender summary out of `status`.
    lines.extend(phase_table_lines(&snap.top_by_total_ms, labels));

    lines
}

/// Top keys by total service time over the process lifetime.
fn top_total_time_lines(snap: &RequestTelemetrySnapshot, labels: &SchemaLabels) -> Vec<String> {
    let mut lines = Vec::new();
    let now = unix_millis();
    lines.push(String::new());
    lines.push("Top by total time — process lifetime; idle long-poll wait excluded".to_string());
    lines.push(
        "  Service time excludes socket queue wait. See the queue_wait phase below.".to_string(),
    );
    lines.extend(super::rollup::calls_table(
        &snap.top_by_total_ms,
        labels,
        now,
    ));
    lines
}

/// Request body volume over the process lifetime.
fn body_volume_lines(snap: &RequestTelemetrySnapshot, labels: &SchemaLabels) -> Vec<String> {
    let mut lines = Vec::new();
    if !snap.top_by_body_bytes.is_empty() {
        lines.push(String::new());
        lines.push(
            "Request body volume — process lifetime (caller input, not disk growth)".to_string(),
        );
        lines.extend(super::rollup::body_volume_table(
            &snap.top_by_body_bytes,
            labels,
        ));
    }
    lines
}

/// Idle long-poll wait, kept apart from node work.
fn idle_wait_lines(snap: &RequestTelemetrySnapshot) -> Vec<String> {
    let mut lines = Vec::new();
    if !snap.idle_wait.is_empty() {
        lines.push(String::new());
        lines.push("Idle long-poll wait — client-requested sleep, not node work".to_string());
        lines.extend(super::rollup::idle_wait_table(&snap.idle_wait));
    }
    lines
}

/// Slowest recent requests with per-request detail.
fn slowest_request_lines(snap: &RequestTelemetrySnapshot, labels: &SchemaLabels) -> Vec<String> {
    let mut lines = Vec::new();
    if !snap.top_by_duration.is_empty() {
        lines.push(String::new());
        lines.push("Slowest recent requests — idle long-poll wait excluded".to_string());
        let recent: Vec<&OpSample> = snap.top_by_duration.iter().take(10).collect();
        let rows: Vec<Vec<String>> = recent
            .iter()
            .enumerate()
            .map(|(i, s)| {
                vec![
                    (i + 1).to_string(),
                    human_duration_ms(s.duration_ms),
                    truncate_cell(&s.client, OPS_CLIENT_CELL_WIDTH),
                    s.kind.as_str().to_string(),
                    identity_cell(s.schema.as_deref(), s.route.as_deref(), labels),
                    s.status.to_string(),
                    s.rows.map_or_else(|| "-".to_string(), human_count),
                    human_bytes(s.body_bytes),
                ]
            })
            .collect();
        lines.extend(aligned_table(
            &[
                "#",
                "TIME",
                "CLIENT",
                "KIND",
                "SCHEMA / ROUTE",
                "HTTP",
                "ROWS",
                "BODY",
            ],
            &rows,
            &[true, true, false, false, false, true, true, true],
        ));

        let mut detail_lines = Vec::new();
        for (i, s) in recent.iter().enumerate() {
            let mut request = Vec::new();
            if let Some(req) = s.request_id.as_deref() {
                request.push(format!("req={}", truncate_cell(req, 48)));
            }
            if let Some(path) = s.path.as_deref() {
                request.push(format!("path={path}"));
            }
            let uds = uds_pool_detail(s.uds_in_flight, s.uds_workers, s.uds_queue_capacity);
            if !uds.is_empty() {
                request.push(uds.trim().to_string());
            }
            let peer = peer_detail(s.peer_pid, s.peer_comm.as_deref());
            if !peer.is_empty() {
                request.push(peer.trim().to_string());
            }
            detail_lines.extend(wrap_detail(&format!("    {}. request: ", i + 1), request));

            if !s.phases.is_empty() {
                let wall_us = s.duration_ms.saturating_mul(1_000);
                let unphased = i64::try_from(wall_us).unwrap_or(i64::MAX)
                    - i64::try_from(s.phases.within_wall_us()).unwrap_or(i64::MAX);
                let mut phases = phase_tokens(s.phases);
                phases.push(error_cell(&residual_detail(unphased, wall_us)));
                let resident = resident_commit_detail(s.resident_operations, s.resident_commits);
                if !resident.is_empty() {
                    phases.push(resident.trim().to_string());
                }
                detail_lines.extend(wrap_detail(&format!("    {}. phases:  ", i + 1), phases));
            }
        }
        if !detail_lines.is_empty() {
            lines.push("  Details:".to_string());
            lines.extend(detail_lines);
        }
    }
    lines
}

pub(super) fn uds_pool_detail(
    in_flight: Option<usize>,
    workers: Option<usize>,
    queue_capacity: Option<usize>,
) -> String {
    match (in_flight, workers, queue_capacity) {
        (Some(in_flight), Some(workers), Some(queue_capacity)) => {
            format!(" uds={in_flight}/{workers}+{queue_capacity}")
        }
        _ => String::new(),
    }
}
