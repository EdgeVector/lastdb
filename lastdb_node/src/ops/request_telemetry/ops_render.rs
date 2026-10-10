use super::*;

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
                 queued_entries={} reserved_entries={} oldest_age_ms={} exclusive_hold_us={} \
                 refuse_bytes={} refuse_entries={} refuse_unhealthy={} \
                 write_throughs={} unhealthy_lanes={} quarantined={} breaker_trips={}",
                row.reserved_bytes,
                row.fair_share_bytes,
                row.queued_entries,
                row.reserved_entries,
                row.oldest_age_ms,
                row.exclusive_hold_us,
                row.refuse_bytes,
                row.refuse_entries,
                row.refuse_unhealthy,
                row.write_throughs,
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
// lint:fn-size-ok moved verbatim from request_telemetry.rs; splitting this function is separate work.
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

    if let Some(key_use) = &snap.key_use {
        lines.push(String::new());
        lines.push(format!(
            "Returned field tip keys — last {}m, approximate distinct count (global {}, overflow {})",
            key_use.window_minutes,
            human_count(key_use.distinct_tip_keys),
            human_count(key_use.overflow_tip_keys)
        ));
        lines.push("  Client / schema                         Distinct tips".to_string());
        for row in key_use.series.iter().take(10) {
            lines.push(format!(
                "  {} / {}: {}",
                row.client,
                row.schema.as_deref().unwrap_or("-"),
                human_count(row.distinct_tip_keys)
            ));
        }
    }

    let now = unix_millis();
    lines.push(String::new());
    lines.push("Top by total time — process lifetime; idle long-poll wait excluded".to_string());
    lines.push(
        "  Service time excludes socket queue wait. See the queue_wait phase below.".to_string(),
    );
    let total_rows: Vec<Vec<String>> = snap
        .top_by_total_ms
        .iter()
        .take(10)
        .enumerate()
        .map(|(i, a)| {
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                human_count(a.count),
                human_duration_ms(a.sum_ms),
                human_duration_ms(a.avg_ms()),
                human_duration_ms(a.max_ms),
                human_count(a.error_count),
                error_cell(&a.error_detail(now)),
            ]
        })
        .collect();
    lines.extend(aligned_table(
        &[
            "#",
            "CLIENT",
            "KIND",
            "SCHEMA / ROUTE",
            "CALLS",
            "TOTAL",
            "AVG",
            "MAX",
            "ERR",
            "ERROR DETAIL",
        ],
        &total_rows,
        &[
            true, false, false, false, true, true, true, true, true, false,
        ],
    ));

    // Ring-window cold loads first: the ranking agents use for the *live*
    // window. Lifetime averages retain start-of-day cold-miss bursts forever and
    // must never be presented as the current per-call load.
    //
    // Neither table is a per-caller read cost, and the header must not imply one
    // — the counter behind both is store-wide (see `OpSample::cold_shard_loads`),
    // so a long request is charged for every concurrent caller's loads.
    let ring_cold = ring_cold_load_rows(snap);
    if !ring_cold.is_empty() {
        lines.push(String::new());
        lines.push("Cold shard loads — recent ring (observed during, not caused by)".to_string());
        lines.push("  Store-wide counter sampled around each request: a call is charged for loads concurrent callers triggered while it ran.".to_string());
        lines.push("  So LOAD/CALL rises with a key's DURATION. Compare LOAD/SEC — if it is flat across keys, this is one store-wide rate, not one heavy caller.".to_string());
        let rows: Vec<Vec<String>> = ring_cold
            .iter()
            .take(10)
            .enumerate()
            .map(|(i, row)| {
                vec![
                    (i + 1).to_string(),
                    truncate_cell(&row.client, OPS_CLIENT_CELL_WIDTH),
                    row.kind.as_str().to_string(),
                    identity_cell(row.schema.as_deref(), row.route.as_deref(), labels),
                    human_count(row.sum_loads),
                    human_count(row.count),
                    human_count(row.loads_per_call()),
                    human_count(row.loads_per_service_second()),
                    human_duration_ms(row.sum_ms),
                ]
            })
            .collect();
        lines.extend(aligned_table(
            &[
                "#",
                "CLIENT",
                "KIND",
                "SCHEMA / ROUTE",
                "LOADS",
                "CALLS",
                "LOAD/CALL",
                "LOAD/SEC",
                "TIME",
            ],
            &rows,
            &[true, false, false, false, true, true, true, true, true],
        ));
    }

    if !snap.top_by_cold_shard_loads.is_empty() {
        lines.push(String::new());
        lines.push(
            "Cold shard loads — process lifetime (observed during, not caused by)".to_string(),
        );
        lines.push("  Start-of-process cold misses remain in this average.".to_string());
        lines.push("  Store-wide counter sampled around each request: a call is charged for loads concurrent callers triggered while it ran.".to_string());
        lines.push("  So LOAD/CALL rises with a key's DURATION. Compare LOAD/SEC — if it is flat across keys, this is one store-wide rate, not one heavy caller.".to_string());
        let rows: Vec<Vec<String>> = snap
            .top_by_cold_shard_loads
            .iter()
            .take(10)
            .enumerate()
            .map(|(i, a)| {
                vec![
                    (i + 1).to_string(),
                    truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                    a.kind.as_str().to_string(),
                    identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                    human_count(a.sum_cold_shard_loads),
                    human_count(a.count),
                    human_count(a.avg_cold_shard_loads()),
                    human_count(a.cold_shard_loads_per_service_second()),
                    human_duration_ms(a.sum_ms),
                ]
            })
            .collect();
        lines.extend(aligned_table(
            &[
                "#",
                "CLIENT",
                "KIND",
                "SCHEMA / ROUTE",
                "LOADS",
                "CALLS",
                "LOAD/CALL",
                "LOAD/SEC",
                "TIME",
            ],
            &rows,
            &[true, false, false, false, true, true, true, true, true],
        ));
    }

    if !snap.top_by_body_bytes.is_empty() {
        lines.push(String::new());
        lines.push(
            "Request body volume — process lifetime (caller input, not disk growth)".to_string(),
        );
        let rows: Vec<Vec<String>> = snap
            .top_by_body_bytes
            .iter()
            .take(10)
            .enumerate()
            .map(|(i, a)| {
                vec![
                    (i + 1).to_string(),
                    truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                    a.kind.as_str().to_string(),
                    identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                    human_bytes(a.sum_body_bytes),
                    human_bytes(a.max_body_bytes),
                    human_bytes(a.avg_body_bytes()),
                    human_count(a.count),
                    human_duration_ms(a.sum_ms),
                ]
            })
            .collect();
        lines.extend(aligned_table(
            &[
                "#",
                "CLIENT",
                "KIND",
                "SCHEMA / ROUTE",
                "TOTAL",
                "MAX",
                "AVG/CALL",
                "CALLS",
                "TIME",
            ],
            &rows,
            &[true, false, false, false, true, true, true, true, true],
        ));
    }

    if !snap.idle_wait.is_empty() {
        lines.push(String::new());
        lines.push("Idle long-poll wait — client-requested sleep, not node work".to_string());
        let rows: Vec<Vec<String>> = snap
            .idle_wait
            .iter()
            .take(10)
            .enumerate()
            .map(|(i, a)| {
                vec![
                    (i + 1).to_string(),
                    truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                    a.kind.as_str().to_string(),
                    human_count(a.count),
                    human_duration_ms(a.sum_ms),
                    human_duration_ms(a.avg_ms()),
                    human_duration_ms(a.max_ms),
                ]
            })
            .collect();
        lines.extend(aligned_table(
            &["#", "CLIENT", "KIND", "CALLS", "WAIT", "AVG", "MAX"],
            &rows,
            &[true, false, false, true, true, true, true],
        ));
    }

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
                    s.cold_shard_loads
                        .map_or_else(|| "-".to_string(), human_count),
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
                "LOADS",
            ],
            &rows,
            &[true, true, false, false, false, true, true, true, true],
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
            if s.partition_read_rejections != 0 {
                request.push(format!(
                    "partition_read_rejections={}",
                    s.partition_read_rejections
                ));
            }
            if s.all_group_walks != 0 {
                request.push(format!("all_group_walks={}", s.all_group_walks));
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
                let molecules =
                    molecule_work_detail(s.molecules_persisted, s.molecule_store_commits);
                if !molecules.is_empty() {
                    phases.push(molecules.trim().to_string());
                }
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

    // LAST on purpose: `lastdb status` prints only the first few of these
    // lines, so a new table above the app/verb block would silently push
    // the offender summary out of `status`.
    lines.extend(phase_table_lines(&snap.top_by_total_ms, labels));

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

/// The "Request phases" table shared by the live snapshot and the durable
/// rollup: for each (client, kind, schema) key whose requests reported a
/// per-phase breakdown, where inside the node that key's time went.
///
/// Zero-hide on both axes, per the cold-load precedent
/// (`cold_load_table_is_absent_when_nothing_reports_loads`): the table is
/// ABSENT when no key reports phases, and within a row zero phases are
/// omitted ([`PhaseTimings::detail`]). Flush in particular is ~0 by design
/// on default builds (background flusher), so a printed `flush=0us` would
/// read as broken instrumentation rather than a measurement.
///
/// The PHASED column uses `<reported>/<total>` as its honest denominator.
/// Phase coverage is partial while only the mutation path reports, so a
/// per-request average over `count` would dilute.
pub(super) fn phase_table_lines(aggregates: &[OpAggregate], labels: &SchemaLabels) -> Vec<String> {
    let mut phased: Vec<&OpAggregate> = aggregates
        .iter()
        .filter(|a| !a.phase_sums.is_empty())
        .collect();
    if phased.is_empty() {
        return Vec::new();
    }
    // `disjoint_us`, not `total_us`: the latter sums the `hydrate` parent
    // beside the four children that partition it, which ranked every query
    // row at ~2x its real phase cost and every mutation row honestly. That
    // put query keys above mutation keys this command's own `Top by total
    // time` table placed below them.
    phased.sort_by(|a, b| {
        b.phase_sums
            .disjoint_us()
            .cmp(&a.phase_sums.disjoint_us())
            .then_with(|| b.phase_count.cmp(&a.phase_count))
            .then_with(|| a.client.cmp(&b.client))
    });
    let selected: Vec<&OpAggregate> = phased.into_iter().take(10).collect();
    let rows: Vec<Vec<String>> = selected
        .iter()
        .enumerate()
        .map(|(i, a)| {
            // Closes the accounting by construction: every phased request's
            // wall clock is inside a named phase or this remainder.
            let residual = a.unphased_us().map_or_else(
                || "-".to_string(),
                |us| error_cell(&residual_detail(us, a.phased_sum_ms.saturating_mul(1_000))),
            );
            vec![
                (i + 1).to_string(),
                truncate_cell(&a.client, OPS_CLIENT_CELL_WIDTH),
                a.kind.as_str().to_string(),
                identity_cell(a.schema.as_deref(), a.route.as_deref(), labels),
                format!("{}/{}", human_count(a.phase_count), human_count(a.count)),
                if a.phased_sum_ms == 0 {
                    "-".to_string()
                } else {
                    human_duration_ms(a.phased_sum_ms)
                },
                residual,
            ]
        })
        .collect();
    // The parent note is not decoration: `hydrate` renders immediately beside
    // the four children that partition it and looks like one more sibling, so
    // a reader adding the printed phases up by hand reproduces exactly the
    // double-count the ranking key was fixed to drop.
    let mut lines = vec![
        String::new(),
        "Request phases — per-key sums; zero phases omitted; ranked by phase time counted once"
            .to_string(),
        "  hydrate is the PARENT of hydrate_atoms/format/sort/filter — do not add it to them"
            .to_string(),
    ];
    lines.extend(aligned_table(
        &[
            "#",
            "CLIENT",
            "KIND",
            "SCHEMA / ROUTE",
            "PHASED",
            "WALL",
            "REMAINDER",
        ],
        &rows,
        &[true, false, false, false, true, true, true],
    ));
    lines.push("  Phase details:".to_string());
    for (i, a) in selected.iter().enumerate() {
        let mut tokens = phase_tokens(a.phase_sums);
        let molecules =
            molecule_work_detail(a.sum_molecules_persisted, a.sum_molecule_store_commits);
        if !molecules.is_empty() {
            tokens.push(molecules.trim().to_string());
        }
        let resident = resident_commit_detail(a.sum_resident_operations, a.sum_resident_commits);
        if !resident.is_empty() {
            tokens.push(resident.trim().to_string());
        }
        lines.extend(wrap_detail(&format!("    {}. ", i + 1), tokens));
    }
    lines
}

/// Render the molecule-persist work counts that go with `persist_molecules_us`.
///
/// ` molecules=<persisted>/<commits> (<n> per commit)`. Absent when nothing was
/// counted, so every non-mutating key's row — and every row served by a daemon
/// predating the counters — stays byte-identical.
///
/// The ratio is the readable part, and it is the one an operator can act on:
/// **1.0 molecules per commit is the per-molecule store path**, F per commit is
/// the shared batch commit, and a value in between counts molecules the batch
/// refused (no durable header yet, or a full order snapshot rather than an
/// append tail). Two decimals because the interesting differences are small
/// integers — 1.00 vs 24.00 — and a truncating integer ratio would render
/// "23 of 24 molecules batched" as the same `1` as "none batched".
pub(super) fn molecule_work_detail(molecules: u64, commits: u64) -> String {
    if molecules == 0 && commits == 0 {
        return String::new();
    }
    // A commit count of zero with molecules counted cannot happen from the
    // recording sites (each commit site increments before it awaits), but
    // rendering `inf` on a hand-built or truncated aggregate is worse than
    // rendering the raw pair.
    if commits == 0 {
        return format!(" molecules={molecules}/0");
    }
    let per_commit = molecules as f64 / commits as f64;
    format!(" molecules={molecules}/{commits} ({per_commit:.2} per commit)")
}

/// `resident=<operations>/<commits>` — the ratio the one-resident-commit
/// design is defined by.
///
/// Rendered next to `molecules=` and read the same way: the numerator is work
/// asked for, the denominator is commits issued. `9/1` is one atomic batch,
/// `9/9` is the serial path it replaces.
pub(super) fn resident_commit_detail(operations: u64, commits: u64) -> String {
    if operations == 0 && commits == 0 {
        return String::new();
    }
    // A commit count of zero with operations counted cannot happen from the
    // recording sites, but rendering `inf` on a truncated aggregate is worse
    // than rendering the raw pair.
    if commits == 0 {
        return format!(" resident={operations}/0");
    }
    let per_commit = operations as f64 / commits as f64;
    format!(" resident={operations}/{commits} ({per_commit:.2} per commit)")
}
// lint:file-size-ok moved verbatim from request_telemetry.rs; cohesive unit, split further in a later pass
