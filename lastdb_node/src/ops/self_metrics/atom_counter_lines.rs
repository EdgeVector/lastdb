use super::*;

/// `lastdb status` line for the atom counter planner.
///
/// The totals count the events since this process started, and a restart
/// resets them: a home with `suspect` counters shows zeros after a restart. The
/// line says so. The durable count of `suspect` counters comes from
/// `lastdb db verify-refs` (one run) or `lastdb db inventory`; the status does
/// not scan for it. A counter fault (underflow, counter absent, dangling
/// removal) keeps the body and raises `suspect`; the recent faults name the
/// atoms. Always emits one line, so a node that has written nothing and a
/// daemon that predates the field read differently.
pub fn atom_counter_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.atom_counters.as_ref() else {
        return vec!["Atom counters: unsupported by daemon (no planner report)".to_string()];
    };
    let mut head = format!(
        "Atom counters: since this process started (a restart sets them to zero): bodies_created={} bodies_deleted={} underflows={} counters_absent={} dangling_removals={} suspect_marked={} molecule_faults={}",
        report.bodies_created,
        report.bodies_deleted,
        report.underflows,
        report.counters_absent,
        report.dangling_removals,
        report.suspect_marked,
        report.molecule_faults,
    );
    if !report.recent_faults.is_empty() {
        head.push_str(&format!(
            " recent_faults=[{}]",
            report.recent_faults.join(", ")
        ));
    }
    head.push_str(
        " (the counters that are suspect now: lastdb db verify-refs or lastdb db inventory)",
    );
    vec![head, verify_refs_line(report), molecule_drain_line(report)]
}

/// The `Verify refs` line: the last complete run of `lastdb db verify-refs`, and
/// a run in progress. The verifier keeps its result in memory, so a restart
/// clears the line.
fn verify_refs_line(report: &fold_db::db_operations::AtomCounterReport) -> String {
    let Some(summary) = report.verify_refs.as_ref() else {
        return "Verify refs: unsupported by daemon (no verifier summary)".to_string();
    };
    let mut line = match summary.last.as_ref() {
        None => "Verify refs: not run since the daemon started (lastdb db verify-refs)".to_string(),
        Some(last) => {
            let m = &last.mismatches;
            format!(
                "Verify refs: last run {} scope={} complete pages={} tips={} faults={} suspect_atoms={} candidates={} (below_tips={} above_tips={} body_without_counter={} counter_without_body={} zero={} tip_without_body={} queue_rows={} unsealed_rows={}){}{}",
                last.finished_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                last.scope.as_deref().unwrap_or("personal"),
                last.pages,
                last.tips_read,
                last.mismatches_total,
                last.suspect_atoms,
                m.unconfirmed_count_mismatch,
                m.counter_below_tips,
                m.counter_above_tips,
                m.body_without_counter,
                m.counter_without_body,
                m.zero_counter,
                m.tip_without_body + m.record_without_body,
                m.record_without_queue_entry + m.queue_entry_without_directory,
                m.unsealed_rows,
                if last.tally_complete {
                    ""
                } else {
                    " INCOMPLETE (counters were not compared with the totals)"
                },
                if last.tips_read == 0 {
                    " WARNING 0 tips read in this scope: check the scope"
                } else {
                    ""
                },
            )
        }
    };
    if let Some(running) = summary.running.as_ref() {
        line.push_str(&format!(
            "; run in progress phase={:?} pages={}",
            running.phase, running.pages
        ));
    }
    line
}

/// The `Molecule drain` line: the candidates the drain holds, the molecules it
/// removed since the daemon started, and the molecules it refused. A refused
/// molecule is a fault of a counter or a record; the drain never removes it.
fn molecule_drain_line(report: &fold_db::db_operations::AtomCounterReport) -> String {
    let Some(drain) = report.molecule_drain.as_ref() else {
        return "Molecule drain: unsupported by daemon (no drain report)".to_string();
    };
    let task = if drain.interval_ms == 0 {
        "task off".to_string()
    } else {
        format!("task every {} ms", drain.interval_ms)
    };
    let mut line = format!(
        "Molecule drain: {task}; pending {} (waiting for kept records {}, refused {}); drained {} molecules since the daemon started ({} tips erased, {} atom bodies deleted, {} kept conflict records released, {} other rows, {} batches); candidates dropped because the molecule was bound again {}",
        drain.pending + drain.parked,
        drain.waiting_records,
        drain.refused,
        drain.drained,
        drain.tips_erased,
        drain.atoms_deleted,
        drain.records_released,
        drain.rows_removed,
        drain.batches,
        drain.rebound,
    );
    if !drain.refused_sample.is_empty() {
        let sample: Vec<String> = drain
            .refused_sample
            .iter()
            .map(|item| {
                let scope = if item.scope.is_empty() {
                    "personal"
                } else {
                    item.scope.as_str()
                };
                format!("{}@{scope}:{}", item.molecule, item.reason)
            })
            .collect();
        line.push_str(&format!("; refused=[{}]", sample.join(", ")));
    }
    if drain.consecutive_failures > 0 {
        line.push_str(&format!(
            "; failed {} wakes in a row: {}",
            drain.consecutive_failures,
            drain.last_error.as_deref().unwrap_or("unknown")
        ));
    }
    line
}

/// `lastdb status` line for the blob counters and the dwell queue.
///
/// The totals are process-lifetime. `queue_depth` and `queue_buckets` are the
/// size of the dwell queue, counted by its bucket directory with bounded reads;
/// a `+` says the count stopped at its cap. `next_due_in` is the earliest time
/// at which the oldest bucket can hold a due entry. A blob counter fault
/// (underflow, dangling removal) keeps the blob and raises `suspect`; the recent
/// faults name the blobs by reference. A pass that failed shows its error and
/// the time to the next try.
pub fn blob_counter_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.blob_counters.as_ref() else {
        return vec!["Blob counters: unsupported by daemon (no blob counter report)".to_string()];
    };
    let now_ms = fold_db::clock::unix_millis();
    let until = |at: Option<u64>| {
        at.map_or_else(
            || "none".to_string(),
            |at| format!("{}s", at.saturating_sub(now_ms) / 1000),
        )
    };
    let since = |at: Option<u64>| {
        at.map_or_else(
            || "none".to_string(),
            |at| format!("{}s", now_ms.saturating_sub(at) / 1000),
        )
    };
    let mut head = format!(
        "Blob counters: created={} uploads_registered={} zero_reached={} dwell_cancelled={} blobs_deleted={} stale_queue_entries={} underflows={} dangling_removals={} late_references={} unreadable_counters={}; queue_depth={}{} queue_buckets={} next_due_in={} last_pass_ago={} dwell={}s passes={}; totals are process-lifetime (reset on daemon restart)",
        report.counters_created,
        report.uploads_registered,
        report.zero_reached,
        report.dwell_cancelled,
        report.blobs_deleted,
        report.stale_queue_entries,
        report.underflows,
        report.dangling_removals,
        report.late_references,
        report.unreadable_counters,
        report.queue_depth,
        if report.queue_depth_capped { "+" } else { "" },
        report
            .queue_buckets
            .map_or_else(|| "unknown".to_string(), |buckets| buckets.to_string()),
        until(report.next_due_unix_ms),
        since(report.last_pass_unix_ms),
        report.dwell_secs,
        report.dwell_passes,
    );
    if report.net_sweeps > 0 {
        head.push_str(&format!(
            " net_sweeps={} buckets_repaired={}",
            report.net_sweeps, report.buckets_repaired
        ));
    }
    if report.pass_failures > 0 {
        head.push_str(&format!(
            " pass_failures={} last_error={:?} retry_in={}s",
            report.pass_failures,
            report.last_error.as_deref().unwrap_or("none"),
            report.retry_in_secs.unwrap_or(0),
        ));
    }
    if !report.recent_faults.is_empty() {
        head.push_str(&format!(
            " recent_faults=[{}]",
            report.recent_faults.join(", ")
        ));
    }
    vec![head]
}
