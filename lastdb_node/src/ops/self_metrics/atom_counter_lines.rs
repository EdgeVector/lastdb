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
        "Atom counters: since this process started (a restart sets them to zero): bodies_created={} bodies_deleted={} underflows={} counters_absent={} dangling_removals={} suspect_marked={}",
        report.bodies_created,
        report.bodies_deleted,
        report.underflows,
        report.counters_absent,
        report.dangling_removals,
        report.suspect_marked,
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
    vec![head, verify_refs_line(report)]
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
                "Verify refs: last run {} scope={} complete pages={} tips={} faults={} suspect_atoms={} candidates={} (below_tips={} above_tips={} body_without_counter={} counter_without_body={} zero={} tip_without_body={} unsealed_rows={}){}{}",
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
