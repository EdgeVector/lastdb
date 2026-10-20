use super::*;

/// `lastdb status` line for the atom counter planner.
///
/// The totals are process-lifetime. A counter fault (underflow, counter
/// absent, dangling removal) keeps the body and raises `suspect`; the recent
/// faults name the atoms. Always emits one line, so a node that has written
/// nothing and a daemon that predates the field read differently.
pub fn atom_counter_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    let Some(report) = snapshot.atom_counters.as_ref() else {
        return vec!["Atom counters: unsupported by daemon (no planner report)".to_string()];
    };
    let mut head = format!(
        "Atom counters: bodies_created={} bodies_deleted={} underflows={} counters_absent={} dangling_removals={}; process-lifetime totals (reset on daemon restart)",
        report.bodies_created,
        report.bodies_deleted,
        report.underflows,
        report.counters_absent,
        report.dangling_removals,
    );
    if !report.recent_faults.is_empty() {
        head.push_str(&format!(
            " recent_faults=[{}]",
            report.recent_faults.join(", ")
        ));
    }
    vec![head]
}
