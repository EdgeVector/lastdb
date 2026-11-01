use super::*;

/// One line for `lastdb status`, but only when something was actually
/// dropped. A healthy node does not carry a permanent "0 rows lost" line.
pub(super) fn integrity_status_line(i: &IntegrityHealth) -> Option<String> {
    let skips = match i.unresolved_atom_skips.value {
        Availability::Measured(0) | Availability::Unavailable(_) => return None,
        Availability::Measured(n) => n,
    };
    // Lead with damage size. Edges are typed Unit::Edges so this cannot print
    // them as row(s) (historical mislabel). Window/noun come from the gauges.
    let floor = if i.unresolved_distinct_capped {
        "+"
    } else {
        ""
    };
    let row_noun = i.unresolved_atom_rows.unit.noun();
    let edge_noun = i.unresolved_atom_distinct.unit.noun();
    let event_noun = i.unresolved_atom_skips.unit.noun();
    let window_q = i.unresolved_atom_skips.window.qualifier();
    let scope = match (
        i.unresolved_atom_rows.as_wire_u64(),
        i.unresolved_atom_distinct.as_wire_u64(),
    ) {
        (Some(rows), Some(edges)) => {
            format!("{rows}{floor} {row_noun} unreadable across {edges}{floor} {edge_noun}")
        }
        (_, Some(edges)) => {
            format!("{edges}{floor} {edge_noun} (row count unavailable from this daemon)")
        }
        _ => "damage scope unavailable from this daemon".to_string(),
    };
    // `\` continuations, so the source indentation stays in the source. Without
    // them this rendered as one ~600-character run carrying literal ten-space
    // gaps mid-sentence, in a `status` block of otherwise short labelled lines
    // — read as corrupted output, and missed entirely by a reader who did not
    // scroll to line 43. The advice inside it is load-bearing ("size a repair
    // with the dry run, not with these counts"), so it has to be readable.
    Some(format!(
        "Read integrity: DEGRADED — {scope}, {skips} {event_noun} {window_q}, \
         because their tip pointed at a missing atom. Reads on the affected \
         partitions return 200 with fewer rows than the index claims. Rows are \
         how much is broken; events are how often callers were served short. \
         Do not size a repair from these counts. Keys: grep the daemon log for \
         'Skipping unresolved atom ref'."
    ))
}

/// Queued background purge work is separate from the logical warm set.
pub(super) fn resident_status_line(r: &ResidentHealth) -> String {
    format!(
        "Background purge work: lanes depth/age_ms/fail={}/{}/{}",
        r.persist_lane_depth, r.persist_lane_oldest_age_ms, r.persist_lane_failures,
    )
}
