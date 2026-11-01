use super::*;

/// Queued background purge work is separate from the logical warm set.
pub(super) fn resident_status_line(r: &ResidentHealth) -> String {
    format!(
        "Background purge work: lanes depth/age_ms/fail={}/{}/{}",
        r.persist_lane_depth, r.persist_lane_oldest_age_ms, r.persist_lane_failures,
    )
}
