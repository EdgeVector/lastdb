//! Stable proposal matching and attachment timestamps.

use super::*;

/// Pick a live result for a facade proposal (proposal_id first, then name).
pub fn match_live_result<'a>(
    live: &'a SchemaResolveResponse,
    proposal_id: &str,
    descriptive_name: &str,
    claimed_keys: &mut HashSet<String>,
) -> Option<&'a SchemaResolveResult> {
    if let Some(r) = live.results.get(proposal_id) {
        claimed_keys.insert(proposal_id.to_string());
        return Some(r);
    }
    if let Some(r) = live.results.get(descriptive_name) {
        // Allow one claim per descriptive_name key when duplicates share a name.
        if claimed_keys.insert(descriptive_name.to_string()) {
            return Some(r);
        }
    }
    None
}

pub(super) fn novel_result() -> SchemaResolveResult {
    SchemaResolveResult {
        outcome: SchemaResolveOutcome::Novel,
        matched_shared_schema_hash: None,
        r#match: None,
        candidates: Vec::new(),
        candidate_shared_schema_hashes: Vec::new(),
        confidence: None,
    }
}

pub(super) fn now_rfc3339() -> String {
    let secs = schema_types::clock::unix_secs();
    // Second precision is enough for attachment audit stamps.
    // Format as RFC3339 UTC without pulling chrono into the client crate.
    let days = secs / 86_400;
    let day_secs = secs % 86_400;
    let hour = day_secs / 3600;
    let min = (day_secs % 3600) / 60;
    let sec = day_secs % 60;
    // Civil date from Unix day count (proleptic Gregorian).
    let (y, m, d) = unix_days_to_ymd(days as i64);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
}

pub(super) fn unix_days_to_ymd(mut z: i64) -> (i32, u32, u32) {
    // Algorithm from civil_from_days (Howard Hinnant).
    z += 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}
