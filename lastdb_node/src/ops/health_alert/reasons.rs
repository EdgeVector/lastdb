//! Per-section degradation reasons for `probe_degradation`, read from one
//! `/api/status` payload. Each function passes the node's own verdict through.

use super::governor_arm::stale_purge_failed_reason;

/// `memory_budget` reasons: runtime degraded, over limit, refused lane
/// entries, slow request, stale purge failure.
pub(super) fn memory_budget_reasons(mem: &serde_json::Value, warn_ms: u64) -> Vec<String> {
    let mut reasons = Vec::new();
    if mem
        .get("runtime_degraded")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        reasons.push("memory_budget.runtime_degraded=true".to_string());
    }
    if mem
        .get("footprint_over_limit")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        reasons.push("memory_budget.footprint_over_limit=true".to_string());
    }
    if let Some(refused) = mem
        .get("deferred_lane_refuse_entries")
        .and_then(serde_json::Value::as_u64)
    {
        if refused > 0 {
            reasons.push(format!("deferred_lane_refuse_entries={refused}"));
        }
    }
    if let Some(slowest) = mem
        .get("slowest_request_ms")
        .and_then(serde_json::Value::as_u64)
    {
        if slowest >= warn_ms {
            reasons.push(format!("slowest_request_ms={slowest} (budget {warn_ms})"));
        }
    }
    if let Some(reason) = stale_purge_failed_reason(mem) {
        reasons.push(reason);
    }
    reasons
}

/// `durability.degraded=true` with the node's own reason list.
pub(super) fn durability_reason(status: &serde_json::Value) -> Option<String> {
    // Durability carries its own verdict AND its own reason list. Pass them
    // through rather than restating them: a 3.5-day-old backup against a 24 h
    // threshold is the node's finding, not this checker's.
    if let Some(dur) = status.get("durability") {
        if dur.get("degraded").and_then(serde_json::Value::as_bool) == Some(true) {
            let why = dur
                .get("reasons")
                .and_then(|v| v.as_array())
                .map(|rs| {
                    rs.iter()
                        .filter_map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .filter(|j| !j.is_empty())
                .unwrap_or_else(|| "no reason given".to_string());
            return Some(format!("durability.degraded=true ({why})"));
        }
    }
    None
}

/// `sync.sync_degraded=true` with the node's own reasons and recovery point.
pub(super) fn sync_reason(status: &serde_json::Value) -> Option<String> {
    // Sync carries the node's OWN aggregated verdict plus its own reason list,
    // exactly like durability, and it is a DIFFERENT exposure: durability asks
    // "how old is the last committed backup manifest", sync asks "has the
    // mutation log published recently". The two are independent paths on
    // independent schedules, so a home with a fresh backup and a mutation log
    // that has not published for hours reads `durability.degraded=false` and
    // was reported healthy here. Measured on the primary 2026-10-05T14:4xZ:
    // `sync_degraded=true`, `degraded_reasons=["mutation_log_lag"]`,
    // `mutation_log_recovery_point_age_secs=58021` — a 16.1 h recovery point
    // that `lastdb status` printed on its `Sync:` line while this probe,
    // reading the same payload, said nothing about it.
    //
    // `sync_degraded` is false when sync is switched OFF (the case
    // `crate::backup_durability` exists to cover), so passing it through does
    // not fire on a deliberately disabled engine.
    if let Some(sync) = status.get("sync") {
        if sync
            .get("sync_degraded")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            let why = sync
                .get("degraded_reasons")
                .and_then(|v| v.as_array())
                .map(|rs| {
                    rs.iter()
                        .filter_map(|r| r.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .filter(|j| !j.is_empty())
                .unwrap_or_else(|| "no reason given".to_string());
            // The recovery point is the number an operator acts on, and it is
            // the node's own field, so it rides along as context rather than
            // as a second judgement by this checker.
            let rpo = sync
                .get("mutation_log_recovery_point_age_secs")
                .and_then(serde_json::Value::as_u64)
                .map(|secs| format!("; rpo_secs={secs}"))
                .unwrap_or_default();
            return Some(format!("sync.sync_degraded=true ({why}){rpo}"));
        }
    }
    None
}

/// Reads served short because a tip pointed at a missing atom.
pub(super) fn integrity_reason(status: &serde_json::Value) -> Option<String> {
    // Read integrity means callers were already served SHORT: a tip pointed at
    // a missing atom, so an affected partition returns 200 with fewer rows than
    // its index claims. This mirrors `integrity_status_line`'s predicate in
    // `self_metrics` exactly -- DEGRADED iff a MEASURED skip count is non-zero
    // -- so the alarm and the `lastdb status` line cannot disagree. An absent
    // or unmeasurable count adds no reason here; whole-payload absence is
    // already `Degradation::Unknown` above.
    if let Some(integrity) = status.get("integrity") {
        if let Some(skips) = integrity
            .get("unresolved_atom_skips")
            .and_then(serde_json::Value::as_u64)
        {
            if skips > 0 {
                let rows = integrity
                    .get("unresolved_atom_rows")
                    .and_then(serde_json::Value::as_u64)
                    .map_or_else(|| "unknown".to_string(), |r| r.to_string());
                return Some(format!(
                    "integrity.unresolved_atom_rows={rows} ({skips} read(s) served short)"
                ));
            }
        }
    }
    None
}
