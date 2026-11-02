//! Per-section degradation reasons for `probe_degradation`, read from one
//! `/api/status` payload. Each function passes the node's own verdict through.

use super::governor_arm::stale_purge_failed_reason;

/// `memory_budget` reasons: runtime degraded, over limit, slow request,
/// stale purge failure.
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
    // Pass through observed V2 job failures; unmeasured freshness stays Unknown.
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
