//! Shadow summaries, disagreement classification and time helpers.

use super::*;

pub(super) fn local_shadow_summary(outcome: &LocalEvaluateOutcome) -> LocalShadowSummary {
    let matched_schema_id = outcome
        .output
        .use_existing
        .as_ref()
        .map(|u| u.schema_id.clone());
    let mut component_schema_ids: Vec<String> = outcome
        .output
        .use_components
        .iter()
        .map(|c| c.schema_id.clone())
        .collect();
    component_schema_ids.sort();
    component_schema_ids.dedup();
    let route = match &outcome.route {
        ResolverPackResolutionRoute::UseLocal => Some("use_local".to_string()),
        ResolverPackResolutionRoute::LiveServiceFallback { reason } => {
            Some(format!("live_fallback:{reason}"))
        }
    };
    LocalShadowSummary {
        decision: outcome.output.decision,
        confidence: outcome.output.confidence,
        matched_schema_id,
        component_schema_ids,
        route,
    }
}

/// Classify disagreement between an optional local reuse summary and live.
pub fn classify_disagreement(
    local: Option<&LocalShadowSummary>,
    live: &SchemaResolveResult,
) -> DisagreementClass {
    let Some(local) = local else {
        return match live.outcome {
            SchemaResolveOutcome::Reuse | SchemaResolveOutcome::CandidateEquivalent => {
                DisagreementClass::MissedReuse
            }
            _ => DisagreementClass::None,
        };
    };

    let local_reuses = matches!(
        local.decision,
        ResolverDecision::UseExisting | ResolverDecision::UseComponents
    ) && local.route.as_deref().is_some_and(|r| r == "use_local");

    let live_reuses = matches!(
        live.outcome,
        SchemaResolveOutcome::Reuse | SchemaResolveOutcome::CandidateEquivalent
    );

    if local_reuses && !live_reuses {
        return DisagreementClass::UnsafeLocalReuse;
    }
    if !local_reuses && live_reuses {
        return DisagreementClass::MissedReuse;
    }
    if local_reuses && live_reuses {
        let local_ids = local_reuse_ids(local);
        let live_ids = live_reuse_ids(live);
        if local_ids != live_ids {
            return DisagreementClass::EquivalentDifferentPlan;
        }
        return DisagreementClass::None;
    }
    if local.decision == ResolverDecision::Ambiguous
        || local.decision == ResolverDecision::NeedsLiveSchemaService
    {
        return DisagreementClass::None;
    }
    DisagreementClass::Other
}

pub(super) fn local_reuse_ids(local: &LocalShadowSummary) -> BTreeMap<&'static str, Vec<String>> {
    let mut m = BTreeMap::new();
    if let Some(id) = &local.matched_schema_id {
        m.insert("matched", vec![id.clone()]);
    }
    if !local.component_schema_ids.is_empty() {
        m.insert("components", local.component_schema_ids.clone());
    }
    m
}

pub(super) fn live_reuse_ids(live: &SchemaResolveResult) -> BTreeMap<&'static str, Vec<String>> {
    let mut m = BTreeMap::new();
    if let Some(id) = &live.matched_shared_schema_hash {
        m.insert("matched", vec![id.clone()]);
    }
    if !live.candidate_shared_schema_hashes.is_empty() {
        let mut c = live.candidate_shared_schema_hashes.clone();
        c.sort();
        m.insert("components", c);
    }
    m
}

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
