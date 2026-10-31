//! A schema declared without a `retention`: the guards that still apply, and
//! what the declare reports about a TTL that runs on it anyway.

use super::retention::{read_policy, validate_declare_retention};
use super::retention_assess::assess;
use super::*;
use fold_db::schema::{RetentionPolicyInReach, RetentionReach, SchemaRetentionPolicy};
use serde_json::json;

/// The policies whose sweep would delete records of `binding` if it were bound
/// to `anchor`: the same closure a TTL declare is checked against.
pub(super) async fn policies_in_reach(
    host: &Host,
    binding: &str,
    anchor: &str,
    predecessors: &[String],
) -> Result<RetentionReach, String> {
    host.db
        .db_ops()
        .schemas()
        .retention_policies_in_reach(binding, anchor, predecessors)
        .await
        .map_err(|e| e.to_string())
}

fn holder_text(hit: &RetentionPolicyInReach) -> String {
    let declared = if hit.declared_by.is_empty() {
        "set by an operator".to_string()
    } else {
        format!("declared by [{}]", hit.declared_by.join(", "))
    };
    format!(
        "{} ({}s, {}; {declared})",
        hit.anchor,
        hit.policy.ttl_seconds,
        hit.policy.source.as_str()
    )
}

/// Why a schema declared without a retention must not be bound to `anchor`.
pub(super) fn bind_refusal(binding: &str, anchor: &str, hits: &[RetentionPolicyInReach]) -> String {
    let holders: Vec<String> = hits.iter().map(holder_text).collect();
    let clear = hits.first().map_or("", |hit| hit.anchor.as_str());
    format!(
        "{binding} cannot be bound to the catalog schema {anchor}: the TTL sweep reaches it \
         through {}. Binding {binding} would put its records under that TTL, and the sweep \
         would delete them for good. Give {binding} its own shape, or clear the TTL \
         (`lastdb schema-retention clear --schema {clear}`)",
        holders.join("; ")
    )
}

/// Refuse a manifest TTL before the declare binds the schema. Writes nothing.
///
/// A schema declared WITHOUT a retention is refused when it would JOIN a
/// catalog schema that a TTL reaches: a TTL on the catalog schema, on a schema
/// it was expanded from, or on any schema bound to either. The TTL would delete
/// its records too.
pub(crate) async fn precheck_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<(), String> {
    let Some(retention) = declare.retention else {
        let binding = declare.local_schema_id.as_str();
        let reach = policies_in_reach(host, binding, anchor, &[]).await?;
        return if reach.policies.is_empty() {
            Ok(())
        } else {
            Err(bind_refusal(binding, anchor, &reach.policies))
        };
    };
    let Some((ttl_seconds, layout)) = validate_declare_retention(declare)? else {
        return Ok(());
    };
    let assessment = assess(host, declare, anchor, ttl_seconds, layout, &[]).await?;
    match assessment.refusal(declare, anchor, retention.confirmed()) {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

/// What a declare reports when the manifest entry has no `retention`: a
/// manifest TTL that still runs on the schema, a TTL that reaches the schema
/// through its catalog schema, or a policy that followed the schema. `None`
/// when none of those holds. A failed read is an error.
pub(super) async fn unmanaged_report(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: Option<&str>,
    moved_from: Option<&str>,
    predecessors: &[String],
) -> Result<Option<Value>, String> {
    let store = host.db.db_ops().schemas();
    let binding = declare.local_schema_id.as_str();
    let running = read_policy(host, binding).await?;
    let own_anchor = store
        .retention_anchor(binding)
        .await
        .map_err(|e| e.to_string())?;
    let mut notes = Vec::new();
    let mut warnings = Vec::new();
    let mut status = "moved";
    let mut ttl_seconds = None;
    let mut blocked: Option<String> = None;
    if let Some(from) = moved_from {
        notes.push(format!(
            "the retention policy followed the schema from {from} to {}",
            anchor.unwrap_or(&own_anchor)
        ));
    }
    if let Some(manifest_ttl) = running
        .as_ref()
        .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
    {
        status = "manifest_policy_still_running";
        ttl_seconds = Some(manifest_ttl);
        warnings.push(format!(
            "this manifest no longer declares a retention for {binding}, but a manifest TTL of \
             {manifest_ttl}s is still running. Removing the key does not clear the policy. \
             Run `lastdb schema-retention clear --schema {binding}` to remove it"
        ));
    } else if let (true, Some(anchor)) = (own_anchor == binding, anchor) {
        let reach = policies_in_reach(host, binding, anchor, predecessors).await?;
        if let Some(first) = reach.policies.first() {
            status = "shares_retained_schema";
            ttl_seconds = Some(first.policy.ttl_seconds);
            blocked = Some(bind_refusal(binding, anchor, &reach.policies));
        }
    }
    if notes.is_empty() && warnings.is_empty() && blocked.is_none() {
        return Ok(None);
    }
    Ok(Some(json!({
        "declared": false,
        "status": status,
        "ttl_seconds": ttl_seconds,
        "anchor": anchor.unwrap_or(&own_anchor),
        "notes": notes,
        "warnings": warnings,
        "blocked": blocked,
    })))
}
