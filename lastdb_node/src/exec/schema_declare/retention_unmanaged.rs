//! A schema declared without a `retention`: the guards that still apply, and
//! what the declare reports about a TTL that runs on it anyway.

use super::retention::validate_declare_retention;
use super::retention_assess::assess;
use super::*;
use fold_db::schema::SchemaRetentionPolicy;
use serde_json::json;

/// The manifest names that declared the TTL on `anchor`, and the policy in
/// force there, when a name other than `binding` declared it.
pub(super) async fn retained_by_other_names(
    host: &Host,
    binding: &str,
    anchor: &str,
) -> Result<Option<(Vec<String>, SchemaRetentionPolicy)>, String> {
    let store = host.db.db_ops().schemas();
    let names: Vec<String> = store
        .retention_aliases_of(anchor)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|name| name != binding)
        .collect();
    if names.is_empty() {
        return Ok(None);
    }
    let policy = store
        .get_schema_retention_policy(anchor)
        .await
        .ok()
        .flatten();
    Ok(policy.map(|policy| (names, policy)))
}

pub(super) fn bind_refusal(
    binding: &str,
    anchor: &str,
    names: &[String],
    policy: &SchemaRetentionPolicy,
) -> String {
    format!(
        "{binding} cannot be bound to the catalog schema {anchor}: it carries a {}s TTL ({}) \
         declared by [{}]. Binding {binding} would put its records under that TTL, and the purge \
         would delete them for good. Give {binding} its own shape",
        policy.ttl_seconds,
        policy.source.as_str(),
        names.join(", ")
    )
}

/// Refuse a manifest TTL before the declare binds the schema. Writes nothing.
///
/// A schema declared WITHOUT a retention is refused when it would be bound to a
/// catalog schema that another manifest name put a TTL on: the TTL would
/// delete its records too.
pub(crate) async fn precheck_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<(), String> {
    let Some(retention) = declare.retention else {
        let binding = declare.local_schema_id.as_str();
        return match retained_by_other_names(host, binding, anchor).await? {
            Some((names, policy)) => Err(bind_refusal(binding, anchor, &names, &policy)),
            None => Ok(()),
        };
    };
    let Some((ttl_seconds, layout)) = validate_declare_retention(declare)? else {
        return Ok(());
    };
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    match assessment.refusal(declare, anchor, retention.confirmed()) {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

/// What a declare reports when the manifest entry has no `retention`: a
/// manifest TTL that still runs on the schema, a retained catalog schema the
/// schema is bound to, or a policy that followed the schema. `None` when none
/// of those holds.
pub(super) async fn unmanaged_report(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: Option<&str>,
    moved_from: Option<&str>,
) -> Option<Value> {
    let store = host.db.db_ops().schemas();
    let binding = declare.local_schema_id.as_str();
    let running = store
        .get_schema_retention_policy(binding)
        .await
        .ok()
        .flatten();
    let own_anchor = store.retention_anchor(binding).await.ok()?;
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
        if let Ok(Some((names, policy))) = retained_by_other_names(host, binding, anchor).await {
            status = "shares_retained_schema";
            ttl_seconds = Some(policy.ttl_seconds);
            blocked = Some(bind_refusal(binding, anchor, &names, &policy));
        } else if let Some(policy) = store
            .get_schema_retention_policy(anchor)
            .await
            .ok()
            .flatten()
        {
            status = "shares_retained_schema";
            ttl_seconds = Some(policy.ttl_seconds);
            warnings.push(format!(
                "{binding} is bound to the catalog schema {anchor}, which carries a {}s TTL set \
                 by an operator. The TTL purge deletes this schema's records too",
                policy.ttl_seconds
            ));
        }
    }
    if notes.is_empty() && warnings.is_empty() && blocked.is_none() {
        return None;
    }
    Some(json!({
        "declared": false,
        "status": status,
        "ttl_seconds": ttl_seconds,
        "anchor": anchor.unwrap_or(&own_anchor),
        "notes": notes,
        "warnings": warnings,
        "blocked": blocked,
    }))
}
