//! A schema declared without a `retention`: pre-declare TTL validation and
//! reporting when a manifest TTL still runs on the schema.

use super::retention::{read_policy, validate_declare_retention};
use super::retention_assess::assess;
use super::*;
use fold_db::schema::SchemaRetentionPolicy;
use serde_json::json;

/// Refuse an invalid or unconfirmed manifest TTL before the declare loads the schema.
pub(crate) async fn precheck_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<(), String> {
    let Some(retention) = declare.retention else {
        return Ok(());
    };
    let Some((ttl_seconds, layout)) = validate_declare_retention(declare)? else {
        return Ok(());
    };
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    match assessment.refusal(declare, retention.confirmed()) {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

/// Report when a manifest entry omits `retention` while a manifest TTL is still running.
pub(super) async fn unmanaged_report(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<Option<Value>, String> {
    let binding = declare.local_schema_id.as_str();
    let running = read_policy(host, anchor).await?;
    let Some(manifest_ttl) = running
        .as_ref()
        .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
    else {
        return Ok(None);
    };
    let warning = format!(
        "this manifest no longer declares a retention for {binding}, but a manifest TTL of \
         {manifest_ttl}s is still running. Removing the key does not clear the policy. \
         Run `lastdb admin retention clear --schema {anchor}` to remove it"
    );
    Ok(Some(json!({
        "declared": false,
        "status": "manifest_policy_still_running",
        "ttl_seconds": manifest_ttl,
        "anchor": anchor,
        "notes": [],
        "warnings": [warning],
        "blocked": null,
    })))
}
