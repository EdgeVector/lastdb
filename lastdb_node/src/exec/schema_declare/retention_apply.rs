//! Applying and previewing a manifest retention at declare time.

use super::retention::{read_policy, validate_declare_retention};
use super::retention_assess::assess;
use super::retention_unmanaged::unmanaged_report;
use super::*;
use fold_db::schema::SchemaRetentionPolicy;

/// Preserve the policy for an explicitly converted schema, then apply the
/// declared TTL or report a manifest TTL that still runs.
pub(crate) async fn finish_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution.runtime_schema.as_str();
    let moved_from = follow_schema(host, declare, anchor).await?;
    let Some(retention) = declare.retention else {
        let report = unmanaged_report(host, declare, Some(anchor), moved_from.as_deref()).await?;
        if let Some(blocked) = report
            .as_ref()
            .and_then(|report| report.get("blocked"))
            .and_then(Value::as_str)
        {
            return Err(format!(
                "{blocked}. {} is already bound: drop it with `lastdb schema drop --schema {}` or clear the TTL",
                declare.local_schema_id, declare.local_schema_id
            ));
        }
        return Ok(report);
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    if let Some(reason) = assessment.refusal(declare, anchor, retention.confirmed()) {
        return Err(reason);
    }
    let outcome = host
        .db
        .db_ops()
        .schemas()
        .apply_manifest_retention(
            binding,
            anchor,
            ttl_seconds,
            &declare.namespace,
            retention.confirmed(),
        )
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        target: "lastdb_node::apps_declare_schema",
        schema = %binding,
        anchor = %outcome.anchor,
        ttl_seconds,
        status = ?outcome.status,
        "applied the manifest retention"
    );
    Ok(Some(super::retention_reply::applied_report(
        &assessment,
        &outcome,
        binding,
        anchor,
        retention.confirmed(),
        moved_from,
    )))
}

async fn follow_schema(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<Option<String>, String> {
    let store = host.db.db_ops().schemas();
    let binding = declare.local_schema_id.as_str();
    let from = store
        .retention_anchor(binding)
        .await
        .map_err(|e| e.to_string())?;
    if from == binding || from == anchor {
        return Ok(None);
    }
    store
        .realign_retention_anchor(binding, anchor, &declare.namespace)
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(from))
}

/// What a read-only check reports: the policy a sync would apply, the policy in
/// force now, who shares the records, and whether the TTL needs confirmation.
pub(crate) async fn preview_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution.runtime_schema.as_str();
    let Some(retention) = declare.retention else {
        return unmanaged_report(host, declare, Some(anchor), None).await;
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    let current: Option<SchemaRetentionPolicy> = read_policy(host, binding).await?;
    let blocked = assessment.refusal(declare, anchor, retention.confirmed());
    Ok(Some(super::retention_reply::preview_report(
        &assessment,
        current.as_ref(),
        binding,
        anchor,
        retention.confirmed(),
        blocked.as_deref(),
    )))
}
