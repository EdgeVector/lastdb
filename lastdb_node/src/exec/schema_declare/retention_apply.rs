//! Applying and previewing a manifest retention at declare time.
//!
//! A TTL sweep uses the owned schema's record molecules. Before a policy
//! changes, the declaration checks these facts:
//!
//! 1. The schema belongs to this app and has no conflicting retention owner.
//! 2. A TTL that would delete records that already exist needs an explicit
//!    confirmation: a new TTL on a Range or HashRange schema that holds data,
//!    or a TTL shorter than the manifest TTL running.
//!
//! Every change to a policy leaves a durable audit row.

use super::retention::{read_policy, validate_declare_retention};
use super::retention_assess::assess;
use super::retention_unmanaged::unmanaged_report;
use super::*;
use crate::retention_audit::{self, RetentionAuditEvent};
use fold_db::schema::SchemaRetentionPolicy;

/// Preserve the policy for an explicitly converted schema, then apply the
/// declared TTL or report a manifest TTL that still runs.
pub(crate) async fn finish_declare_retention(
    host: &Host,
    req: &UdsRequest,
    ctx: &AccessContext,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution.runtime_schema.as_str();
    let moved_from = follow_schema(host, req, ctx, declare, anchor).await?;
    let Some(retention) = declare.retention else {
        let report =
            unmanaged_report(host, declare, Some(anchor), moved_from.as_deref(), &[]).await?;
        // The pre-check ran under the same lock, so a refusal here means the
        // state changed under it: fail loudly, never a 200 with `blocked`.
        if let Some(blocked) = report
            .as_ref()
            .and_then(|report| report.get("blocked"))
            .and_then(Value::as_str)
        {
            return Err(format!(
                "{blocked}. {} is already bound: drop it with `lastdb schema drop --schema {}`                  or clear the TTL",
                declare.local_schema_id, declare.local_schema_id
            ));
        }
        return Ok(report);
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout, &[]).await?;
    if let Some(reason) = assessment.refusal(declare, anchor, retention.confirmed()) {
        return Err(reason);
    }
    // The audit row comes first: if it cannot be written, nothing changes.
    let event = match &assessment.plan {
        Ok(plan) if plan.next.is_some() => {
            let mut event = audit_event(req, ctx, "manifest_apply", binding, anchor)
                .with_policies(plan.current.clone(), plan.next.clone());
            event.app_id = Some(declare.namespace.clone());
            event.confirmed_expire_existing = retention.confirmed();
            retention_audit::append(&host.home, &event).map_err(|e| {
                format!("no policy change was made: its audit row could not be written: {e}")
            })?;
            Some(event)
        }
        _ => None,
    };
    let applied = host
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
        .map_err(|e| e.to_string());
    let outcome = match applied {
        Ok(outcome) => outcome,
        Err(error) => {
            if let Some(event) = &event {
                let _ = retention_audit::append(&host.home, &event.closed(Err(&error)));
            }
            return Err(error);
        }
    };
    if let Some(event) = &event {
        retention_audit::append(&host.home, &event.closed(Ok(()))).map_err(|e| {
            format!("the policy changed, but the audit row that closes the change failed: {e}")
        })?;
    }
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

fn audit_event(
    req: &UdsRequest,
    ctx: &AccessContext,
    action: &str,
    binding: &str,
    anchor: &str,
) -> RetentionAuditEvent {
    RetentionAuditEvent::new(
        "apps_declare_schema",
        &crate::request_telemetry::client_from_request(req),
        ctx.user_id.as_str(),
        action,
        binding,
        anchor,
    )
}

/// Preserve an explicitly converted schema's retained policy association.
/// Every policy move keeps its durable audit rows.
async fn follow_schema(
    host: &Host,
    req: &UdsRequest,
    ctx: &AccessContext,
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
    let previous = read_policy(host, binding).await?;
    let mut event = audit_event(req, ctx, "anchor_move", binding, anchor)
        .with_policies(previous.clone(), previous);
    event.app_id = Some(declare.namespace.clone());
    event.moved_from = Some(from.clone());
    retention_audit::append(&host.home, &event).map_err(|e| {
        format!("no policy change was made: its audit row could not be written: {e}")
    })?;
    let moved = store
        .realign_retention_anchor(binding, anchor, &declare.namespace)
        .await
        .map_err(|e| e.to_string());
    if let Err(error) = &moved {
        let _ = retention_audit::append(&host.home, &event.closed(Err(error)));
    }
    moved?;
    retention_audit::append(&host.home, &event.closed(Ok(()))).map_err(|e| {
        format!("the policy moved, but the audit row that closes the move failed: {e}")
    })?;
    Ok(Some(from))
}

/// What a read-only check reports: the policy a sync would apply, the policy in
/// force now, who shares the records, and whether the TTL needs confirmation.
/// Writes nothing. A TTL a sync would refuse is reported as `blocked`.
pub(crate) async fn preview_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution.runtime_schema.as_str();
    let Some(retention) = declare.retention else {
        return unmanaged_report(host, declare, Some(anchor), None, &[]).await;
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout, &[]).await?;
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
