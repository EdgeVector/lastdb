//! Applying and previewing a manifest retention at declare time.
//!
//! A TTL purge runs through the catalog schema the declare resolved to. It
//! erases a record from every molecule that schema names, so it reaches every
//! schema bound to it. Before any policy is written the declare checks, in
//! order:
//!
//! 1. The catalog schema belongs to this app and no other schema is bound to
//!    it (`shared_with`). Otherwise the TTL is refused.
//! 2. A TTL that would delete records that already exist needs an explicit
//!    confirmation: a new TTL on a Range or HashRange schema that holds data,
//!    or a TTL shorter than the manifest TTL running.
//!
//! Every change to a policy leaves a durable audit row.

use super::retention::validate_declare_retention;
use super::retention_assess::{assess, Assessment, NEEDS_ONE_SCHEMA};
use super::retention_unmanaged::unmanaged_report;
use super::*;
use crate::retention_audit::{self, RetentionAuditEvent};
use fold_db::schema::{ManifestRetentionStatus, RetentionSource, SchemaRetentionPolicy};
use serde_json::json;

/// The catalog schema both installed names share, when the resolution names one.
pub(crate) fn resolution_anchor(resolution: &DirectDeclareResolution) -> Option<&str> {
    match resolution {
        DirectDeclareResolution::Register { catalog_hash, .. }
        | DirectDeclareResolution::Reuse { catalog_hash, .. } => Some(catalog_hash.as_str()),
        DirectDeclareResolution::Compose { .. } => None,
    }
}

/// Run after the declare bound the schema, before it returns: make the policy
/// follow the schema to its catalog schema, then apply the declared TTL, or
/// report a running manifest TTL the manifest no longer declares.
pub(crate) async fn finish_declare_retention(
    host: &Host,
    req: &UdsRequest,
    ctx: &AccessContext,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution_anchor(resolution);
    let moved_from = match anchor {
        Some(anchor) => follow_schema(host, req, ctx, declare, anchor).await?,
        None => None,
    };
    let Some(retention) = declare.retention else {
        return Ok(unmanaged_report(host, declare, anchor, moved_from.as_deref()).await);
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let anchor = anchor.ok_or_else(|| NEEDS_ONE_SCHEMA.to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
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
    let moved_from = moved_from.or_else(|| outcome.moved_from.clone());
    let mut notes = apply_notes(&assessment, &outcome);
    if let Some(from) = &moved_from {
        notes.insert(
            0,
            format!("the retention policy followed the schema from {from} to {anchor}"),
        );
    }
    let mut report = assessment.report(binding, anchor, retention.confirmed());
    report.insert("status".into(), json!(outcome.status));
    report.insert("effective".into(), json!(outcome.effective));
    report.insert("moved_from".into(), json!(moved_from));
    report.insert("notes".into(), json!(notes));
    Ok(Some(Value::Object(report)))
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

fn apply_notes(
    assessment: &Assessment,
    outcome: &fold_db::schema::ManifestRetentionOutcome,
) -> Vec<String> {
    let ttl = assessment.ttl_seconds;
    let mut notes = Vec::new();
    match outcome.status {
        ManifestRetentionStatus::Applied => {
            notes.push(format!("a new manifest TTL of {ttl}s is in force"));
        }
        ManifestRetentionStatus::Updated => notes.push(format!(
            "the manifest TTL changed from {}s to {ttl}s ({})",
            assessment.previous_ttl_seconds().unwrap_or(0),
            assessment.change()
        )),
        ManifestRetentionStatus::Unchanged => {}
        ManifestRetentionStatus::OperatorOverride => notes.push(format!(
            "an operator policy of {}s is in force and wins; the manifest TTL of {ttl}s is kept \
             beside it",
            outcome.effective.ttl_seconds
        )),
    }
    notes
}

/// Move the policy with the schema when the manifest name is bound to a new
/// catalog schema (the app added a field). Writes an audit row for the move.
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
    let previous = store
        .get_schema_retention_policy(binding)
        .await
        .ok()
        .flatten();
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
    anchor: Option<&str>,
) -> Result<Option<Value>, String> {
    let Some(retention) = declare.retention else {
        return Ok(unmanaged_report(host, declare, anchor, None).await);
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let anchor = anchor.ok_or_else(|| NEEDS_ONE_SCHEMA.to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    let current: Option<SchemaRetentionPolicy> = host
        .db
        .db_ops()
        .schemas()
        .get_schema_retention_policy(binding)
        .await
        .ok()
        .flatten();
    let operator_wins = current
        .as_ref()
        .is_some_and(|policy| policy.source == RetentionSource::Operator);
    let blocked = assessment.refusal(declare, anchor, retention.confirmed());
    let mut report = assessment.report(binding, anchor, retention.confirmed());
    report.insert(
        "would_apply".into(),
        json!(!operator_wins && blocked.is_none()),
    );
    report.insert("current".into(), json!(current));
    report.insert(
        "requires_confirmation".into(),
        json!(assessment.needs_confirmation()),
    );
    report.insert("blocked".into(), json!(blocked));
    Ok(Some(Value::Object(report)))
}
