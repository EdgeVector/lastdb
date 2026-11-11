//! Manifest-declared retention: validation, precheck, apply, preview, and reporting.

use fold_db::schema::types::FieldValueType;
use fold_db::schema::{
    ManifestRetention, ManifestRetentionOutcome, ManifestRetentionStatus, RetentionSource,
    SchemaError, SchemaRetentionPolicy,
};
use serde_json::{json, Value};

use super::retention_assess::{assess, Assessment};
use super::*;

const RANGE_KEY_RULE: &str = "records expire by the entry time at the front of the range key: \
     unix seconds, zero-padded to 20 digits. A range key without that prefix never expires";
const WRITE_TIME_RULE: &str = "records expire by the time since the last write; a rewrite \
     resets the age";

pub(super) struct RetentionLayout {
    pub(super) age_rule: &'static str,
    pub(super) warnings: Vec<String>,
    pub(super) range_keyed: bool,
}

pub(super) fn prepare_declare_retention(
    ctx: &AccessContext,
    embedded_retention: bool,
    requested: Option<ManifestRetention>,
) -> Result<Option<ManifestRetention>, (u16, String)> {
    if embedded_retention {
        return Err((
            400,
            "retention must sit beside `schema` in the declare body, not inside it".to_string(),
        ));
    }
    let Some(retention) = requested else {
        return Ok(None);
    };
    if !ctx.is_owner {
        return Err((
            403,
            "a schema retention can be declared only on the owner socket".to_string(),
        ));
    }
    retention.ttl().map_err(|e| (400, e))?;
    Ok(Some(retention))
}

pub(super) fn validate_declare_retention(
    declare: &DirectDeclareProposal,
) -> Result<Option<(u64, RetentionLayout)>, String> {
    let Some(retention) = declare.retention else {
        return Ok(None);
    };
    let ttl_seconds = retention.ttl()?;
    let layout = retention_layout(&declare.catalog_schema)?;
    Ok(Some((ttl_seconds, layout)))
}

fn retention_layout(schema: &DeclarativeSchemaDefinition) -> Result<RetentionLayout, String> {
    if matches!(schema.schema_type, SchemaType::Hash | SchemaType::Single) {
        return Ok(RetentionLayout {
            age_rule: WRITE_TIME_RULE,
            warnings: Vec::new(),
            range_keyed: false,
        });
    }
    let primary = &schema
        .record_keys
        .as_ref()
        .ok_or("retention requires explicit record_keys")?
        .primary;
    let warnings = if primary
        .range_type
        .as_ref()
        .is_some_and(can_hold_time_prefix)
    {
        Vec::new()
    } else {
        vec!["retention uses 20 zero-padded unix-second digits at the start of an explicit String range key".into()]
    };
    Ok(RetentionLayout {
        age_rule: RANGE_KEY_RULE,
        warnings,
        range_keyed: true,
    })
}

fn can_hold_time_prefix(kind: &FieldValueType) -> bool {
    match kind {
        FieldValueType::String | FieldValueType::Any => true,
        FieldValueType::OneOf(variants) => variants.iter().any(can_hold_time_prefix),
        _ => false,
    }
}

async fn read_policy(host: &Host, name: &str) -> Result<Option<SchemaRetentionPolicy>, String> {
    match host
        .db
        .db_ops()
        .schemas()
        .get_schema_retention_policy(name)
        .await
    {
        Ok(policy) => Ok(policy),
        Err(SchemaError::NotFound(_)) => Ok(None),
        Err(error) => Err(format!("reading the retention policy of {name}: {error}")),
    }
}

pub(super) async fn precheck_declare_retention(
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
    assessment
        .refusal(declare, retention.confirmed())
        .map_or(Ok(()), Err)
}

async fn unmanaged_report(
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

pub(super) async fn finish_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution.runtime_schema.as_str();
    let Some(retention) = declare.retention else {
        return unmanaged_report(host, declare, anchor).await;
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    if let Some(reason) = assessment.refusal(declare, retention.confirmed()) {
        return Err(reason);
    }
    let outcome = host
        .db
        .db_ops()
        .schemas()
        .apply_manifest_retention(
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
    Ok(Some(applied_report(
        &assessment,
        &outcome,
        binding,
        anchor,
        retention.confirmed(),
    )))
}

pub(super) async fn preview_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let anchor = resolution.runtime_schema.as_str();
    let Some(retention) = declare.retention else {
        return unmanaged_report(host, declare, anchor).await;
    };
    let (ttl_seconds, layout) = validate_declare_retention(declare)?
        .ok_or_else(|| "the declare carries no retention".to_string())?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    let current: Option<SchemaRetentionPolicy> = read_policy(host, anchor).await?;
    let blocked = assessment.refusal(declare, retention.confirmed());
    Ok(Some(preview_report(
        &assessment,
        current.as_ref(),
        binding,
        anchor,
        retention.confirmed(),
        blocked.as_deref(),
    )))
}

fn applied_report(
    assessment: &Assessment,
    outcome: &ManifestRetentionOutcome,
    binding: &str,
    anchor: &str,
    confirmed: bool,
) -> Value {
    let notes = apply_notes(assessment, outcome);
    let mut report = assessment.report(binding, anchor, confirmed);
    report.insert("status".into(), json!(outcome.status));
    report.insert("effective".into(), json!(outcome.effective));
    report.insert("notes".into(), json!(notes));
    Value::Object(report)
}

fn preview_report(
    assessment: &Assessment,
    current: Option<&SchemaRetentionPolicy>,
    binding: &str,
    anchor: &str,
    confirmed: bool,
    blocked: Option<&str>,
) -> Value {
    let operator_wins = current.is_some_and(|policy| policy.source == RetentionSource::Operator);
    let mut report = assessment.report(binding, anchor, confirmed);
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
    Value::Object(report)
}

fn apply_notes(assessment: &Assessment, outcome: &ManifestRetentionOutcome) -> Vec<String> {
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
             beside it. Clearing the operator policy puts the manifest TTL in force: when it is \
             shorter, that needs `--confirm-expire-existing`",
            outcome.effective.ttl_seconds
        )),
    }
    notes
}
