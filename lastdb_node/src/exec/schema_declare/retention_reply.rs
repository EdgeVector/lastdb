//! Data-only declaration retention replies. The executor owns audits and writes.

use super::retention_assess::Assessment;
use fold_db::schema::{
    ManifestRetentionOutcome, ManifestRetentionStatus, RetentionSource, SchemaRetentionPolicy,
};
use serde_json::{json, Value};

pub(super) fn applied_report(
    assessment: &Assessment,
    outcome: &ManifestRetentionOutcome,
    binding: &str,
    anchor: &str,
    confirmed: bool,
    moved_from: Option<String>,
) -> Value {
    let moved_from = moved_from.or_else(|| outcome.moved_from.clone());
    let mut notes = apply_notes(assessment, outcome);
    if let Some(from) = &moved_from {
        notes.insert(
            0,
            format!("the retention policy followed the schema from {from} to {anchor}"),
        );
    }
    let mut report = assessment.report(binding, anchor, confirmed);
    report.insert("status".into(), json!(outcome.status));
    report.insert("effective".into(), json!(outcome.effective));
    report.insert("moved_from".into(), json!(moved_from));
    report.insert("notes".into(), json!(notes));
    Value::Object(report)
}

pub(super) fn preview_report(
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
             beside it. Clearing the operator policy puts the manifest TTL in force: when it is \
             shorter, that needs `--confirm-expire-existing`",
            outcome.effective.ttl_seconds
        )),
    }
    notes
}
