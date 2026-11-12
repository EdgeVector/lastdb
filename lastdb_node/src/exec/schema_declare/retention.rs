//! Manifest-declared retention: validation, assessment, precheck, apply, and preview.

use fold_db::schema::types::FieldValueType;
use fold_db::schema::{
    ManifestRetention, ManifestRetentionPlan, ManifestRetentionStatus, RetentionSource,
    SchemaError, SchemaRetentionPolicy,
};
use serde_json::{json, Value};

use super::*;

pub(super) struct RetentionLayout {
    age_rule: &'static str,
    warnings: Vec<String>,
    range_keyed: bool,
}

pub(super) fn prepare_declare_retention(
    ctx: &AccessContext,
    embedded_retention: bool,
    requested: Option<ManifestRetention>,
) -> Result<Option<ManifestRetention>, (u16, String)> {
    if embedded_retention {
        return Err((
            400,
            "retention must sit beside `schema` in the declare body, not inside it".into(),
        ));
    }
    let Some(retention) = requested else {
        return Ok(None);
    };
    if !ctx.is_owner {
        return Err((
            403,
            "a schema retention can be declared only on the owner socket".into(),
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
    let schema = &declare.catalog_schema;
    if matches!(schema.schema_type, SchemaType::Hash | SchemaType::Single) {
        return Ok(Some((
            ttl_seconds,
            RetentionLayout {
                age_rule:
                    "records expire by the time since the last write; a rewrite resets the age",
                warnings: Vec::new(),
                range_keyed: false,
            },
        )));
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
    Ok(Some((
        ttl_seconds,
        RetentionLayout {
            age_rule: "records expire by the entry time at the front of the range key: unix seconds, zero-padded to 20 digits. A range key without that prefix never expires",
            warnings,
            range_keyed: true,
        },
    )))
}

fn can_hold_time_prefix(kind: &FieldValueType) -> bool {
    match kind {
        FieldValueType::String | FieldValueType::Any => true,
        FieldValueType::OneOf(variants) => variants.iter().any(can_hold_time_prefix),
        _ => false,
    }
}

pub(super) async fn precheck_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<(), String> {
    let (Some(retention), Some((ttl_seconds, layout))) =
        (declare.retention, validate_declare_retention(declare)?)
    else {
        return Ok(());
    };
    assess(host, declare, anchor, ttl_seconds, layout)
        .await?
        .refusal(declare, retention.confirmed())
        .map_or(Ok(()), Err)
}

async fn unmanaged_report(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
) -> Result<Option<Value>, String> {
    let policy = match host
        .records()
        .db_ops()
        .schemas()
        .get_schema_retention_policy(anchor)
        .await
    {
        Ok(policy) => policy,
        Err(SchemaError::NotFound(_)) => None,
        Err(e) => return Err(format!("reading the retention policy of {anchor}: {e}")),
    };
    let Some(manifest_ttl) = policy
        .as_ref()
        .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
    else {
        return Ok(None);
    };
    let warning = format!(
        "this manifest no longer declares a retention for {}, but a manifest TTL of {manifest_ttl}s is still running. Removing the key does not clear the policy. Run `lastdb admin retention clear --schema {anchor}` to remove it",
        declare.local_schema_id
    );
    Ok(Some(json!({
        "declared": false, "status": "manifest_policy_still_running",
        "ttl_seconds": manifest_ttl, "anchor": anchor,
        "notes": [], "warnings": [warning], "blocked": null,
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
    let (ttl_seconds, layout) =
        validate_declare_retention(declare)?.ok_or("the declare carries no retention")?;
    let binding = declare.local_schema_id.as_str();
    let a = assess(host, declare, anchor, ttl_seconds, layout).await?;
    if let Some(reason) = a.refusal(declare, retention.confirmed()) {
        return Err(reason);
    }
    let outcome = host
        .records()
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
        schema = %binding, anchor = %outcome.anchor, ttl_seconds, status = ?outcome.status,
        "applied the manifest retention"
    );
    let plan = a.plan.as_ref().ok();
    let prev_ttl = plan.and_then(|p| p.current.as_ref()?.manifest_ttl_seconds());
    let shortened = plan.is_some_and(|p| p.shortened);
    let change = match plan.map(|p| (p.status, p.current.as_ref())) {
        None => "unknown",
        Some((ManifestRetentionStatus::Unchanged, _)) => "unchanged",
        Some((_, Some(c))) if c.source == RetentionSource::Manifest => {
            if ttl_seconds < c.ttl_seconds {
                "shortened"
            } else {
                "lengthened"
            }
        }
        Some(_) => "new",
    };
    let mut warnings = a.layout.warnings;
    if a.existing_records_may_expire {
        warnings.push(existing_warning(binding, ttl_seconds));
    }
    let notes = match outcome.status {
        ManifestRetentionStatus::Applied => vec![format!("a new manifest TTL of {ttl_seconds}s is in force")],
        ManifestRetentionStatus::Updated => vec![format!(
            "the manifest TTL changed from {}s to {ttl_seconds}s ({change})",
            prev_ttl.unwrap_or(0),
        )],
        ManifestRetentionStatus::Unchanged => Vec::new(),
        ManifestRetentionStatus::OperatorOverride => vec![format!(
            "an operator policy of {}s is in force and wins; the manifest TTL of {ttl_seconds}s is kept beside it. Clearing the operator policy puts the manifest TTL in force: when it is shorter, that needs `--confirm-expire-existing`",
            outcome.effective.ttl_seconds
        )],
    };
    Ok(Some(json!({
        "ttl_seconds": ttl_seconds,
        "source": RetentionSource::Manifest.as_str(),
        "anchor": anchor,
        "change": change,
        "previous_ttl_seconds": prev_ttl,
        "age_rule": a.layout.age_rule,
        "warnings": warnings,
        "existing_records_may_expire": a.existing_records_may_expire,
        "shortened": shortened,
        "confirmed_expire_existing": retention.confirmed(),
        "status": outcome.status,
        "effective": outcome.effective,
        "notes": notes,
    })))
}

struct Assessment {
    ttl_seconds: u64,
    layout: RetentionLayout,
    foreign_owner: Option<String>,
    plan: Result<ManifestRetentionPlan, String>,
    existing_records_may_expire: bool,
}

fn existing_warning(binding: &str, ttl_seconds: u64) -> String {
    format!(
        "existing_records_may_expire: {binding} already holds records. Every record whose range key is older than {ttl_seconds}s is deleted for good: at the next sweep for a Range schema, and in every HashRange partition that receives a later write"
    )
}

async fn assess(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
    ttl_seconds: u64,
    layout: RetentionLayout,
) -> Result<Assessment, String> {
    let metadata = host
        .records()
        .schema_manager()
        .get_schema_metadata(anchor)
        .map_err(|e| format!("failed to read schema {anchor}: {e}"))?;
    let foreign_owner = metadata
        .as_ref()
        .filter(|s| s.owner_app_id.as_deref() != Some(declare.namespace.as_str()))
        .map(|s| s.owner_app_id.clone().unwrap_or_else(|| "unowned".into()));
    let plan = host
        .records()
        .db_ops()
        .schemas()
        .plan_manifest_retention(anchor, ttl_seconds)
        .await
        .map_err(|e| e.to_string());
    let new_policy = plan
        .as_ref()
        .is_ok_and(|p| p.status == ManifestRetentionStatus::Applied);
    let existing_records_may_expire = match (layout.range_keyed && new_policy, metadata.as_ref()) {
        (true, Some(schema)) => host
            .records()
            .db_ops()
            .records()
            .schema_may_hold_records(schema)
            .await
            .map_err(|e| format!("failed to probe the records of {anchor}: {e}"))?,
        _ => false,
    };
    Ok(Assessment {
        ttl_seconds,
        layout,
        foreign_owner,
        plan,
        existing_records_may_expire,
    })
}

impl Assessment {
    fn refusal(&self, declare: &DirectDeclareProposal, confirmed: bool) -> Option<String> {
        let binding = declare.local_schema_id.as_str();
        if let Some(owner) = &self.foreign_owner {
            return Some(format!(
                "manifest retention refused on {binding}: schema is owned by {owner}, not {}",
                declare.namespace
            ));
        }
        let shortened = match &self.plan {
            Ok(plan) => plan.shortened,
            Err(error) => return Some(error.clone()),
        };
        if confirmed || (!shortened && !self.existing_records_may_expire) {
            return None;
        }
        let mut why = Vec::new();
        if self.existing_records_may_expire {
            why.push(existing_warning(binding, self.ttl_seconds));
        }
        if shortened {
            why.push(format!(
                "the manifest TTL would be shortened to {}s, so records between the two TTLs expire",
                self.ttl_seconds
            ));
        }
        Some(format!(
            "a manifest TTL on {binding} needs confirmation. {}. Review what would expire, then run `lastdb app check --sync --confirm-expire-existing`",
            why.join(". ")
        ))
    }
}
