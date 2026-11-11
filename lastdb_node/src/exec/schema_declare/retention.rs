//! Manifest-declared retention: validation, assessment, precheck, apply, and preview.

use fold_db::schema::types::FieldValueType;
use fold_db::schema::{
    ManifestRetention, ManifestRetentionPlan, ManifestRetentionStatus, RetentionSource,
    SchemaError, SchemaRetentionPolicy,
};
use serde_json::{json, Map, Value};

use super::*;

const RANGE_KEY_RULE: &str = "records expire by the entry time at the front of the range key: unix seconds, zero-padded to 20 digits. A range key without that prefix never expires";
const WRITE_TIME_RULE: &str =
    "records expire by the time since the last write; a rewrite resets the age";

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
                age_rule: WRITE_TIME_RULE,
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
        vec![
            "retention uses 20 zero-padded unix-second digits at the start of an explicit String range key"
                .into(),
        ]
    };
    Ok(Some((
        ttl_seconds,
        RetentionLayout {
            age_rule: RANGE_KEY_RULE,
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
        Err(e) => Err(format!("reading the retention policy of {name}: {e}")),
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
    let binding = declare.local_schema_id.as_str();
    let Some(manifest_ttl) = read_policy(host, anchor)
        .await?
        .as_ref()
        .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
    else {
        return Ok(None);
    };
    let warning = format!(
        "this manifest no longer declares a retention for {binding}, but a manifest TTL of {manifest_ttl}s is still running. Removing the key does not clear the policy. Run `lastdb admin retention clear --schema {anchor}` to remove it"
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
        schema = %binding, anchor = %outcome.anchor, ttl_seconds, status = ?outcome.status,
        "applied the manifest retention"
    );
    let mut report = assessment.report(binding, anchor, retention.confirmed());
    let ttl = assessment.ttl_seconds;
    let notes = match outcome.status {
        ManifestRetentionStatus::Applied => vec![format!("a new manifest TTL of {ttl}s is in force")],
        ManifestRetentionStatus::Updated => vec![format!(
            "the manifest TTL changed from {}s to {ttl}s ({})",
            assessment.previous_ttl_seconds().unwrap_or(0),
            assessment.change()
        )],
        ManifestRetentionStatus::Unchanged => Vec::new(),
        ManifestRetentionStatus::OperatorOverride => vec![format!(
            "an operator policy of {}s is in force and wins; the manifest TTL of {ttl}s is kept beside it. Clearing the operator policy puts the manifest TTL in force: when it is shorter, that needs `--confirm-expire-existing`",
            outcome.effective.ttl_seconds
        )],
    };
    report.extend([
        ("status".into(), json!(outcome.status)),
        ("effective".into(), json!(outcome.effective)),
        ("notes".into(), json!(notes)),
    ]);
    Ok(Some(Value::Object(report)))
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
    let (ttl_seconds, layout) =
        validate_declare_retention(declare)?.ok_or("the declare carries no retention")?;
    let binding = declare.local_schema_id.as_str();
    let assessment = assess(host, declare, anchor, ttl_seconds, layout).await?;
    let current = read_policy(host, anchor).await?;
    let blocked = assessment.refusal(declare, retention.confirmed());
    let operator_wins = current
        .as_ref()
        .is_some_and(|p| p.source == RetentionSource::Operator);
    let mut report = assessment.report(binding, anchor, retention.confirmed());
    report.extend([
        (
            "would_apply".into(),
            json!(!operator_wins && blocked.is_none()),
        ),
        ("current".into(), json!(current)),
        (
            "requires_confirmation".into(),
            json!(assessment.needs_confirmation()),
        ),
        ("blocked".into(), json!(blocked)),
    ]);
    Ok(Some(Value::Object(report)))
}

struct Assessment {
    ttl_seconds: u64,
    layout: RetentionLayout,
    foreign_owner: Option<String>,
    plan: Result<ManifestRetentionPlan, String>,
    existing_records_may_expire: bool,
}

async fn assess(
    host: &Host,
    declare: &DirectDeclareProposal,
    anchor: &str,
    ttl_seconds: u64,
    layout: RetentionLayout,
) -> Result<Assessment, String> {
    let metadata = host
        .db
        .schema_manager()
        .get_schema_metadata(anchor)
        .map_err(|e| format!("failed to read schema {anchor}: {e}"))?;
    let foreign_owner = metadata
        .as_ref()
        .filter(|s| s.owner_app_id.as_deref() != Some(declare.namespace.as_str()))
        .map(|s| s.owner_app_id.clone().unwrap_or_else(|| "unowned".into()));
    let plan = host
        .db
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
            .db
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
    fn shortened(&self) -> bool {
        self.plan.as_ref().is_ok_and(|p| p.shortened)
    }

    fn needs_confirmation(&self) -> bool {
        self.shortened() || self.existing_records_may_expire
    }

    fn existing_warning(&self, binding: &str) -> String {
        format!(
            "existing_records_may_expire: {binding} already holds records. Every record whose range key is older than {}s is deleted for good: at the next sweep for a Range schema, and in every HashRange partition that receives a later write",
            self.ttl_seconds
        )
    }

    fn refusal(&self, declare: &DirectDeclareProposal, confirmed: bool) -> Option<String> {
        let binding = declare.local_schema_id.as_str();
        if let Some(owner) = &self.foreign_owner {
            return Some(format!(
                "manifest retention refused on {binding}: schema is owned by {owner}, not {}",
                declare.namespace
            ));
        }
        if let Err(error) = &self.plan {
            return Some(error.clone());
        }
        if confirmed || !self.needs_confirmation() {
            return None;
        }
        let mut why = Vec::new();
        if self.existing_records_may_expire {
            why.push(self.existing_warning(binding));
        }
        if self.shortened() {
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

    fn change(&self) -> &'static str {
        let Ok(plan) = &self.plan else {
            return "unknown";
        };
        match (plan.status, &plan.current) {
            (ManifestRetentionStatus::Unchanged, _) => "unchanged",
            (_, Some(c)) if c.source == RetentionSource::Manifest => {
                if self.ttl_seconds < c.ttl_seconds {
                    "shortened"
                } else {
                    "lengthened"
                }
            }
            _ => "new",
        }
    }

    fn previous_ttl_seconds(&self) -> Option<u64> {
        self.plan
            .as_ref()
            .ok()?
            .current
            .as_ref()?
            .manifest_ttl_seconds()
    }

    fn report(&self, binding: &str, anchor: &str, confirmed: bool) -> Map<String, Value> {
        let mut warnings = self.layout.warnings.clone();
        if self.existing_records_may_expire {
            warnings.push(self.existing_warning(binding));
        }
        let Value::Object(map) = json!({
            "ttl_seconds": self.ttl_seconds,
            "source": RetentionSource::Manifest.as_str(),
            "anchor": anchor,
            "change": self.change(),
            "previous_ttl_seconds": self.previous_ttl_seconds(),
            "age_rule": self.layout.age_rule,
            "warnings": warnings,
            "existing_records_may_expire": self.existing_records_may_expire,
            "shortened": self.shortened(),
            "confirmed_expire_existing": confirmed,
        }) else {
            unreachable!()
        };
        map
    }
}
