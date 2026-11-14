//! Manifest retention validation, pre-apply safety checks, and policy application.

use fold_db::schema::types::{DeclarativeSchemaDefinition, FieldValueType, SchemaType};
use fold_db::schema::{
    ManifestRetention, ManifestRetentionPlan, ManifestRetentionStatus, RetentionSource,
    SchemaError, SchemaRetentionPolicy,
};
use serde_json::{json, Value};

use super::Proposal;
use crate::host::Host;

pub(super) struct ManifestRetentionLayout {
    pub(super) ttl_seconds: u64,
    pub(super) age_rule: &'static str,
    pub(super) warnings: Vec<String>,
    pub(super) range_keyed: bool,
}

pub(super) fn validate_manifest_retention(
    retention: Option<ManifestRetention>,
    schema: &DeclarativeSchemaDefinition,
) -> Result<Option<ManifestRetentionLayout>, String> {
    fn str_prefix(k: &FieldValueType) -> bool {
        match k {
            FieldValueType::String | FieldValueType::Any => true,
            FieldValueType::OneOf(v) => v.iter().any(str_prefix),
            _ => false,
        }
    }
    let Some(ret) = retention else {
        return Ok(None);
    };
    let ttl_seconds = ret.ttl()?;
    if matches!(schema.schema_type, SchemaType::Hash | SchemaType::Single) {
        return Ok(Some(ManifestRetentionLayout {
            ttl_seconds,
            age_rule: "records expire by the time since the last write; a rewrite resets the age",
            warnings: Vec::new(),
            range_keyed: false,
        }));
    }
    let rk = schema
        .record_keys
        .as_ref()
        .ok_or("retention requires explicit record_keys")?;
    let warnings = (!rk.primary.range_type.as_ref().is_some_and(str_prefix))
        .then(|| {
            "retention uses 20 zero-padded unix-second digits at the start of an explicit String range key"
                .into()
        })
        .into_iter()
        .collect();
    Ok(Some(ManifestRetentionLayout {
        ttl_seconds,
        age_rule: "records expire by the entry time at the front of the range key: unix seconds, zero-padded to 20 digits. A range key without that prefix never expires",
        warnings,
        range_keyed: true,
    }))
}

fn existing_warning(binding: &str, ttl: u64) -> String {
    format!(
        "existing_records_may_expire: {binding} already holds records. Every record whose range key is older than {ttl}s is deleted for good: at the next sweep for a Range schema, and in every HashRange partition that receives a later write"
    )
}

pub(super) async fn check_retention(
    host: &Host,
    d: &Proposal,
    anchor: &str,
    ttl: u64,
    range_keyed: bool,
) -> Result<(ManifestRetentionPlan, bool), String> {
    let binding = d.local_schema_id.as_str();
    let meta = host
        .records()
        .schema_manager()
        .get_schema_metadata(anchor)
        .map_err(|e| format!("failed to read schema {anchor}: {e}"))?;
    if let Some(s) = meta
        .as_ref()
        .filter(|s| s.owner_app_id.as_deref() != Some(d.namespace.as_str()))
    {
        return Err(format!(
            "manifest retention refused on {binding}: schema is owned by {}, not {}",
            s.owner_app_id.as_deref().unwrap_or("unowned"),
            d.namespace
        ));
    }
    let plan = host
        .records()
        .db_ops()
        .schemas()
        .plan_manifest_retention(anchor, ttl)
        .await
        .map_err(|e| e.to_string())?;
    let may_expire = match meta.as_ref() {
        Some(s) if range_keyed && plan.status == ManifestRetentionStatus::Applied => host
            .records()
            .db_ops()
            .records()
            .schema_may_hold_records(s)
            .await
            .map_err(|e| format!("failed to probe the records of {anchor}: {e}"))?,
        _ => false,
    };
    if !d
        .retention
        .as_ref()
        .is_some_and(ManifestRetention::confirmed)
        && (plan.shortened || may_expire)
    {
        let mut why = Vec::new();
        if may_expire {
            why.push(existing_warning(binding, ttl));
        }
        if plan.shortened {
            why.push(format!(
                "the manifest TTL would be shortened to {ttl}s, so records between the two TTLs expire"
            ));
        }
        return Err(format!(
            "a manifest TTL on {binding} needs confirmation. {}. Review what would expire, then run `lastdb app check --sync --confirm-expire-existing`",
            why.join(". ")
        ));
    }
    Ok((plan, may_expire))
}

pub(super) async fn finish_retention(
    host: &Host,
    d: &Proposal,
    anchor: &str,
    layout: Option<ManifestRetentionLayout>,
    checked: Option<&(ManifestRetentionPlan, bool)>,
) -> Result<Option<Value>, String> {
    let records = host.records();
    let schemas = records.db_ops().schemas();
    let Some((mut lay, &(ref plan, may_expire))) = layout.zip(checked) else {
        let policy = match schemas.get_schema_retention_policy(anchor).await {
            Ok(p) => p,
            Err(SchemaError::NotFound(_)) => None,
            Err(e) => return Err(format!("reading the retention policy of {anchor}: {e}")),
        };
        let Some(m_ttl) = policy
            .as_ref()
            .and_then(SchemaRetentionPolicy::manifest_ttl_seconds)
        else {
            return Ok(None);
        };
        let warn = format!(
            "this manifest no longer declares a retention for {}, but a manifest TTL of {m_ttl}s is still running. Removing the key does not clear the policy. Run `lastdb admin retention clear --schema {anchor}` to remove it",
            d.local_schema_id
        );
        return Ok(Some(json!({
            "declared": false, "status": "manifest_policy_still_running",
            "ttl_seconds": m_ttl, "anchor": anchor, "notes": [], "warnings": [warn], "blocked": null,
        })));
    };
    let ttl = lay.ttl_seconds;
    let (confirmed, binding) = (
        d.retention
            .as_ref()
            .is_some_and(ManifestRetention::confirmed),
        d.local_schema_id.as_str(),
    );
    let out = schemas
        .apply_manifest_retention(anchor, ttl, &d.namespace, confirmed)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        target: "lastdb_node::apps_declare_schema",
        schema = %binding, anchor = %out.anchor, ttl_seconds = ttl, status = ?out.status,
        "applied the manifest retention"
    );
    let prev_ttl = plan
        .current
        .as_ref()
        .and_then(SchemaRetentionPolicy::manifest_ttl_seconds);
    let change = match (plan.status, plan.current.as_ref()) {
        (ManifestRetentionStatus::Unchanged, _) => "unchanged",
        (_, Some(c)) if c.source == RetentionSource::Manifest => {
            if ttl < c.ttl_seconds {
                "shortened"
            } else {
                "lengthened"
            }
        }
        _ => "new",
    };
    if may_expire {
        lay.warnings.push(existing_warning(binding, ttl));
    }
    let notes = match out.status {
        ManifestRetentionStatus::Applied => {
            vec![format!("a new manifest TTL of {ttl}s is in force")]
        }
        ManifestRetentionStatus::Updated => vec![format!(
            "the manifest TTL changed from {}s to {ttl}s ({change})",
            prev_ttl.unwrap_or(0)
        )],
        ManifestRetentionStatus::Unchanged => Vec::new(),
        ManifestRetentionStatus::OperatorOverride => vec![format!(
            "an operator policy of {}s is in force and wins; the manifest TTL of {ttl}s is kept beside it. Clearing the operator policy puts the manifest TTL in force: when it is shorter, that needs `--confirm-expire-existing`",
            out.effective.ttl_seconds
        )],
    };
    Ok(Some(json!({
        "ttl_seconds": ttl, "source": RetentionSource::Manifest.as_str(),
        "anchor": anchor, "change": change, "previous_ttl_seconds": prev_ttl,
        "age_rule": lay.age_rule, "warnings": lay.warnings,
        "existing_records_may_expire": may_expire, "shortened": plan.shortened,
        "confirmed_expire_existing": confirmed, "status": out.status,
        "effective": out.effective, "notes": notes,
    })))
}
