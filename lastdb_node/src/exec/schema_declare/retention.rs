//! Manifest-declared retention, applied at declare time.
//!
//! The `retention` key of a manifest schema entry travels beside `schema` in
//! the declare body. It is node-local policy and never part of the schema, so
//! it cannot change the schema artifact, its identity hash, or canonicalization.
//!
//! A catalog sync applies the TTL before it returns, so the policy exists
//! before the first write. A read-only check only reports what it would apply.

use super::*;
use fold_db::schema::types::FieldValueType;
use fold_db::schema::{ManifestRetention, RetentionSource, SchemaRetentionPolicy};

const RANGE_KEY_RULE: &str = "records expire by the entry time at the front of the range key: \
     unix seconds, zero-padded to 20 digits. A range key without that prefix never expires";
const WRITE_TIME_RULE: &str = "records expire by the time since the last write; a rewrite \
     resets the age";

/// How the age of a record is read for one schema layout, and what the manifest
/// already shows is wrong with that layout.
pub(crate) struct RetentionLayout {
    pub(crate) age_rule: &'static str,
    pub(crate) warnings: Vec<String>,
}

/// Check the `retention` of a declare body and who may ask for it.
///
/// A non-owner caller may not set a TTL: it deletes data. The key must sit
/// beside `schema`; the same key inside `schema` would be dropped without a
/// word by the schema parser, so it is refused.
pub(crate) fn prepare_declare_retention(
    ctx: &AccessContext,
    body: &[u8],
    requested: Option<ManifestRetention>,
) -> Result<Option<ManifestRetention>, (u16, String)> {
    let embedded = serde_json::from_slice::<Value>(body)
        .ok()
        .is_some_and(|raw| raw.get("schema").and_then(|s| s.get("retention")).is_some());
    if embedded {
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

/// Refuse a retention the declared layout cannot carry, before anything is
/// written. `Ok(None)` means the declare asks for no retention.
pub(crate) fn validate_declare_retention(
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
    let range_field = match schema.schema_type {
        SchemaType::Hash | SchemaType::Single => {
            return Ok(RetentionLayout {
                age_rule: WRITE_TIME_RULE,
                warnings: Vec::new(),
            });
        }
        SchemaType::Range | SchemaType::HashRange => schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
            .filter(|field| !field.is_empty())
            .ok_or_else(|| {
                format!(
                    "a {:?} schema needs key.range_field to carry a retention",
                    schema.schema_type
                )
            })?,
    };
    let mut warnings = Vec::new();
    if let Some(kind) = schema
        .field_types
        .get(range_field)
        .filter(|kind| !can_hold_time_prefix(kind))
    {
        warnings.push(format!(
            "range key field '{range_field}' is declared {kind:?}; only a String field can hold \
             20 digits of zero-padded unix seconds, so no record would expire"
        ));
    }
    Ok(RetentionLayout {
        age_rule: RANGE_KEY_RULE,
        warnings,
    })
}

fn can_hold_time_prefix(kind: &FieldValueType) -> bool {
    match kind {
        FieldValueType::String | FieldValueType::Any => true,
        FieldValueType::OneOf(variants) => variants.iter().any(can_hold_time_prefix),
        _ => false,
    }
}

/// Apply the declared TTL under the catalog schema both installed names share.
///
/// Runs after the declare resolved and bound the schema, before the response
/// returns. Idempotent. It never reads, changes or deletes product data.
pub(crate) async fn apply_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
    resolution: &DirectDeclareResolution,
) -> Result<Option<Value>, String> {
    let Some((ttl_seconds, layout)) = validate_declare_retention(declare)? else {
        return Ok(None);
    };
    let anchor = match resolution {
        DirectDeclareResolution::Register { catalog_hash, .. }
        | DirectDeclareResolution::Reuse { catalog_hash, .. } => catalog_hash.as_str(),
        DirectDeclareResolution::Compose { .. } => {
            return Err(
                "a retention needs one catalog schema, and this declare composed several"
                    .to_string(),
            );
        }
    };
    let outcome = host
        .db
        .db_ops()
        .schemas()
        .apply_manifest_retention(&declare.local_schema_id, anchor, ttl_seconds)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        target: "lastdb_node::apps_declare_schema",
        schema = %declare.local_schema_id,
        anchor = %outcome.anchor,
        ttl_seconds,
        status = ?outcome.status,
        "applied the manifest retention"
    );
    Ok(Some(serde_json::json!({
        "ttl_seconds": ttl_seconds,
        "source": RetentionSource::Manifest.as_str(),
        "status": outcome.status,
        "anchor": outcome.anchor,
        "effective": outcome.effective,
        "age_rule": layout.age_rule,
        "warnings": layout.warnings,
    })))
}

/// What a read-only check reports: the policy a sync would apply, the policy in
/// force now, and anything the manifest already shows is wrong. Writes nothing.
pub(crate) async fn preview_declare_retention(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<Option<Value>, String> {
    let Some((ttl_seconds, layout)) = validate_declare_retention(declare)? else {
        return Ok(None);
    };
    let current: Option<SchemaRetentionPolicy> = host
        .db
        .db_ops()
        .schemas()
        .get_schema_retention_policy(&declare.local_schema_id)
        .await
        .ok()
        .flatten();
    let operator_wins = current
        .as_ref()
        .is_some_and(|policy| policy.source == RetentionSource::Operator);
    Ok(Some(serde_json::json!({
        "ttl_seconds": ttl_seconds,
        "source": RetentionSource::Manifest.as_str(),
        "would_apply": !operator_wins,
        "current": current,
        "age_rule": layout.age_rule,
        "warnings": layout.warnings,
    })))
}
