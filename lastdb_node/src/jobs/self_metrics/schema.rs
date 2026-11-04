use super::*;
use fold_db::schema::types::FieldValueType;

/// Optional best-effort persist into LastDB (off by default).
pub(super) async fn record_sample_to_db(
    host: &Host,
    snapshot: &StatusSnapshot,
    retention_cap: usize,
) -> Result<usize, String> {
    ensure_schema(host).await?;
    write_snapshot(host, snapshot).await?;
    write_request_ops_rollup(host, snapshot).await?;
    let outcome = prune_retention(host, retention_cap).await?;
    prune_request_ops_rollup_retention(host, request_ops_rollup_retention_from_env()).await?;
    Ok(outcome.remaining)
}

pub(super) async fn ensure_schema(host: &Host) -> Result<(), String> {
    ensure_self_metric_schema(host).await?;
    ensure_request_ops_rollup_schema(host).await
}

pub(super) async fn ensure_self_metric_schema(host: &Host) -> Result<(), String> {
    ensure_telemetry_schema(
        host,
        SELF_METRIC_SCHEMA,
        self_metric_schema(),
        missing_self_metric_fields,
        |missing| {
            tracing::info!(
                target: "lastdbd::self_metrics",
                missing_fields = ?missing,
                "upgrading stale self-metrics schema"
            );
        },
    )
    .await
}

pub(super) async fn ensure_request_ops_rollup_schema(host: &Host) -> Result<(), String> {
    ensure_telemetry_schema(
        host,
        REQUEST_OPS_ROLLUP_SCHEMA,
        request_ops_rollup_schema(),
        missing_request_ops_rollup_fields,
        |missing| {
            tracing::info!(
                target: "lastdbd::self_metrics",
                missing_fields = ?missing,
                "upgrading stale request-ops rollup schema"
            );
        },
    )
    .await
}

async fn ensure_telemetry_schema<F>(
    host: &Host,
    name: &str,
    current: Result<DeclarativeSchemaDefinition, String>,
    missing_of: fn(&DeclarativeSchemaDefinition) -> Vec<&'static str>,
    log_upgrade: F,
) -> Result<(), String>
where
    F: Fn(&[&str]),
{
    let current = current?;
    let schema_manager = host.db.schema_manager();
    if let Some(existing) = schema_manager
        .get_schema_metadata(name)
        .map_err(|e| e.to_string())?
    {
        let missing = missing_of(&existing);
        // A schema with a missing field or a stale field type reloads in place.
        if missing.is_empty() && existing.field_types == current.field_types {
            return Ok(());
        }
        log_upgrade(&missing);
        return schema_manager
            .update_schema(&current)
            .await
            .map_err(|e| e.to_string());
    }
    schema_manager
        .load_schema_internal(current)
        .await
        .map_err(|e| e.to_string())
}

pub(super) fn self_metric_schema() -> Result<DeclarativeSchemaDefinition, String> {
    telemetry_hash_range_schema(
        SELF_METRIC_SCHEMA,
        SELF_METRIC_FIELDS,
        "LastDBD Self Metric Sample",
        "Bounded lastdbd process, disk, uptime, sampler, and backup vitals.",
    )
}

pub(super) fn request_ops_rollup_schema() -> Result<DeclarativeSchemaDefinition, String> {
    telemetry_hash_range_schema(
        REQUEST_OPS_ROLLUP_SCHEMA,
        REQUEST_OPS_ROLLUP_FIELDS,
        "LastDB Request Ops Rollup",
        "Bounded sampler rollups of top request-op aggregate buckets across daemon restarts.",
    )
}

fn telemetry_hash_range_schema(
    name: &str,
    fields: &[&str],
    descriptive_name: &str,
    purpose: &str,
) -> Result<DeclarativeSchemaDefinition, String> {
    let mut schema = DeclarativeSchemaDefinition::new(
        name.to_string(),
        DeclarativeSchemaType::HashRange,
        None,
        Some(
            fields
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
        ),
        None,
    );
    schema.record_keys = Some(schema_types::record_keys::RecordKeys {
        primary: schema_types::record_keys::RecordKeyDefinition::hash_range(FieldValueType::String),
        alternates: Default::default(),
    });
    schema.field_types = fields
        .iter()
        .map(|&field| (field.to_string(), telemetry_field_type(field)))
        .collect();
    schema.descriptive_name = Some(descriptive_name.to_string());
    schema.purpose_statement = Some(purpose.to_string());
    schema.owner_app_id = Some(TELEMETRY_NAMESPACE.to_string());
    schema.compute_identity_hash();
    schema
        .populate_runtime_fields()
        .map_err(|e| e.to_string())?;
    Ok(schema)
}

/// Every telemetry field names the type its writer stores. A gauge that the
/// platform does not serve is written as null.
fn telemetry_field_type(field: &str) -> FieldValueType {
    match field {
        "series" | "sample_id" | "governor_state" | "host_pressure" | "allocator_name"
        | "client" | "kind" | "schema" | "route" => FieldValueType::String,
        "sampler_last_error" => {
            FieldValueType::OneOf(vec![FieldValueType::String, FieldValueType::Null])
        }
        "footprint_sticky"
        | "purge_failed"
        | "backup_v2_observed"
        | "backup_v2_accepted_backup_recorded"
        | "backup_automatic_enabled" => FieldValueType::Boolean,
        "backup_durability_degraded" => {
            FieldValueType::OneOf(vec![FieldValueType::Boolean, FieldValueType::Null])
        }
        "backup_durability_reasons" => FieldValueType::Array(Box::new(FieldValueType::String)),
        _ => FieldValueType::OneOf(vec![FieldValueType::Number, FieldValueType::Null]),
    }
}

pub(super) fn missing_self_metric_fields(
    schema: &DeclarativeSchemaDefinition,
) -> Vec<&'static str> {
    missing_named_fields(schema, SELF_METRIC_FIELDS)
}

pub(super) fn missing_request_ops_rollup_fields(
    schema: &DeclarativeSchemaDefinition,
) -> Vec<&'static str> {
    missing_named_fields(schema, REQUEST_OPS_ROLLUP_FIELDS)
}

fn missing_named_fields(
    schema: &DeclarativeSchemaDefinition,
    fields: &[&'static str],
) -> Vec<&'static str> {
    let declared = schema.fields.as_deref().unwrap_or_default();
    fields
        .iter()
        .copied()
        .filter(|field| {
            !declared.iter().any(|declared| declared == field)
                || !schema.runtime_fields.contains_key(*field)
        })
        .collect()
}
