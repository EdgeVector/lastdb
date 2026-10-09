use super::*;

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
    let current = self_metric_schema()?;
    let schema_manager = host.db.schema_manager();

    if let Some(existing) = schema_manager
        .get_schema_metadata(SELF_METRIC_SCHEMA)
        .map_err(|e| e.to_string())?
    {
        let missing = missing_self_metric_fields(&existing);
        if missing.is_empty() {
            return Ok(());
        }
        tracing::info!(
            target: "lastdbd::self_metrics",
            missing_fields = ?missing,
            "upgrading stale self-metrics schema"
        );
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

pub(super) async fn ensure_request_ops_rollup_schema(host: &Host) -> Result<(), String> {
    let current = request_ops_rollup_schema()?;
    let schema_manager = host.db.schema_manager();

    if let Some(existing) = schema_manager
        .get_schema_metadata(REQUEST_OPS_ROLLUP_SCHEMA)
        .map_err(|e| e.to_string())?
    {
        let missing = missing_request_ops_rollup_fields(&existing);
        if missing.is_empty() {
            return Ok(());
        }
        tracing::info!(
            target: "lastdbd::self_metrics",
            missing_fields = ?missing,
            "upgrading stale request-ops rollup schema"
        );
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
    let mut schema = DeclarativeSchemaDefinition::new(
        SELF_METRIC_SCHEMA.to_string(),
        DeclarativeSchemaType::HashRange,
        Some(KeyConfig::new(
            Some("series".to_string()),
            Some("sample_id".to_string()),
        )),
        Some(
            SELF_METRIC_FIELDS
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
        ),
        None,
        None,
    );
    schema.descriptive_name = Some("LastDBD Self Metric Sample".to_string());
    schema.purpose_statement =
        Some("Bounded lastdbd process, disk, uptime, sampler, and cloud-sync vitals.".to_string());
    schema.owner_app_id = Some(TELEMETRY_NAMESPACE.to_string());
    schema.compute_identity_hash();
    schema
        .populate_runtime_fields()
        .map_err(|e| e.to_string())?;
    Ok(schema)
}

pub(super) fn request_ops_rollup_schema() -> Result<DeclarativeSchemaDefinition, String> {
    let mut schema = DeclarativeSchemaDefinition::new(
        REQUEST_OPS_ROLLUP_SCHEMA.to_string(),
        DeclarativeSchemaType::HashRange,
        Some(KeyConfig::new(
            Some("series".to_string()),
            Some("sample_id".to_string()),
        )),
        Some(
            REQUEST_OPS_ROLLUP_FIELDS
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
        ),
        None,
        None,
    );
    schema.descriptive_name = Some("LastDB Request Ops Rollup".to_string());
    schema.purpose_statement = Some(
        "Bounded sampler rollups of top request-op aggregate buckets across daemon restarts."
            .to_string(),
    );
    schema.owner_app_id = Some(TELEMETRY_NAMESPACE.to_string());
    schema.compute_identity_hash();
    schema
        .populate_runtime_fields()
        .map_err(|e| e.to_string())?;
    Ok(schema)
}

pub(super) fn missing_self_metric_fields(
    schema: &DeclarativeSchemaDefinition,
) -> Vec<&'static str> {
    let declared = schema.fields.as_deref().unwrap_or_default();
    SELF_METRIC_FIELDS
        .iter()
        .copied()
        .filter(|field| {
            !declared.iter().any(|declared| declared == field)
                || !schema.runtime_fields.contains_key(*field)
        })
        .collect()
}

pub(super) fn missing_request_ops_rollup_fields(
    schema: &DeclarativeSchemaDefinition,
) -> Vec<&'static str> {
    let declared = schema.fields.as_deref().unwrap_or_default();
    REQUEST_OPS_ROLLUP_FIELDS
        .iter()
        .copied()
        .filter(|field| {
            !declared.iter().any(|declared| declared == field)
                || !schema.runtime_fields.contains_key(*field)
        })
        .collect()
}
