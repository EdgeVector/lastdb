//! Restore ordinary proposal aliases before strict typed molecule resolution.

use super::*;

pub(crate) async fn prepare_catalog_for_load(
    host: &Host,
    schema: &mut Schema,
) -> Result<(), String> {
    if schema.field_mappers.as_ref().is_none_or(HashMap::is_empty) {
        return Ok(());
    }
    let loaded = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|e| format!("failed to inspect catalog mapper sources: {e}"))?;
    let mut aliases = load_persisted_proposal_catalog_aliases(
        host,
        schema.owner_app_id.as_deref(),
        &loaded,
        std::iter::once(&*schema),
    )
    .await?;
    recover_proposal_catalog_aliases_from_exact_target(schema, &loaded, &mut aliases)?;
    let recovered = reuse_field_mappers_with_loaded_sources(schema, &loaded, &aliases)?;
    adopt_catalog_mapper_sources(schema, &recovered)
}

/// Reuse already recovered names, without adding app renames to the catalog.
pub(crate) fn adopt_catalog_mapper_sources(
    schema: &mut Schema,
    recovered: &HashMap<String, String>,
) -> Result<(), String> {
    let Some(existing) = schema.field_mappers.as_ref() else {
        return Ok(());
    };
    let mut mappers = existing.clone();
    for (field, mapper) in &mut mappers {
        if let Some(value) = recovered.get(field) {
            *mapper = FieldMapper::try_from(value.as_str())
                .map_err(|e| format!("invalid recovered catalog mapper for {field}: {e}"))?;
        }
    }
    if &mappers != existing {
        schema.field_mappers = Some(mappers);
        schema.runtime_fields.clear();
    }
    Ok(())
}
