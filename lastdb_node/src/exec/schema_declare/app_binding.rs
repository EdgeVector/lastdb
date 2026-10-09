//! Single-catalog app bindings and molecule adoption for empty catalogs.

use super::*;

/// Build the app-qualified runtime edge for one canonical catalog schema.
///
/// Loading only `catalog_hash` is insufficient: `/api/query` resolves the
/// app-facing `owner/name`, while the catalog row is keyed by its hash and can
/// be shared by more than one app. The edge keeps the app proposal identity
/// and maps its molecules onto the canonical schema. It does not mint a
/// catalog identity or copy rows.
pub(crate) fn app_single_catalog_binding(
    declare: &DirectDeclareProposal,
    catalog_hash: &str,
    field_mappers: &HashMap<String, String>,
) -> Result<Schema, String> {
    let mut binding = declare.catalog_schema.clone();
    let fields = binding.fields.clone().unwrap_or_default();
    let mut mappers = HashMap::with_capacity(fields.len());
    for field in fields {
        let mapper = match field_mappers.get(&field) {
            Some(mapper) => FieldMapper::try_from(qualify_catalog_mapper(catalog_hash, mapper))
                .map_err(|e| {
                    format!(
                        "invalid catalog field mapper for {}.{field}: {e}",
                        declare.local_schema_id
                    )
                })?,
            None => FieldMapper::new(catalog_hash, &field),
        };
        mappers.insert(field, mapper);
    }
    binding.name = declare.local_schema_id.clone();
    binding.field_mappers = Some(mappers);
    binding
        .populate_runtime_fields()
        .map_err(|e| format!("failed to build app catalog binding: {e}"))?;
    Ok(binding)
}

pub(crate) async fn load_app_single_catalog_binding(
    host: &Host,
    declare: &DirectDeclareProposal,
    catalog_hash: &str,
    field_mappers: &HashMap<String, String>,
) -> Result<(), String> {
    let binding = app_single_catalog_binding(declare, catalog_hash, field_mappers)?;
    if let Some(catalog_fields) = host
        .db
        .schema_manager()
        .get_schema_metadata(catalog_hash)
        .map_err(|e| format!("failed to read catalog schema {catalog_hash}: {e}"))?
        .and_then(|catalog| catalog.fields)
    {
        check_binding_sources_exist(&binding, catalog_hash, &catalog_fields)?;
    }
    adopt_existing_app_molecules_for_empty_catalog(host, &binding, catalog_hash).await?;
    host.db
        .schema_manager()
        .load_schema_internal(binding)
        .await
        .map_err(|e| {
            format!(
                "failed to load app binding {} -> {catalog_hash}: {e}",
                declare.local_schema_id
            )
        })?;

    // The catalog schema can carry molecule UUIDs that predate the current
    // deterministic derivation. Loading the mapper metadata alone leaves the
    // app alias on derived, empty molecules, so every historical row appears
    // absent. Apply the mappers now and persist the catalog's stored molecule
    // locations onto the app-qualified schema.
    host.db
        .schema_manager()
        .apply_field_mappers(&declare.local_schema_id)
        .await
        .map_err(|e| {
            format!(
                "failed to apply app binding {} -> {catalog_hash}: {e}",
                declare.local_schema_id
            )
        })?;
    // apply_field_mappers already copies R when the catalog has one.
    // Call apply_record_mapper as well so a bind that ships only a
    // RecordMapper (no FieldMappers) still lands on the catalog's R.
    host.db
        .schema_manager()
        .apply_record_mapper(&declare.local_schema_id)
        .await
        .map_err(|e| {
            format!(
                "failed to apply record mapper {} -> {catalog_hash}: {e}",
                declare.local_schema_id
            )
        })
}

/// Preserve local rows when an existing app schema first binds to an empty
/// exact catalog identity.
///
/// The app schema's physical map is the only source of truth for rows written
/// before catalog bind. If its key molecule has live rows and the catalog key
/// molecule has none, move the catalog metadata onto that map before the app
/// mapper resolves. This changes metadata only. A catalog that already has
/// rows is a conflict and must not be repointed automatically.
// lint:fn-size-ok moved verbatim from schema_declare.rs; splitting this function is separate work
pub(crate) async fn adopt_existing_app_molecules_for_empty_catalog(
    host: &Host,
    binding: &Schema,
    catalog_hash: &str,
) -> Result<(), String> {
    let manager = host.db.schema_manager();
    let Some(existing_app) = manager
        .get_schema_metadata(&binding.name)
        .map_err(|e| format!("failed to read existing app schema {}: {e}", binding.name))?
    else {
        return Ok(());
    };
    let Some(catalog) = manager
        .get_schema_metadata(catalog_hash)
        .map_err(|e| format!("failed to read catalog schema {catalog_hash}: {e}"))?
    else {
        return Err(format!("catalog schema {catalog_hash} is not loaded"));
    };

    let app_map = existing_app
        .field_molecule_uuids
        .clone()
        .unwrap_or_default();
    let catalog_map = catalog.field_molecule_uuids.clone().unwrap_or_default();
    if app_map.is_empty() || app_map == catalog_map {
        return Ok(());
    }
    // A catalog that only ADDS a field to an otherwise-unchanged app schema is
    // not a physical-map collision: every molecule the two maps already share
    // by field name still resolves to the identical UUID (including the key
    // molecule, so the "both have live rows" check below would otherwise trip
    // on the app's own rows through its own unchanged key). Only a genuine
    // conflicting remap of an EXISTING shared field is unsafe to skip.
    let shared_fields_match = app_map
        .iter()
        .all(|(field, uuid)| match catalog_map.get(field) {
            Some(catalog_uuid) => catalog_uuid == uuid,
            None => true,
        });
    if shared_fields_match {
        return Ok(());
    }

    let key_molecule = |schema: &Schema, map: &HashMap<String, String>| {
        schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
            .and_then(|field| map.get(field))
            .cloned()
    };
    let has_live_key = |molecule: Option<String>| async move {
        let Some(molecule) = molecule else {
            return Ok(false);
        };
        host.db
            .db_ops()
            .atoms()
            .list_live_record_keys(&molecule, 1, None, None)
            .await
            .map(|(keys, _, _)| !keys.is_empty())
            .map_err(|e| format!("failed to probe key molecule {molecule}: {e}"))
    };
    let app_has_rows = has_live_key(key_molecule(&existing_app, &app_map)).await?;
    if !app_has_rows {
        return Ok(());
    }
    let catalog_has_rows = has_live_key(key_molecule(&catalog, &catalog_map)).await?;
    if catalog_has_rows {
        return Err(format!(
            "refusing catalog bind for {}: both the app schema and catalog schema have rows on different physical maps",
            binding.name
        ));
    }

    let mut replacement = catalog_map.clone();
    let binding_mappers = binding
        .field_mappers
        .as_ref()
        .ok_or_else(|| format!("app catalog binding {} has no field mappers", binding.name))?;
    for (app_field, mapper) in binding_mappers {
        if mapper.source_schema() != catalog_hash {
            continue;
        }
        let Some(molecule) = app_map.get(app_field) else {
            continue;
        };
        let source_field = mapper.source_field().to_string();
        if let Some(previous) = replacement.insert(source_field.clone(), molecule.clone()) {
            if previous != *molecule
                && binding_mappers.iter().any(|(other_field, other_mapper)| {
                    other_field != app_field
                        && other_mapper.source_schema() == catalog_hash
                        && other_mapper.source_field() == source_field
                        && app_map
                            .get(other_field)
                            .is_some_and(|other| other != molecule)
                })
            {
                return Err(format!(
                    "refusing catalog bind for {}: app fields disagree on catalog field {source_field}",
                    binding.name
                ));
            }
        }
    }

    let expected = fold_db::schema::SchemaCore::field_molecule_map_fingerprint(&catalog_map);
    manager
        .repair_field_molecule_uuids(catalog_hash, &expected, replacement)
        .await
        .map_err(|e| {
            format!(
                "failed to preserve existing app molecules for {} -> {catalog_hash}: {e}",
                binding.name
            )
        })?;
    tracing::warn!(
        target: "lastdb_node::apps_declare_schema",
        schema = %binding.name,
        catalog = %catalog_hash,
        "adopted the existing app field map because the catalog key molecule was empty"
    );
    Ok(())
}
