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
    // One declare at a time for this catalog schema, from the pre-check to the
    // end of the declare: the pre-check, the bind and the retention step must
    // not interleave with another declare of the same schema.
    declare.anchor_lock.hold(catalog_hash).await;
    // A TTL that must be refused (a shared catalog schema, another app's
    // schema, existing records that would expire) is refused before the
    // schema is bound, so a refusal leaves the node as it was.
    if let Err(refusal) = precheck_declare_retention(host, declare, catalog_hash).await {
        discard_orphan_catalog_schema(host, catalog_hash).await;
        return Err(refusal);
    }
    let mut binding = app_single_catalog_binding(declare, catalog_hash, field_mappers)?;
    prepare_catalog_for_load(host, &mut binding).await?;
    if let Some(mut catalog) = host
        .db
        .schema_manager()
        .get_schema_metadata(catalog_hash)
        .map_err(|e| format!("failed to read catalog schema {catalog_hash}: {e}"))?
    {
        let previous_mappers = catalog.field_mappers.clone();
        prepare_catalog_for_load(host, &mut catalog).await?;
        if catalog.field_mappers != previous_mappers {
            host.db
                .schema_manager()
                .load_schema_internal(catalog.clone())
                .await
                .map_err(|e| format!("failed to refresh recovered catalog mappers: {e}"))?;
        }
        anchor_binding_mappers(&mut binding, &catalog)?;
    }
    if let Some(catalog_fields) = host
        .db
        .schema_manager()
        .get_schema_metadata(catalog_hash)
        .map_err(|e| format!("failed to read catalog schema {catalog_hash}: {e}"))?
        .and_then(|mut catalog| catalog.fields.take())
    {
        check_binding_sources_exist(&binding, catalog_hash, &catalog_fields)?;
    }
    adopt_existing_app_molecules_for_empty_catalog(host, &binding, catalog_hash).await?;
    // The catalog schema can carry molecule UUIDs that predate the current
    // deterministic derivation. The load's typed mapper pass binds each app
    // field to the catalog's stored molecule, so one load is enough.
    host.db
        .schema_manager()
        .load_schema_internal(binding)
        .await
        .map_err(|e| {
            format!(
                "failed to load app binding {} -> {catalog_hash}: {e}",
                declare.local_schema_id
            )
        })
}

/// Keep the catalog hop: flattening its cross-key mapper bypasses the catalog's
/// own ordered molecule and routes an app range read onto its hash-only sibling.
fn anchor_binding_mappers(binding: &mut Schema, catalog: &Schema) -> Result<(), String> {
    let binding_name = binding.name.clone();
    let Some(mappers) = binding.field_mappers.as_mut() else {
        return Ok(());
    };
    for (field, mapper) in mappers {
        if mapper.source_schema() == catalog.name {
            continue;
        }
        let catalog_field = if catalog
            .field_mappers
            .as_ref()
            .and_then(|sources| sources.get(field))
            == Some(mapper)
        {
            Some(field.clone())
        } else {
            let candidates: Vec<_> = catalog
                .field_mappers
                .iter()
                .flat_map(|m| m.iter())
                .filter(|(_, source)| *source == mapper)
                .map(|(name, _)| name.clone())
                .collect();
            match candidates.as_slice() {
                [only] => Some(only.clone()),
                [] => None,
                _ => {
                    return Err(format!(
                        "ambiguous catalog field for {binding_name}.{field}"
                    ))
                }
            }
        };
        if let Some(catalog_field) = catalog_field {
            *mapper = FieldMapper::new(&catalog.name, catalog_field);
        }
    }
    binding.field_molecule_uuids = None;
    binding
        .populate_runtime_fields()
        .map_err(|e| format!("invalid catalog binding: {e}"))
}

/// Preserve local rows when an existing app schema first binds to an empty
/// exact catalog identity.
///
/// The app schema's physical map is the only source of truth for rows written
/// before catalog bind. If its key molecule has live rows and the catalog key
/// molecule has none, move the catalog metadata onto that map before the app
/// mapper resolves. This changes metadata only. A catalog that already has
/// rows is a conflict and must not be repointed automatically.
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

    let app_has_rows = has_live_key(host, key_molecule(&existing_app, &app_map)).await?;
    if !app_has_rows {
        return Ok(());
    }
    let catalog_has_rows = has_live_key(host, key_molecule(&catalog, &catalog_map)).await?;
    if catalog_has_rows {
        return Err(format!(
            "refusing catalog bind for {}: both the app schema and catalog schema have rows on different physical maps",
            binding.name
        ));
    }

    let replacement = adopted_molecule_map(binding, catalog_hash, &app_map, &catalog_map)?;

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

/// Physical molecule of the schema's key field, when the map names one.
pub(crate) fn key_molecule(schema: &Schema, map: &HashMap<String, String>) -> Option<String> {
    schema
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_ref().or(key.range_field.as_ref()))
        .and_then(|field| map.get(field))
        .cloned()
}

/// Whether the key molecule holds at least one live record.
pub(crate) async fn has_live_key(host: &Host, molecule: Option<String>) -> Result<bool, String> {
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
}

/// Catalog field map with the app's molecules substituted for every catalog
/// field the app binding maps, refusing when app fields disagree.
fn adopted_molecule_map(
    binding: &Schema,
    catalog_hash: &str,
    app_map: &HashMap<String, String>,
    catalog_map: &HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
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
    Ok(replacement)
}
