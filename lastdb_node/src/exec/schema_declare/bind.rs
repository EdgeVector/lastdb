//! Catalog loads and field mapper construction/validation.

use super::*;

pub(crate) async fn load_catalog_hashes_on_host(
    host: &Host,
    client: &schema_service_client::SchemaServiceClient,
    hashes: &[String],
) -> Result<(), String> {
    let mut seen = HashSet::new();
    for hash in hashes {
        let hash = hash.trim();
        if hash.is_empty() || !seen.insert(hash.to_string()) {
            continue;
        }
        let envelope = client
            .get_schema(hash)
            .await
            .map_err(|e| format!("failed to fetch catalog schema {hash}: {e}"))?;
        host.db
            .schema_manager()
            .load_schema_internal(envelope.schema.into())
            .await
            .map_err(|e| format!("failed to load catalog schema {hash}: {e}"))?;
    }
    Ok(())
}

/// Spell a binding mapper as `schema.field`.
///
/// Producers disagree on the right-hand side: a service rename
/// (`mutation_mappers`, some `field_rename_map`s) names only the catalog
/// field, while inherited mappers carry `source_schema.field`. A bare name
/// has no source schema, and `FieldMapper::try_from` refuses it, so it is
/// read as a field of `catalog_hash` — the schema the binding targets.
pub(crate) fn qualify_catalog_mapper(catalog_hash: &str, mapper: &str) -> String {
    let mapper = mapper.trim();
    if mapper.contains('.') {
        mapper.to_string()
    } else {
        format!("{catalog_hash}.{mapper}")
    }
}

/// Field mappers for an app binding onto a schema Schema Service just
/// registered or expanded.
///
/// The two inputs are keyed in different name spaces:
/// - `catalog_mappers` are the registered schema's own mappers, keyed by its
///   canonical field names (an expansion inherits its predecessor's rows).
/// - `mutation_mappers` are the service's renames, `proposal_field ->
///   canonical_field`, recorded when canonicalization or a semantic expand
///   renamed a proposal field.
///
/// Reading only the first left every renamed proposal field to the
/// same-name default in [`app_single_catalog_binding`], i.e. a field the
/// catalog does not have: lastgit `LastgitRepoIndex.updated_at` was renamed
/// to `open_crs_updated_at` and the declare failed in `apply_field_mappers`
/// (2026-09-22). A renamed field follows the catalog's own mapper for its
/// canonical name, so it lands where that field's rows live.
pub(crate) fn registered_binding_field_mappers(
    catalog_hash: &str,
    catalog_mappers: &HashMap<String, String>,
    mutation_mappers: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut mappers = catalog_mappers.clone();
    for (proposal_field, canonical) in mutation_mappers {
        let canonical = qualify_catalog_mapper(catalog_hash, canonical);
        let Ok(parsed) = FieldMapper::try_from(canonical.as_str()) else {
            continue;
        };
        if parsed.source_schema() == catalog_hash && parsed.source_field() == proposal_field {
            continue;
        }
        let target = if parsed.source_schema() == catalog_hash {
            catalog_mappers
                .get(parsed.source_field())
                .cloned()
                .unwrap_or(canonical)
        } else {
            canonical
        };
        mappers.insert(proposal_field.clone(), target);
    }
    mappers
}

/// Refuse a binding that maps onto a catalog field the catalog does not have.
///
/// Without this the binding loads and `apply_field_mappers` fails later with
/// `source field '<catalog>.<field>' missing from runtime_fields`, which names
/// neither the proposal field nor what the catalog does carry.
pub(crate) fn check_binding_sources_exist(
    binding: &Schema,
    catalog_hash: &str,
    catalog_fields: &[String],
) -> Result<(), String> {
    let Some(mappers) = binding.field_mappers.as_ref() else {
        return Ok(());
    };
    let mut missing: Vec<String> = mappers
        .iter()
        .filter(|(_, mapper)| mapper.source_schema() == catalog_hash)
        .filter(|(_, mapper)| !catalog_fields.iter().any(|f| f == mapper.source_field()))
        .map(|(field, mapper)| format!("{field} -> {}", mapper.source_field()))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    missing.sort();
    Err(format!(
        "app binding {} maps onto field(s) catalog schema {catalog_hash} does not have: {}; \
         catalog fields: {}. The schema service renamed or dropped these fields and no \
         mapper names their catalog spelling",
        binding.name,
        missing.join(", "),
        catalog_fields.join(", "),
    ))
}
