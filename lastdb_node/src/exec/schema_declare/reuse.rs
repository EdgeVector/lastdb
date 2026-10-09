//! Catalog reuse resolution: mapper reuse and exact-target matching.

use super::*;

/// Rewrite historical mapper sources that name an app proposal identity to
/// the unique installed catalog schema that carries that identity.
///
/// Catalog registration once stored the proposal identity in expansion
/// mappers even when Schema Service returned a different storage name. Keep a
/// missing or ambiguous source unchanged so `apply_field_mappers` still fails
/// closed instead of choosing an unrelated molecule map.
pub(crate) fn reuse_field_mappers_with_loaded_sources(
    schema: &DeclarativeSchemaDefinition,
    schemas: &HashMap<String, Schema>,
    persisted_aliases: &HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    let owner = schema.owner_app_id.as_deref();
    let Some(mappers) = schema.field_mappers.as_ref() else {
        return Ok(HashMap::new());
    };
    mappers
        .iter()
        .map(|(field, mapper)| {
            let source = mapper.source_schema();
            if schemas.contains_key(source) {
                return Ok((field.clone(), format!("{source}.{}", mapper.source_field())));
            }

            let mut matches: Vec<&str> = schemas
                .values()
                .filter(|candidate| {
                    candidate.identity_hash.as_deref() == Some(source)
                        && candidate.owner_app_id.as_deref() == owner
                        && is_catalog_hash(&candidate.name)
                })
                .map(|candidate| candidate.name.as_str())
                .collect();
            if let Some(catalog_hash) = persisted_aliases.get(source) {
                if schemas.get(catalog_hash).is_some_and(|candidate| {
                    candidate.owner_app_id.as_deref() == owner && is_catalog_hash(&candidate.name)
                }) {
                    matches.push(catalog_hash);
                }
            }
            // An expansion response records its old catalog identity as the
            // predecessor. This is the one bounded recovery path for nodes
            // that predate the durable alias row. Once selected, the caller
            // persists `source -> predecessor`, so sibling exact declarations
            // use the point-read alias after the next restart.
            matches.extend(
                schemas
                    .values()
                    .filter(|candidate| {
                        candidate.superseded_by.as_deref() == Some(schema.name.as_str())
                            && candidate.owner_app_id.as_deref() == owner
                            && is_catalog_hash(&candidate.name)
                    })
                    .map(|candidate| candidate.name.as_str()),
            );
            matches.sort_unstable();
            matches.dedup();
            let resolved = match matches.as_slice() {
                [catalog_name] => *catalog_name,
                [] => source,
                _ => {
                    return Err(format!(
                        "mapper source {source} matches multiple installed catalog schemas: {}",
                        matches.join(", ")
                    ))
                }
            };
            Ok((
                field.clone(),
                format!("{resolved}.{}", mapper.source_field()),
            ))
        })
        .collect()
}

pub(crate) async fn persist_repaired_mapper_source_aliases(
    host: &Host,
    schema: &DeclarativeSchemaDefinition,
    schemas: &HashMap<String, Schema>,
    field_mappers: &HashMap<String, String>,
    write: bool,
) -> Result<(), String> {
    let Some(owner_app_id) = schema.owner_app_id.as_deref() else {
        return Ok(());
    };
    let Some(original_mappers) = schema.field_mappers.as_ref() else {
        return Ok(());
    };
    for (field, original) in original_mappers {
        if schemas.contains_key(original.source_schema()) {
            continue;
        }
        let Some(repaired) = field_mappers.get(field) else {
            continue;
        };
        let repaired = FieldMapper::try_from(repaired.as_str())
            .map_err(|e| format!("invalid repaired field mapper for {field}: {e}"))?;
        if repaired.source_schema() == original.source_schema()
            || !schemas.contains_key(repaired.source_schema())
        {
            continue;
        }
        if write {
            persist_proposal_catalog_alias(
                host,
                owner_app_id,
                original.source_schema(),
                repaired.source_schema(),
            )
            .await?;
        } else {
            check_proposal_catalog_alias(
                host,
                owner_app_id,
                original.source_schema(),
                repaired.source_schema(),
            )
            .await?;
        }
    }
    Ok(())
}

/// Resolve a proposal that this node already bound to a catalog identity.
///
/// Schema Service can normalize a proposal into an existing canonical whose
/// hash differs from the proposal hash. The loaded schema preserves both
/// identities: `name` is the catalog hash and `identity_hash` is the proposal
/// hash. That pair is the durable point mapping for later declarations. Use it
/// before a network lookup so an unavailable resolver cannot send the same
/// proposal through the quota-counted add path on every routine run.
pub(crate) fn locally_loaded_catalog_reuse_resolution(
    schemas: &HashMap<String, Schema>,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
    persisted_aliases: &HashMap<String, String>,
) -> Result<Option<DirectDeclareResolution>, String> {
    let mut matches: Vec<&Schema> = schemas
        .values()
        .filter(|schema| {
            schema.identity_hash.as_deref() == Some(proposed_hash)
                && schema.owner_app_id.as_deref() == Some(declare.namespace.as_str())
                && schema.descriptive_name.as_deref()
                    == Some(declare.proposal.descriptive_name.as_str())
                && schema.name.len() == 64
                && schema.name.chars().all(|c| c.is_ascii_hexdigit())
        })
        .collect();
    matches.sort_by(|a, b| a.name.cmp(&b.name));
    matches.dedup_by(|a, b| a.name == b.name);

    match matches.as_slice() {
        [] => Ok(None),
        [schema] => Ok(Some(DirectDeclareResolution::Reuse {
            catalog_hash: schema.name.clone(),
            field_mappers: reuse_field_mappers_with_loaded_sources(
                schema,
                schemas,
                persisted_aliases,
            )?,
            confidence: Some(1.0),
        })),
        schemas => Err(format!(
            "proposal {proposed_hash} is bound to multiple loaded catalog identities: {}",
            schemas
                .iter()
                .map(|schema| schema.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

pub(crate) fn exact_catalog_reuse_resolution(
    proposed_hash: &str,
    schema: &DeclarativeSchemaDefinition,
    loaded_schemas: Option<&HashMap<String, Schema>>,
    persisted_aliases: &HashMap<String, String>,
) -> Result<DirectDeclareResolution, String> {
    let catalog_hash = schema
        .identity_hash
        .as_deref()
        .unwrap_or(schema.name.as_str());
    if catalog_hash != proposed_hash {
        return Err(format!(
            "catalog returned identity {catalog_hash} for exact lookup {proposed_hash}"
        ));
    }
    let field_mappers = match loaded_schemas {
        Some(schemas) => {
            reuse_field_mappers_with_loaded_sources(schema, schemas, persisted_aliases)?
        }
        None => reuse_field_mappers(schema),
    };
    Ok(DirectDeclareResolution::Reuse {
        catalog_hash: catalog_hash.to_string(),
        field_mappers,
        confidence: Some(1.0),
    })
}
