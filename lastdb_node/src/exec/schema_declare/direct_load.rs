//! Load or register the catalog schema for a direct declare and bind it.

use super::*;

type CatalogClient = schema_service_client::SchemaServiceClient;

pub(crate) async fn load_catalog_schema_for_direct_declare(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclareResolution, String> {
    let url = folddb_profile::endpoints::schema_service_url();
    let client = crate::schema_resolver_host::schema_service_client_with_node_identity(
        &host.home,
        CatalogClient::new(&url),
    )?;
    let proposed_hash = declare
        .proposal
        .identity_hash
        .as_deref()
        .ok_or_else(|| "catalog sync proposal is missing its identity hash".to_string())?;
    if let Some(resolution) = reuse_loaded_binding(host, declare, proposed_hash).await? {
        return Ok(resolution);
    }
    if let Some(resolution) = reuse_exact_catalog(host, &client, declare, proposed_hash).await? {
        return Ok(resolution);
    }
    let (resolution, service_registered_schema) =
        resolve_or_register_catalog(host, &client, &url, declare).await?;
    apply_resolution(
        host,
        &client,
        declare,
        proposed_hash,
        &resolution,
        service_registered_schema,
    )
    .await?;
    Ok(resolution)
}

/// Reuse a proposal-to-catalog binding that is already loaded on this node.
async fn reuse_loaded_binding(
    host: &Host,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
) -> Result<Option<DirectDeclareResolution>, String> {
    let Ok(schemas) = host.db.schema_manager().get_schemas() else {
        return Ok(None);
    };
    let Some(resolution) =
        read_loaded_catalog_reuse(host, declare, proposed_hash, &schemas).await?
    else {
        return Ok(None);
    };
    tracing::debug!(
        target: "lastdb_node::apps_declare_schema",
        schema = %declare.local_schema_id,
        identity_hash = %proposed_hash,
        "reusing the loaded proposal-to-catalog binding"
    );
    if let DirectDeclareResolution::Reuse {
        catalog_hash,
        field_mappers,
        ..
    } = &resolution
    {
        if let Some(schema) = schemas.get(catalog_hash) {
            for alias in repaired_mapper_source_aliases(schema, &schemas, field_mappers) {
                let alias = alias?;
                persist_proposal_catalog_alias(
                    host,
                    alias.owner_app_id,
                    alias.proposal,
                    alias.repaired.source_schema(),
                )
                .await?;
            }
        }
        persist_proposal_catalog_alias(host, &declare.namespace, proposed_hash, catalog_hash)
            .await?;
        load_app_single_catalog_binding(host, declare, catalog_hash, field_mappers).await?;
    }
    Ok(Some(resolution))
}

/// Look the proposal hash up in the catalog; reuse the exact schema if found.
async fn reuse_exact_catalog(
    host: &Host,
    client: &CatalogClient,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
) -> Result<Option<DirectDeclareResolution>, String> {
    let envelope = match client.find_schema(proposed_hash).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            tracing::debug!(
                target: "lastdb_node::apps_declare_schema",
                schema = %declare.local_schema_id,
                identity_hash = %proposed_hash,
                "exact catalog identity not found; resolving proposal"
            );
            return Ok(None);
        }
        Err(e) => {
            return Err(format!(
                "exact catalog lookup failed for {proposed_hash}; refusing ambiguous registration: {e}"
            ));
        }
    };
    let fold_schema: Schema = envelope.schema.into();
    let loaded_schemas = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|e| format!("failed to inspect loaded schemas for exact reuse: {e}"))?;
    let resolution =
        read_exact_catalog_reuse(host, proposed_hash, &fold_schema, &loaded_schemas).await?;
    if let DirectDeclareResolution::Reuse { field_mappers, .. } = &resolution {
        for alias in repaired_mapper_source_aliases(&fold_schema, &loaded_schemas, field_mappers) {
            let alias = alias?;
            persist_proposal_catalog_alias(
                host,
                alias.owner_app_id,
                alias.proposal,
                alias.repaired.source_schema(),
            )
            .await?;
        }
    }
    host.db
        .schema_manager()
        .load_schema_internal(fold_schema)
        .await
        .map_err(|e| format!("failed to load catalog schema {proposed_hash}: {e}"))?;
    if let DirectDeclareResolution::Reuse {
        catalog_hash,
        field_mappers,
        ..
    } = &resolution
    {
        load_app_single_catalog_binding(host, declare, catalog_hash, field_mappers).await?;
    }
    Ok(Some(resolution))
}

/// Run the resolver; when it yields no bind, register the schema with Schema
/// Service through the local-matcher fallback. Also returns the schema the
/// service registered, so the caller can load it without a second lookup.
async fn resolve_or_register_catalog(
    host: &Host,
    client: &CatalogClient,
    url: &str,
    declare: &DirectDeclareProposal,
) -> Result<(DirectDeclareResolution, Option<Schema>), String> {
    let result = resolve_direct_declare_result(host, url, declare).await?;
    let resolved = result
        .as_ref()
        .map(|result| resolve_declare_bind(&declare.catalog_schema, result));
    if let Some(Ok(resolution)) = &resolved {
        return Ok((resolution.clone(), None));
    }
    let mut fallback = catalog_registration_fallback(declare, result.as_ref(), &resolved);
    if fallback.predecessor.is_none() {
        if let Ok(schemas) = host.db.schema_manager().get_schemas() {
            fallback.predecessor =
                anchor_catalog_expansion_from_local(&schemas, &mut fallback.schema, declare);
        }
    }
    let schema = fallback.schema;
    let fallback_reason = fallback.reason;
    let service_schema = schema_types::DeclarativeSchemaDefinition::from(&schema);
    let added = client
        .add_schema_with_match_source(
            &service_schema,
            HashMap::new(),
            "local_matcher_fallback",
            Some(fallback_reason),
        )
        .await
        .map_err(|e| {
            format!(
                "failed to register catalog schema {}: {e}",
                declare.local_schema_id
            )
        })?;
    let catalog_hash = added
        .schema
        .identity_hash
        .clone()
        .unwrap_or_else(|| added.schema.name.clone());
    let catalog_mappers: HashMap<String, String> = added
        .schema
        .field_mappers
        .as_ref()
        .map(|mappers| {
            mappers
                .iter()
                .map(|(field, mapper)| {
                    (
                        field.clone(),
                        format!("{}.{}", mapper.source_schema(), mapper.source_field()),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let field_mappers =
        registered_binding_field_mappers(&catalog_hash, &catalog_mappers, &added.mutation_mappers);
    let registered = service_registered_schema_for_identity(added.schema.into(), &catalog_hash);
    Ok((
        DirectDeclareResolution::Register {
            catalog_hash,
            field_mappers,
            replaced_schema: added.replaced_schema,
        },
        Some(registered),
    ))
}

/// Load the resolved catalog schemas on the host, persist the aliases and bind.
async fn apply_resolution(
    host: &Host,
    client: &CatalogClient,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
    resolution: &DirectDeclareResolution,
    service_registered_schema: Option<Schema>,
) -> Result<(), String> {
    match resolution {
        DirectDeclareResolution::Register {
            catalog_hash,
            field_mappers,
            replaced_schema,
        } => {
            if let Some(schema) = service_registered_schema {
                host.db
                    .schema_manager()
                    .load_schema_internal(schema)
                    .await
                    .map_err(|e| {
                        format!("failed to load registered catalog schema {catalog_hash}: {e}")
                    })?;
            } else {
                load_catalog_hashes_on_host(host, client, std::slice::from_ref(catalog_hash))
                    .await?;
            }
            if let Some(predecessor) = replaced_schema.as_deref() {
                persist_predecessor_aliases(host, declare, field_mappers, predecessor).await?;
            }
            persist_proposal_catalog_alias(host, &declare.namespace, proposed_hash, catalog_hash)
                .await?;
            load_app_single_catalog_binding(host, declare, catalog_hash, field_mappers).await?;
        }
        DirectDeclareResolution::Reuse {
            catalog_hash,
            field_mappers,
            ..
        } => {
            load_catalog_hashes_on_host(host, client, std::slice::from_ref(catalog_hash)).await?;
            persist_proposal_catalog_alias(host, &declare.namespace, proposed_hash, catalog_hash)
                .await?;
            load_app_single_catalog_binding(host, declare, catalog_hash, field_mappers).await?;
        }
        DirectDeclareResolution::Compose { components, .. } => {
            let hashes: Vec<String> = components.iter().map(|c| c.catalog_hash.clone()).collect();
            load_catalog_hashes_on_host(host, client, &hashes).await?;
        }
    }
    Ok(())
}

/// A registered expansion replaced `predecessor`: alias every mapper source
/// schema that is not loaded to that predecessor.
async fn persist_predecessor_aliases(
    host: &Host,
    declare: &DirectDeclareProposal,
    field_mappers: &HashMap<String, String>,
    predecessor: &str,
) -> Result<(), String> {
    let loaded_schemas = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|e| format!("failed to inspect registered expansion: {e}"))?;
    for mapper in field_mappers.values() {
        let mapper = FieldMapper::try_from(mapper.as_str())
            .map_err(|e| format!("invalid registered field mapper: {e}"))?;
        if !loaded_schemas.contains_key(mapper.source_schema()) {
            persist_proposal_catalog_alias(
                host,
                &declare.namespace,
                mapper.source_schema(),
                predecessor,
            )
            .await?;
        }
    }
    Ok(())
}
