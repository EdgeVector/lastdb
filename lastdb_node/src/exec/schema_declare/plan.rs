//! Direct-declare planning and catalog schema loading.

use super::*;

/// What a read-only `check` declare found: the resolution catalog sync would
/// take, and, for a registration, why it would register.
pub(crate) struct DirectDeclarePlan {
    pub(crate) resolution: DirectDeclareResolution,
    pub(crate) register_reason: Option<&'static str>,
}

/// Validate a reuse bind without loading or persisting anything.
///
/// Mirrors the checks the catalog-sync bind runs before it writes: the
/// proposal-to-catalog alias must not conflict, every field mapper must
/// parse, and every mapper must name a field the catalog schema has.
pub(crate) async fn check_reuse_bind(
    host: &Host,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
    catalog_hash: &str,
    field_mappers: &HashMap<String, String>,
    catalog_fields: Option<&[String]>,
) -> Result<(), String> {
    check_proposal_catalog_alias(host, &declare.namespace, proposed_hash, catalog_hash).await?;
    let binding = app_single_catalog_binding(declare, catalog_hash, field_mappers)?;
    if let Some(catalog_fields) = catalog_fields {
        check_binding_sources_exist(&binding, catalog_hash, catalog_fields)?;
    }
    Ok(())
}

/// Read-only twin of [`load_catalog_schema_for_direct_declare`] for the
/// `check` intent.
///
/// It takes the same decisions in the same order (loaded reuse, exact
/// catalog lookup, resolver, registration fallback) and runs the same bind
/// validation, but it never registers with Schema Service, never loads a
/// schema, never persists an alias, and never binds. Schema Service is only
/// read (`GET /v1/schema/{hash}` and resolve).
pub(crate) async fn plan_catalog_schema_for_direct_declare(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclarePlan, String> {
    let url = folddb_profile::endpoints::schema_service_url();
    let client = crate::schema_resolver_host::schema_service_client_with_node_identity(
        &host.home,
        schema_service_client::SchemaServiceClient::new(&url),
    )?;
    let proposed_hash = declare
        .proposal
        .identity_hash
        .as_deref()
        .ok_or_else(|| "catalog sync proposal is missing its identity hash".to_string())?;
    let loaded_schemas = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|e| format!("failed to inspect loaded schemas: {e}"))?;

    if let Some(plan) =
        plan_from_loaded_schemas(host, declare, proposed_hash, &loaded_schemas).await?
    {
        return Ok(plan);
    }
    if let Some(plan) =
        plan_from_exact_catalog(host, &client, declare, proposed_hash, &loaded_schemas).await?
    {
        return Ok(plan);
    }
    plan_from_resolver(host, &client, &url, declare, proposed_hash, &loaded_schemas).await
}

/// Plan step 1: a locally loaded schema already satisfies the proposal.
async fn plan_from_loaded_schemas(
    host: &Host,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
    loaded_schemas: &HashMap<String, Schema>,
) -> Result<Option<DirectDeclarePlan>, String> {
    let Some(resolution) =
        read_loaded_catalog_reuse(host, declare, proposed_hash, loaded_schemas).await?
    else {
        return Ok(None);
    };
    if let DirectDeclareResolution::Reuse {
        catalog_hash,
        field_mappers,
        ..
    } = &resolution
    {
        if let Some(schema) = loaded_schemas.get(catalog_hash) {
            for alias in repaired_mapper_source_aliases(schema, loaded_schemas, field_mappers) {
                let alias = alias?;
                check_proposal_catalog_alias(
                    host,
                    alias.owner_app_id,
                    alias.proposal,
                    alias.repaired.source_schema(),
                )
                .await?;
            }
        }
        let catalog_fields = loaded_schemas
            .get(catalog_hash)
            .and_then(|schema| schema.fields.clone());
        check_reuse_bind(
            host,
            declare,
            proposed_hash,
            catalog_hash,
            field_mappers,
            catalog_fields.as_deref(),
        )
        .await?;
    }
    Ok(Some(DirectDeclarePlan {
        resolution,
        register_reason: None,
    }))
}

/// Plan step 2: Schema Service already holds a schema with the proposal's
/// exact identity hash.
async fn plan_from_exact_catalog(
    host: &Host,
    client: &schema_service_client::SchemaServiceClient,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
    loaded_schemas: &HashMap<String, Schema>,
) -> Result<Option<DirectDeclarePlan>, String> {
    let envelope = match client.find_schema(proposed_hash).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => return Ok(None),
        Err(e) => {
            return Err(format!(
                "exact catalog lookup failed for {proposed_hash}; refusing ambiguous registration: {e}"
            ));
        }
    };
    let fold_schema: Schema = envelope.schema.into();
    let resolution =
        read_exact_catalog_reuse(host, proposed_hash, &fold_schema, loaded_schemas).await?;
    if let DirectDeclareResolution::Reuse {
        catalog_hash,
        field_mappers,
        ..
    } = &resolution
    {
        for alias in repaired_mapper_source_aliases(&fold_schema, loaded_schemas, field_mappers) {
            let alias = alias?;
            check_proposal_catalog_alias(
                host,
                alias.owner_app_id,
                alias.proposal,
                alias.repaired.source_schema(),
            )
            .await?;
        }
        check_reuse_bind(
            host,
            declare,
            proposed_hash,
            catalog_hash,
            field_mappers,
            fold_schema.fields.as_deref(),
        )
        .await?;
    }
    Ok(Some(DirectDeclarePlan {
        resolution,
        register_reason: None,
    }))
}

/// Plan step 3: ask the resolver; reuse its answer or report the
/// registration fallback.
async fn plan_from_resolver(
    host: &Host,
    client: &schema_service_client::SchemaServiceClient,
    url: &str,
    declare: &DirectDeclareProposal,
    proposed_hash: &str,
    loaded_schemas: &HashMap<String, Schema>,
) -> Result<DirectDeclarePlan, String> {
    let result = resolve_direct_declare_result(host, url, declare).await;
    let resolved = result
        .as_ref()
        .map(|result| resolve_declare_bind(&declare.catalog_schema, result));
    if let Some(Ok(resolution)) = &resolved {
        if let DirectDeclareResolution::Reuse {
            catalog_hash,
            field_mappers,
            ..
        } = resolution
        {
            let catalog_fields = if let Some(schema) = loaded_schemas.get(catalog_hash) {
                schema.fields.clone()
            } else {
                let envelope = client
                    .get_schema(catalog_hash)
                    .await
                    .map_err(|e| format!("failed to fetch catalog schema {catalog_hash}: {e}"))?;
                let mut schema: Schema = envelope.schema.into();
                schema.fields.take()
            };
            check_reuse_bind(
                host,
                declare,
                proposed_hash,
                catalog_hash,
                field_mappers,
                catalog_fields.as_deref(),
            )
            .await?;
        }
        return Ok(DirectDeclarePlan {
            resolution: resolution.clone(),
            register_reason: None,
        });
    }

    // Catalog sync would register (or expand) here. Schema Service decides
    // the stored identity, so report the proposal identity and the anchored
    // predecessor, which is what the registration request would carry.
    let mut fallback = catalog_registration_fallback(declare, result.as_ref(), &resolved);
    if fallback.predecessor.is_none() {
        if let Ok(schemas) = host.db.schema_manager().get_schemas() {
            fallback.predecessor =
                anchor_catalog_expansion_from_local(&schemas, &mut fallback.schema, declare);
        }
    }
    Ok(DirectDeclarePlan {
        resolution: DirectDeclareResolution::Register {
            catalog_hash: proposed_hash.to_string(),
            field_mappers: reuse_field_mappers(&fallback.schema),
            replaced_schema: fallback.predecessor,
        },
        register_reason: Some(fallback.reason),
    })
}
