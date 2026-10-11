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
// lint:fn-size-ok moved verbatim from schema_declare.rs; splitting this function is separate work
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

    let persisted_aliases = load_persisted_proposal_catalog_aliases(
        host,
        Some(declare.namespace.as_str()),
        &loaded_schemas,
        loaded_schemas.values().filter(|schema| {
            schema.identity_hash.as_deref() == Some(proposed_hash)
                && schema.owner_app_id.as_deref() == Some(declare.namespace.as_str())
        }),
    )
    .await?;
    if let Some(resolution) = locally_loaded_catalog_reuse_resolution(
        &loaded_schemas,
        declare,
        proposed_hash,
        &persisted_aliases,
    )? {
        if let DirectDeclareResolution::Reuse {
            catalog_hash,
            field_mappers,
            ..
        } = &resolution
        {
            if let Some(schema) = loaded_schemas.get(catalog_hash) {
                persist_repaired_mapper_source_aliases(
                    host,
                    schema,
                    &loaded_schemas,
                    field_mappers,
                    false,
                )
                .await?;
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
        return Ok(DirectDeclarePlan {
            resolution,
            register_reason: None,
        });
    }

    match client.find_schema(proposed_hash).await {
        Ok(Some(envelope)) => {
            let fold_schema: Schema = envelope.schema.into();
            let mut persisted_aliases = load_persisted_proposal_catalog_aliases(
                host,
                fold_schema.owner_app_id.as_deref(),
                &loaded_schemas,
                std::iter::once(&fold_schema),
            )
            .await?;
            recover_proposal_catalog_aliases_from_exact_target(
                &fold_schema,
                &loaded_schemas,
                &mut persisted_aliases,
            )?;
            let resolution = exact_catalog_reuse_resolution(
                proposed_hash,
                &fold_schema,
                Some(&loaded_schemas),
                &persisted_aliases,
            )?;
            if let DirectDeclareResolution::Reuse {
                catalog_hash,
                field_mappers,
                ..
            } = &resolution
            {
                persist_repaired_mapper_source_aliases(
                    host,
                    &fold_schema,
                    &loaded_schemas,
                    field_mappers,
                    false,
                )
                .await?;
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
            return Ok(DirectDeclarePlan {
                resolution,
                register_reason: None,
            });
        }
        Ok(None) => {}
        Err(e) => {
            return Err(format!(
                "exact catalog lookup failed for {proposed_hash}; refusing ambiguous registration: {e}"
            ));
        }
    }

    let result = resolve_direct_declare_result(host, &url, declare).await?;
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
                let schema: Schema = envelope.schema.into();
                schema.fields
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
    let fallback = catalog_registration_fallback(host, declare, result.as_ref(), &resolved);
    Ok(DirectDeclarePlan {
        resolution: DirectDeclareResolution::Register {
            catalog_hash: proposed_hash.to_string(),
            field_mappers: reuse_field_mappers(&fallback.schema),
            replaced_schema: fallback.predecessor,
        },
        register_reason: Some(fallback.reason),
    })
}
