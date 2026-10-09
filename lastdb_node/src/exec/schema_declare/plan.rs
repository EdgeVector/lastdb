//! Direct-declare planning and catalog schema loading.
// lint:file-size-ok moved verbatim from schema_declare.rs; cohesive unit, split further in a later pass

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

// lint:fn-size-ok moved verbatim from schema_declare.rs; splitting this function is separate work
pub(crate) async fn load_catalog_schema_for_direct_declare(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclareResolution, String> {
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
    if let Ok(schemas) = host.db.schema_manager().get_schemas() {
        let persisted_aliases = load_persisted_proposal_catalog_aliases(
            host,
            Some(declare.namespace.as_str()),
            &schemas,
            schemas.values().filter(|schema| {
                schema.identity_hash.as_deref() == Some(proposed_hash)
                    && schema.owner_app_id.as_deref() == Some(declare.namespace.as_str())
            }),
        )
        .await?;
        if let Some(resolution) = locally_loaded_catalog_reuse_resolution(
            &schemas,
            declare,
            proposed_hash,
            &persisted_aliases,
        )? {
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
                    persist_repaired_mapper_source_aliases(
                        host,
                        schema,
                        &schemas,
                        field_mappers,
                        true,
                    )
                    .await?;
                }
                persist_proposal_catalog_alias(
                    host,
                    &declare.namespace,
                    proposed_hash,
                    catalog_hash,
                )
                .await?;
                load_app_single_catalog_binding(host, declare, catalog_hash, field_mappers).await?;
            }
            return Ok(resolution);
        }
    }
    match client.find_schema(proposed_hash).await {
        Ok(Some(envelope)) => {
            let fold_schema: Schema = envelope.schema.into();
            let loaded_schemas =
                host.db.schema_manager().get_schemas().map_err(|e| {
                    format!("failed to inspect loaded schemas for exact reuse: {e}")
                })?;
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
            if let DirectDeclareResolution::Reuse { field_mappers, .. } = &resolution {
                persist_repaired_mapper_source_aliases(
                    host,
                    &fold_schema,
                    &loaded_schemas,
                    field_mappers,
                    true,
                )
                .await?;
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
            return Ok(resolution);
        }
        Ok(None) => {
            tracing::debug!(
                target: "lastdb_node::apps_declare_schema",
                schema = %declare.local_schema_id,
                identity_hash = %proposed_hash,
                "exact catalog identity not found; resolving proposal"
            );
        }
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
    let mut service_registered_schema = None;
    let resolution = if let Some(Ok(resolution)) = &resolved {
        resolution.clone()
    } else {
        let fallback = catalog_registration_fallback(host, declare, result.as_ref(), &resolved);
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
        let field_mappers = registered_binding_field_mappers(
            &catalog_hash,
            &catalog_mappers,
            &added.mutation_mappers,
        );
        service_registered_schema = Some(service_registered_schema_for_identity(
            added.schema.into(),
            &catalog_hash,
        ));
        DirectDeclareResolution::Register {
            catalog_hash,
            field_mappers,
            replaced_schema: added.replaced_schema,
        }
    };
    match &resolution {
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
                load_catalog_hashes_on_host(host, &client, std::slice::from_ref(catalog_hash))
                    .await?;
            }
            if let Some(predecessor) = replaced_schema.as_deref() {
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
            load_catalog_hashes_on_host(host, &client, std::slice::from_ref(catalog_hash)).await?;
            persist_proposal_catalog_alias(host, &declare.namespace, proposed_hash, catalog_hash)
                .await?;
            load_app_single_catalog_binding(host, declare, catalog_hash, field_mappers).await?;
        }
        DirectDeclareResolution::Compose { components, .. } => {
            let hashes: Vec<String> = components.iter().map(|c| c.catalog_hash.clone()).collect();
            load_catalog_hashes_on_host(host, &client, &hashes).await?;
        }
    }
    Ok(resolution)
}
