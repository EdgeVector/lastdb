//! One owned app schema. Schema Service supplies definitions, never value aliases.

use super::*;

fn client(host: &Host) -> Result<schema_service_client::SchemaServiceClient, String> {
    crate::schema_resolver_host::schema_service_client_with_node_identity(
        &host.home,
        schema_service_client::SchemaServiceClient::new(
            &folddb_profile::endpoints::schema_service_url(),
        ),
    )
}

/// A repeat declare cannot convert old data, change its key, or discard a layout.
fn check_local(host: &Host, declare: &DirectDeclareProposal) -> Result<(), String> {
    let proposed = &declare.catalog_schema;
    proposed.validate_alternate_keys()?;
    let schemas = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| error.to_string())?;
    for stored in schemas.values() {
        if stored.name == declare.local_schema_id {
            if !stored.uses_independent_fields()
                || stored.identity_hash != proposed.identity_hash
                || stored.alternate_keys != proposed.alternate_keys
                || stored.field_descriptions != proposed.field_descriptions
            {
                return Err("existing app data requires an explicit schema conversion or alternate-key change; declare cannot replace it".into());
            }
        } else if stored.identity_hash == proposed.identity_hash
            || (stored.owner_app_id == proposed.owner_app_id
                && stored.descriptive_name == proposed.descriptive_name)
        {
            return Err("this definition already has a local data schema; use its runtime name or explicitly convert the existing data".into());
        }
    }
    Ok(())
}

/// Keep exact field meanings and the primary key. Resolver expansions and
/// catalog value mappers do not authorize any change to an owned app record.
fn owned_schema(
    declare: &DirectDeclareProposal,
    definition: schema_types::DeclarativeSchemaDefinition,
) -> Result<Schema, String> {
    let requested = &declare.catalog_schema;
    let mut actual_fields = definition.fields.clone().unwrap_or_default();
    let mut requested_fields = requested.fields.clone().unwrap_or_default();
    actual_fields.sort();
    requested_fields.sort();
    if definition.identity_hash != requested.identity_hash
        || definition.owner_app_id != requested.owner_app_id
        || definition.descriptive_name != requested.descriptive_name
        || definition.schema_type != requested.schema_type
        || definition.key != requested.key
        || actual_fields != requested_fields
        || definition.field_descriptions != requested.field_descriptions
        || requested
            .field_types
            .iter()
            .any(|(field, kind)| definition.field_types.get(field) != Some(kind))
    {
        return Err("Schema Service returned a different field or primary-key definition; refusing an implicit app conversion".into());
    }
    let mut schema = requested.clone();
    // Only definition metadata crosses this boundary. No service molecule ID,
    // predecessor, transform, mapper, or alternate layout owns local values.
    schema.field_types = definition.field_types;
    schema.field_hashes = definition.field_hashes;
    schema.field_versions = definition.field_versions;
    schema.field_declarations = definition.field_declarations;
    schema.field_classifications = definition.field_classifications;
    schema.field_data_classifications = definition.field_data_classifications;
    schema.field_interest_categories = definition.field_interest_categories;
    schema.independent_fields = true;
    schema.field_molecule_uuids = None;
    schema
        .populate_runtime_fields()
        .map_err(|error| error.to_string())?;
    schema.validate_alternate_keys()?;
    Ok(schema)
}

fn resolution(declare: &DirectDeclareProposal, reused: bool) -> DirectDeclareResolution {
    if reused {
        DirectDeclareResolution::Reuse {
            catalog_hash: declare.local_schema_id.clone(),
            field_mappers: HashMap::new(),
            confidence: None,
        }
    } else {
        DirectDeclareResolution::Register {
            catalog_hash: declare.local_schema_id.clone(),
            field_mappers: HashMap::new(),
            replaced_schema: None,
        }
    }
}

pub(super) async fn plan(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclarePlan, String> {
    check_local(host, declare)?;
    let identity = declare
        .proposal
        .identity_hash
        .as_deref()
        .ok_or("missing definition identity")?;
    let found = client(host)?
        .find_schema(identity)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(envelope) = &found {
        owned_schema(declare, envelope.schema.clone())?;
    }
    precheck_declare_retention(host, declare, &declare.local_schema_id).await?;
    Ok(DirectDeclarePlan {
        resolution: resolution(declare, found.is_some()),
        register_reason: found.is_none().then_some("independent_definition"),
    })
}

pub(super) async fn load(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclareResolution, String> {
    // Serialize all independent declarations, including a second local name
    // for one definition. Legacy declaration locks retain their own anchors.
    declare
        .anchor_lock
        .hold("independent-app-declarations")
        .await;
    check_local(host, declare)?;
    precheck_declare_retention(host, declare, &declare.local_schema_id).await?;
    let identity = declare
        .proposal
        .identity_hash
        .as_deref()
        .ok_or("missing definition identity")?;
    let client = client(host)?;
    let found = client
        .find_schema(identity)
        .await
        .map_err(|error| error.to_string())?;
    let reused = found.is_some();
    let definition = match found {
        Some(envelope) => envelope.schema,
        None => {
            let mut definition =
                schema_types::DeclarativeSchemaDefinition::from(&declare.catalog_schema);
            // Alternate declarations are node-owned and separately versioned.
            // Older services can supply the same canonical field definitions.
            definition.alternate_keys.clear();
            definition.independent_fields = false;
            definition.field_molecule_uuids = None;
            client
                .add_schema_with_match_source(
                    &definition,
                    HashMap::new(),
                    "independent_definition",
                    None,
                )
                .await
                .map_err(|error| error.to_string())?
                .schema
        }
    };
    let schema = owned_schema(declare, definition)?;
    host.db
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|error| error.to_string())?;
    Ok(resolution(declare, reused))
}
