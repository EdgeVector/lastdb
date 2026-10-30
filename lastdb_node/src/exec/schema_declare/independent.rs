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

/// Refuse an older caller before it can load a catalog schema that shadows an
/// owned schema's identity alias. A later load refusal is already too late.
pub(super) fn check_legacy(host: &Host, declare: &DirectDeclareProposal) -> Result<(), String> {
    let schemas = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| error.to_string())?;
    let proposed = &declare.catalog_schema;
    if schemas.values().any(|stored| {
        stored.uses_independent_fields()
            && (stored.name == declare.local_schema_id
                || stored.identity_hash == proposed.identity_hash
                || (stored.owner_app_id == proposed.owner_app_id
                    && stored.descriptive_name == proposed.descriptive_name))
    }) {
        return Err("this app already owns independent values; repeat its independent declaration or use the explicit conversion command".into());
    }
    Ok(())
}

/// A repeat declare cannot convert old data, change its key, or discard a layout.
fn check_local(host: &Host, declare: &DirectDeclareProposal) -> Result<Option<Schema>, String> {
    let proposed = &declare.catalog_schema;
    proposed.validate_alternate_keys()?;
    let schemas = host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| error.to_string())?;
    let mut installed = None;
    for stored in schemas.values() {
        let related = stored.name == declare.local_schema_id
            || stored.identity_hash == proposed.identity_hash
            || (stored.owner_app_id == proposed.owner_app_id
                && stored.descriptive_name == proposed.descriptive_name);
        if !related {
            continue;
        }
        if !stored.uses_independent_fields()
            || stored.identity_hash != proposed.identity_hash
            || stored.owner_app_id != proposed.owner_app_id
            || stored.descriptive_name != proposed.descriptive_name
            || stored.schema_type != proposed.schema_type
            || stored.key != proposed.key
            || stored.alternate_keys != proposed.alternate_keys
            || stored.field_descriptions != proposed.field_descriptions
            || stored.field_types != proposed.field_types
            || installed.is_some()
        {
            return Err("existing app data requires an explicit schema conversion or alternate-key change; declare cannot replace it".into());
        }
        installed = Some(stored.clone_for_read_shared());
    }
    Ok(installed)
}

/// Keep exact field meanings and the primary key. Resolver expansions and
/// catalog value mappers do not authorize any change to an owned app record.
fn owned_schema(
    declare: &DirectDeclareProposal,
    definition: schema_types::DeclarativeSchemaDefinition,
    installed: Option<&Schema>,
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
    schema.field_molecule_uuids = installed.and_then(|schema| schema.field_molecule_uuids.clone());
    if let Some(installed) = installed {
        schema.name = installed.name.clone();
    }
    schema
        .populate_runtime_fields()
        .map_err(|error| error.to_string())?;
    schema.validate_alternate_keys()?;
    Ok(schema)
}

fn resolution(runtime: &str, reused: bool) -> DirectDeclareResolution {
    if reused {
        DirectDeclareResolution::Reuse {
            catalog_hash: runtime.to_string(),
            field_mappers: HashMap::new(),
            confidence: None,
        }
    } else {
        DirectDeclareResolution::Register {
            catalog_hash: runtime.to_string(),
            field_mappers: HashMap::new(),
            replaced_schema: None,
        }
    }
}

pub(super) async fn plan(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclarePlan, String> {
    let installed = check_local(host, declare)?;
    let runtime = installed
        .as_ref()
        .map_or(declare.local_schema_id.as_str(), |schema| {
            schema.name.as_str()
        });
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
        owned_schema(declare, envelope.schema.clone(), installed.as_ref())?;
    }
    precheck_declare_retention(host, declare, runtime).await?;
    Ok(DirectDeclarePlan {
        resolution: resolution(runtime, found.is_some()),
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
    let installed = check_local(host, declare)?;
    let runtime = installed
        .as_ref()
        .map_or(declare.local_schema_id.as_str(), |schema| {
            schema.name.as_str()
        });
    precheck_declare_retention(host, declare, runtime).await?;
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
    let definition = if let Some(envelope) = found {
        envelope.schema
    } else {
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
    };
    let schema = owned_schema(declare, definition, installed.as_ref())?;
    host.db
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|error| error.to_string())?;
    Ok(resolution(runtime, reused))
}
