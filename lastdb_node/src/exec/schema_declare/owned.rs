//! One owned app schema. Schema Service supplies definitions, never value aliases.

use super::*;
use std::collections::BTreeSet;

pub(super) struct DirectDeclarePlan {
    pub(super) resolution: DirectDeclareResolution,
    pub(super) register_reason: Option<&'static str>,
}

fn client(host: &Host) -> Result<schema_service_client::SchemaServiceClient, String> {
    crate::schema_resolver_host::schema_service_client_with_node_identity(
        &host.home,
        schema_service_client::SchemaServiceClient::new(
            &folddb_profile::endpoints::schema_service_url(),
        ),
    )
}

const BREAKING_CHANGE: &str = "this declaration changes an existing field, the key, or the alternate keys of an installed schema; declare a new schema name and copy the records explicitly";

/// How a repeat declaration relates to the installed schema.
enum Installed {
    /// The same definition, or one that adds fields. It loads in place: the
    /// existing fields keep their field keys and old records read a new field
    /// as absent.
    Load(Schema),
    /// An older app declares a subset of the installed fields with equal
    /// definitions. It reuses the installed schema and loads nothing.
    Reuse(Schema),
}

impl Installed {
    fn schema(&self) -> &Schema {
        match self {
            Self::Load(schema) | Self::Reuse(schema) => schema,
        }
    }
}

/// `Some(true)` when `proposed` adds fields or equals `stored`, `Some(false)`
/// when it declares a subset. A field in both must keep its type and
/// description. `None` for any other change.
fn field_change(stored: &Schema, proposed: &Schema) -> Option<bool> {
    let stored_fields: BTreeSet<&str> =
        stored.fields.iter().flatten().map(String::as_str).collect();
    let proposed_fields: BTreeSet<&str> = proposed
        .fields
        .iter()
        .flatten()
        .map(String::as_str)
        .collect();
    let equal_definitions = stored_fields.intersection(&proposed_fields).all(|field| {
        stored.field_types.get(*field) == proposed.field_types.get(*field)
            && stored.field_descriptions.get(*field) == proposed.field_descriptions.get(*field)
    });
    if !equal_definitions {
        return None;
    }
    if stored_fields == proposed_fields {
        return (stored.identity_hash == proposed.identity_hash).then_some(true);
    }
    if stored_fields.is_subset(&proposed_fields) {
        return Some(true);
    }
    proposed_fields.is_subset(&stored_fields).then_some(false)
}

/// A repeat declare can add fields. It cannot convert old data, change a field,
/// change the key, or discard a layout.
fn check_local(host: &Host, declare: &DirectDeclareProposal) -> Result<Option<Installed>, String> {
    let proposed = &declare.catalog_schema;
    proposed.validate_owned_schema()?;
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
        if stored.owner_app_id != proposed.owner_app_id
            || stored.descriptive_name != proposed.descriptive_name
            || stored.schema_type != proposed.schema_type
            || stored.key != proposed.key
            || stored.alternate_keys != proposed.alternate_keys
            || installed.is_some()
        {
            return Err(BREAKING_CHANGE.into());
        }
        let schema = stored.clone_for_read_shared();
        installed = Some(match field_change(stored, proposed) {
            Some(true) => Installed::Load(schema),
            Some(false) => Installed::Reuse(schema),
            None => return Err(BREAKING_CHANGE.into()),
        });
    }
    Ok(installed)
}

/// The declaration reuses the installed schema of an older field set.
fn reuse(installed: &Schema) -> DirectDeclareResolution {
    DirectDeclareResolution {
        runtime_schema: installed.name.clone(),
        reused: true,
    }
}

/// Keep exact field meanings and the primary key. Resolver expansions do not
/// authorize any change to an owned app record.
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
    // Only definition metadata crosses this boundary. No service field key ID,
    // predecessor, transform, or alternate layout owns local values.
    schema.field_types = definition.field_types;
    schema.field_hashes = definition.field_hashes;
    schema.field_versions = definition.field_versions;
    schema.field_declarations = definition.field_declarations;
    schema.field_classifications = definition.field_classifications;
    schema.field_data_classifications = definition.field_data_classifications;
    schema.field_interest_categories = definition.field_interest_categories;
    schema.field_key_ids = installed.and_then(|schema| schema.field_key_ids.clone());
    if let Some(installed) = installed {
        schema.name = installed.name.clone();
    }
    schema
        .populate_runtime_fields()
        .map_err(|error| error.to_string())?;
    schema.validate_owned_schema()?;
    Ok(schema)
}

pub(super) async fn plan(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclarePlan, String> {
    let installed = check_local(host, declare)?;
    if let Some(Installed::Reuse(schema)) = &installed {
        precheck_declare_retention(host, declare, &schema.name).await?;
        return Ok(DirectDeclarePlan {
            resolution: reuse(schema),
            register_reason: None,
        });
    }
    let installed = installed.as_ref().map(Installed::schema);
    let runtime = installed.map_or(declare.local_schema_id.as_str(), |schema| {
        schema.name.as_str()
    });
    let identity = declare
        .catalog_schema
        .identity_hash
        .as_deref()
        .ok_or("missing definition identity")?;
    let found = client(host)?
        .find_schema(identity)
        .await
        .map_err(|error| error.to_string())?;
    if let Some(envelope) = &found {
        owned_schema(declare, envelope.schema.clone(), installed)?;
    }
    precheck_declare_retention(host, declare, runtime).await?;
    Ok(DirectDeclarePlan {
        resolution: DirectDeclareResolution {
            runtime_schema: runtime.to_string(),
            reused: found.is_some(),
        },
        register_reason: found.is_none().then_some("independent_definition"),
    })
}

pub(super) async fn load(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclareResolution, String> {
    let installed = check_local(host, declare)?;
    if let Some(Installed::Reuse(schema)) = &installed {
        precheck_declare_retention(host, declare, &schema.name).await?;
        return Ok(reuse(schema));
    }
    let installed = installed.as_ref().map(Installed::schema);
    let runtime = installed.map_or(declare.local_schema_id.as_str(), |schema| {
        schema.name.as_str()
    });
    precheck_declare_retention(host, declare, runtime).await?;
    let identity = declare
        .catalog_schema
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
        // Alternate declarations are node-owned. Older services can supply
        // the same canonical field definitions.
        definition.alternate_keys.clear();
        definition.field_key_ids = None;
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
    let schema = owned_schema(declare, definition, installed)?;
    host.db
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|error| error.to_string())?;
    Ok(DirectDeclareResolution {
        runtime_schema: runtime.to_string(),
        reused,
    })
}
