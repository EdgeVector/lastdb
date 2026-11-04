//! One owned app schema. The node declares it locally; Schema Service gives
//! advice only and is not asked (`design-lastdb-advisory-schema-service`).

use super::*;
use schema_types::FieldValueType;
use std::collections::BTreeSet;

pub(super) struct DirectDeclarePlan {
    pub(super) resolution: DirectDeclareResolution,
    pub(super) register_reason: Option<&'static str>,
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

/// Until `Any` is deleted (`decision-2026-11-02-field-types-no-any`), a repeat
/// declare may give an `Any` or untyped field a concrete type in place. Old
/// values are not re-checked; the next write of the field must match.
fn retypes_any(stored: Option<&FieldValueType>, proposed: Option<&FieldValueType>) -> bool {
    matches!(stored, None | Some(FieldValueType::Any))
        && proposed.is_some_and(|kind| *kind != FieldValueType::Any)
}

/// `Some(true)` when `proposed` adds fields or equals `stored`, `Some(false)`
/// when it declares a subset. A field in both must keep its type and
/// description, except that a non-key field may leave `Any`. `None` for any
/// other change.
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
        let stored_type = stored.field_types.get(*field);
        let proposed_type = proposed.field_types.get(*field);
        (stored_type == proposed_type || retypes_any(stored_type, proposed_type))
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
            || stored.record_keys != proposed.record_keys
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

/// The schema to load, built on the node from the declaration alone. Field
/// versions and classifications come from the declaration. A field whose type,
/// description, and version equal the installed field keeps its installed
/// hash and fills missing classifications from it; every other field hash is
/// computed here with the catalog's `compute_field_hash`. The installed schema
/// keeps its runtime name and field keys.
fn owned_schema(
    declare: &DirectDeclareProposal,
    installed: Option<&Schema>,
) -> Result<Schema, String> {
    let mut schema = declare.catalog_schema.clone();
    schema.reset_field_hashes();
    if let Some(installed) = installed {
        let unchanged: Vec<String> = schema
            .fields
            .iter()
            .flatten()
            .filter(|field| {
                installed.field_types.get(*field) == schema.field_types.get(*field)
                    && installed.field_descriptions.get(*field)
                        == schema.field_descriptions.get(*field)
                    && schema
                        .field_versions
                        .get(*field)
                        .is_none_or(|version| installed.field_versions.get(*field) == Some(version))
            })
            .cloned()
            .collect();
        for field in unchanged {
            if let Some(hash) = installed.field_hashes.get(&field) {
                schema.field_hashes.insert(field.clone(), hash.clone());
            }
            if let Some(version) = installed.field_versions.get(&field) {
                schema.field_versions.insert(field.clone(), *version);
            }
            if let Some(id) = installed.field_declarations.get(&field) {
                schema
                    .field_declarations
                    .entry(field.clone())
                    .or_insert_with(|| id.clone());
            }
            if let Some(tags) = installed.field_classifications.get(&field) {
                schema
                    .field_classifications
                    .entry(field.clone())
                    .or_insert_with(|| tags.clone());
            }
            if let Some(label) = installed.field_data_classifications.get(&field) {
                schema
                    .field_data_classifications
                    .entry(field.clone())
                    .or_insert_with(|| label.clone());
            }
            if let Some(category) = installed.field_interest_categories.get(&field) {
                schema
                    .field_interest_categories
                    .entry(field)
                    .or_insert_with(|| category.clone());
            }
        }
        schema.field_key_ids = installed.field_key_ids.clone();
        schema.name = installed.name.clone();
    }
    schema.ensure_field_hashes();
    schema
        .populate_runtime_fields()
        .map_err(|error| error.to_string())?;
    schema.validate_owned_schema()?;
    Ok(schema)
}

/// `reuse` when the installed schema has this identity, `register` otherwise.
fn resolution(
    runtime: &str,
    declare: &DirectDeclareProposal,
    installed: Option<&Schema>,
) -> DirectDeclareResolution {
    DirectDeclareResolution {
        runtime_schema: runtime.to_string(),
        reused: installed
            .is_some_and(|schema| schema.identity_hash == declare.catalog_schema.identity_hash),
    }
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
    owned_schema(declare, installed)?;
    precheck_declare_retention(host, declare, runtime).await?;
    let resolution = resolution(runtime, declare, installed);
    Ok(DirectDeclarePlan {
        register_reason: (!resolution.reused).then_some("independent_definition"),
        resolution,
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
    let schema = owned_schema(declare, installed)?;
    host.db
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|error| error.to_string())?;
    Ok(resolution(runtime, declare, installed))
}
