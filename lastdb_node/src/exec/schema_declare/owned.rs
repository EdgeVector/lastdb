//! Node-local owned app schema compatibility check and resolution.

use super::*;
use schema_types::FieldValueType;
use std::collections::BTreeSet;

const BREAKING_CHANGE: &str = "this declaration changes an existing field, the key, or the alternate keys of an installed schema; declare a new schema name and copy the records explicitly";

enum Installed {
    Load(Schema),
    Reuse(Schema),
}

impl Installed {
    fn schema(&self) -> &Schema {
        match self {
            Self::Load(s) | Self::Reuse(s) => s,
        }
    }
}

fn retypes_any(stored: Option<&FieldValueType>, proposed: Option<&FieldValueType>) -> bool {
    matches!(stored, None | Some(FieldValueType::Any))
        && proposed.is_some_and(|kind| *kind != FieldValueType::Any)
}

fn field_change(stored: &Schema, proposed: &Schema) -> Option<bool> {
    let stored_fields: BTreeSet<&str> =
        stored.fields.iter().flatten().map(String::as_str).collect();
    let proposed_fields: BTreeSet<&str> = proposed
        .fields
        .iter()
        .flatten()
        .map(String::as_str)
        .collect();
    let equal_defs = stored_fields.intersection(&proposed_fields).all(|f| {
        let (st, pt) = (stored.field_types.get(*f), proposed.field_types.get(*f));
        (st == pt || retypes_any(st, pt))
            && stored.field_descriptions.get(*f) == proposed.field_descriptions.get(*f)
    });
    if !equal_defs {
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

fn check_local(host: &Host, declare: &DirectDeclareProposal) -> Result<Option<Installed>, String> {
    let proposed = &declare.catalog_schema;
    proposed.validate_owned_schema()?;
    let schemas = host
        .records()
        .schema_manager()
        .get_schemas()
        .map_err(|e| e.to_string())?;
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
        let s = stored.clone();
        installed = Some(match field_change(stored, proposed) {
            Some(true) => Installed::Load(s),
            Some(false) => Installed::Reuse(s),
            None => return Err(BREAKING_CHANGE.into()),
        });
    }
    Ok(installed)
}

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
            .filter(|f| {
                installed.field_types.get(*f) == schema.field_types.get(*f)
                    && installed.field_descriptions.get(*f) == schema.field_descriptions.get(*f)
                    && schema
                        .field_versions
                        .get(*f)
                        .is_none_or(|v| installed.field_versions.get(*f) == Some(v))
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
            if let Some(cat) = installed.field_interest_categories.get(&field) {
                schema
                    .field_interest_categories
                    .entry(field)
                    .or_insert_with(|| cat.clone());
            }
        }
        schema.name = installed.name.clone();
    }
    schema.ensure_field_hashes();
    schema.validate().map_err(|e| e.to_string())?;
    schema.validate_owned_schema()?;
    Ok(schema)
}

fn make_resolution(
    runtime: &str,
    declare: &DirectDeclareProposal,
    installed: Option<&Schema>,
) -> DirectDeclareResolution {
    DirectDeclareResolution {
        runtime_schema: runtime.to_string(),
        reused: installed.is_some_and(|s| s.identity_hash == declare.catalog_schema.identity_hash),
    }
}

pub(super) async fn load(
    host: &Host,
    declare: &DirectDeclareProposal,
) -> Result<DirectDeclareResolution, String> {
    let installed = check_local(host, declare)?;
    if let Some(Installed::Reuse(schema)) = &installed {
        precheck_declare_retention(host, declare, &schema.name).await?;
        return Ok(DirectDeclareResolution {
            runtime_schema: schema.name.clone(),
            reused: true,
        });
    }
    let installed = installed.as_ref().map(Installed::schema);
    let runtime = installed.map_or(declare.local_schema_id.as_str(), |s| s.name.as_str());
    precheck_declare_retention(host, declare, runtime).await?;
    let schema = owned_schema(declare, installed)?;
    host.records()
        .schema_manager()
        .load_schema_internal(schema)
        .await
        .map_err(|e| e.to_string())?;
    Ok(make_resolution(runtime, declare, installed))
}
