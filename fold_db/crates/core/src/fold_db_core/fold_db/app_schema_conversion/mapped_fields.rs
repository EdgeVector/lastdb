//! Explicit published field transitions preserve values, key roles, and privacy.

use super::*;

impl OwnedSchemaConversion {
    /// Validate the optional mapped-transition pair before any service lookup.
    pub fn mapping_target(&self) -> Result<Option<&str>, SchemaError> {
        match (&self.target_definition_identity, self.field_map.is_empty()) {
            (None, true) => Ok(None),
            (Some(identity), false)
                if valid_identity(identity)
                    && self.expected_identity.as_deref().is_some_and(valid_identity)
                    && self.field_map.len() <= 128 =>
            {
                Ok(Some(identity))
            }
            _ => Err(invalid("a field transition requires exact source and target identities and a complete field map")),
        }
    }
}

fn valid_identity(identity: &str) -> bool {
    identity.len() == 64
        && identity
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn identity(
    definition: &schema_types::DeclarativeSchemaDefinition,
    expected: &str,
) -> Result<(), SchemaError> {
    if !valid_identity(expected)
        || definition.identity_hash.as_deref() != Some(expected)
        || definition.identity_hash_algo_version.unwrap_or(1)
            > schema_types::IDENTITY_HASH_ALGO_VERSION
    {
        return Err(invalid(
            "the published target identity differs or uses a newer identity algorithm",
        ));
    }
    let mut computed = definition.clone();
    computed.compute_identity_hash();
    if computed.identity_hash.as_deref() != Some(expected) {
        return Err(invalid(
            "the target definition does not reproduce its published identity",
        ));
    }
    Ok(())
}

pub(super) fn definition(
    source: &Schema,
    request: &OwnedSchemaConversion,
) -> Result<Schema, SchemaError> {
    let Some(expected) = request.mapping_target()? else {
        return Ok(source.clone_for_read_shared());
    };
    let definition = request.resolved_definition.as_ref().ok_or_else(|| {
        invalid("the host must resolve the target through Schema Service before conversion")
    })?;
    identity(definition, expected)?;
    if definition
        .transform_fields
        .as_ref()
        .is_some_and(|map| !map.is_empty())
    {
        return Err(invalid(
            "a field transition requires an ordinary published target",
        ));
    }
    let mut target = source.clone_for_read_shared();
    // Match native owned_schema: legacy catalog mappers are value authorities,
    // not field definitions. They never enter this explicit independent target.
    // Every target value comes from the checked local source and total field map.
    target.fields = definition.fields.clone();
    target.field_mappers = None;
    target.key = definition.key.clone();
    target.field_types = definition.field_types.clone();
    target.field_descriptions = definition.field_descriptions.clone();
    target.field_hashes = definition.field_hashes.clone();
    target.field_versions = definition.field_versions.clone();
    target.field_declarations = definition.field_declarations.clone();
    target.field_classifications = definition.field_classifications.clone();
    target.field_data_classifications = definition.field_data_classifications.clone();
    target.field_interest_categories = definition.field_interest_categories.clone();
    target.ref_fields = definition.ref_fields.clone();
    target.identity_hash = definition.identity_hash.clone();
    target.identity_hash_algo_version = definition.identity_hash_algo_version;
    target.owner_app_id = definition.owner_app_id.clone();
    target.descriptive_name = definition.descriptive_name.clone();
    target.schema_type = definition.schema_type.clone();
    target.independent_fields = true;
    target.alternate_keys = request.alternate_keys.clone();
    target.field_molecule_uuids = None;
    target.runtime_fields.clear();
    target.populate_runtime_fields()?;
    validate(source, &target, &request.field_map, expected)?;
    Ok(target)
}

pub(super) fn validate_stored(target: &Target) -> Result<(), SchemaError> {
    match (
        &target.target_definition_identity,
        target.field_map.is_empty(),
    ) {
        (None, true) => Ok(()),
        (Some(expected), false) => {
            validate(&target.source, &target.target, &target.field_map, expected)
        }
        _ => Err(invalid("the stored field transition scope is incomplete")),
    }
}

fn validate(
    source: &Schema,
    target: &Schema,
    fields: &BTreeMap<String, String>,
    expected: &str,
) -> Result<(), SchemaError> {
    identity(target, expected)?;
    let source_fields: BTreeSet<_> = source.fields.iter().flatten().collect();
    let target_fields: BTreeSet<_> = target.fields.iter().flatten().collect();
    if source.name != target.name
        || source.owner_app_id != target.owner_app_id
        || source.descriptive_name != target.descriptive_name
        || source.schema_type != target.schema_type
        || source_fields.len() != fields.len()
        || target_fields.len() != fields.len()
        || fields.keys().collect::<BTreeSet<_>>() != source_fields
        || fields.values().collect::<BTreeSet<_>>() != target_fields
        || source.runtime_fields.keys().collect::<BTreeSet<_>>() != source_fields
        || target.runtime_fields.keys().collect::<BTreeSet<_>>() != target_fields
    {
        return Err(invalid("field conversion requires one app record type and a total one-to-one map of ordinary fields"));
    }
    let mut key = source
        .key
        .clone()
        .ok_or_else(|| invalid("the source primary key is absent"))?;
    key.hash_field = key
        .hash_field
        .as_deref()
        .map(|name| key_field(name, fields))
        .transpose()?;
    key.range_field = key
        .range_field
        .as_deref()
        .map(|name| key_field(name, fields))
        .transpose()?;
    if target.key.as_ref() != Some(&key) {
        return Err(invalid(
            "a mapped field conversion cannot change primary key roles or paths",
        ));
    }
    let references = source
        .ref_fields
        .iter()
        .map(|(field, schema)| Ok((key_field(field, fields)?, schema.clone())))
        .collect::<Result<std::collections::HashMap<_, _>, SchemaError>>()?;
    if target.ref_fields != references {
        return Err(invalid(
            "a field transition cannot change child schema references",
        ));
    }
    for (before, after) in fields {
        if !source.field_types.contains_key(before)
            || source.field_types.get(before) != target.field_types.get(after)
        {
            return Err(invalid(
                "a mapped field conversion cannot coerce field values or key comparison types",
            ));
        }
        privacy(source, target, before, after)?;
    }
    Ok(())
}

pub(super) fn key_field(
    name: &str,
    fields: &BTreeMap<String, String>,
) -> Result<String, SchemaError> {
    if let Some(mapped) = fields.get(name) {
        return Ok(mapped.clone());
    }
    if let Some((root, path)) = name.split_once('.') {
        if let Some(mapped) = fields.get(root) {
            return Ok(format!("{mapped}.{path}"));
        }
    }
    Err(invalid(
        "a primary key path is outside the explicit field map",
    ))
}

pub(super) fn privacy(
    source: &Schema,
    target: &Schema,
    before: &str,
    after: &str,
) -> Result<(), SchemaError> {
    if let Some(old) = source.field_data_classifications.get(before) {
        if target
            .field_data_classifications
            .get(after)
            .is_none_or(|new| {
                new.data_domain != old.data_domain || new.sensitivity_level < old.sensitivity_level
            })
        {
            return Err(invalid(
                "a mapped field conversion cannot remove or lower data classification",
            ));
        }
    }
    let protected = |schema: &Schema, field: &str, label: &str| {
        schema
            .field_classifications
            .get(field)
            .is_some_and(|labels| {
                labels
                    .iter()
                    .any(|value| value.to_ascii_lowercase().replace('-', "_") == label)
            })
    };
    for label in ["secret", "no_index"] {
        if protected(source, before, label) && !protected(target, after, label) {
            return Err(invalid(
                "a mapped field conversion cannot remove a protected search classification",
            ));
        }
    }
    Ok(())
}
