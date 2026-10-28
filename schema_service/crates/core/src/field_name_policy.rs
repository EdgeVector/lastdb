//! One name rule for canonical publication, declarations, schemas and imports.

use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use schema_types::{FoldDbError, FoldDbResult, Schema};
use std::collections::{BTreeSet, HashMap};

/// Validation rule version; excluded from field identity hashes.
pub const NAME_POLICY_VERSION: u32 = 1;

const APP_ABBREVIATIONS: &[&str] = &[
    "bcl", "bes", "bfc", "bge", "bll", "bln", "blp", "btd", "bts", "clu", "ctd", "eph", "ie",
    "klh", "liv", "nh", "prk", "psi", "rle", "tmb",
];

const SHORT_WORDS: &[&str] = &[
    "at", "by", "in", "of", "on", "or", "to", "id", "ip", "cm", "kg", "km", "mm", "ms", "ns", "ph",
    "us",
];

/// Syntax is the first gate. Meaning, type and concept reuse are checked too.
pub fn validate_new_field_name(name: &str) -> Result<(), String> {
    let words: Vec<_> = name.split('_').collect();
    let reason = if name.len() > 96 || name.is_empty() {
        Some("use 1 through 96 characters")
    } else if words.iter().any(|word| {
        word.is_empty()
            || !word.as_bytes()[0].is_ascii_lowercase()
            || !word
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    }) {
        Some("use lowercase words separated by single underscores")
    } else if words
        .iter()
        .filter(|word| {
            !matches!(
                **word,
                "of" | "to" | "by" | "in" | "at" | "for" | "and" | "the"
            )
        })
        .count()
        < 2
    {
        Some("name both the subject and its property")
    } else if words.iter().any(|word| word.len() == 1) {
        Some("spell out each concept; a one-letter key role is not a concept")
    } else if words
        .iter()
        .any(|word| word.len() == 2 && !SHORT_WORDS.contains(word))
    {
        Some("spell out short names; use standard key terms, connectives or units")
    } else if words.iter().enumerate().any(|(index, word)| {
        APP_ABBREVIATIONS.contains(word)
            && !(*word == "psi"
                && index > 0
                && words[index - 1] == "pressure"
                && index + 1 == words.len())
    }) {
        Some("spell out app abbreviations")
    } else if words
        .iter()
        .any(|word| matches!(*word, "generic" | "entity" | "item"))
    {
        Some("replace the generic subject with the actual concept")
    } else {
        None
    };
    match reason {
        Some(reason) => Err(format!(
            "invalid_canonical_name: '{name}': {reason}; for example 'file_size_bytes'"
        )),
        None => Ok(()),
    }
}

pub(crate) fn invalid_canonical_name(name: &str) -> bool {
    validate_new_field_name(name).is_err()
}

pub fn validate_canonical_field(name: &str, field: &CanonicalField) -> FoldDbResult<()> {
    validate_new_field_name(name).map_err(FoldDbError::Config)?;
    if field.name_policy_version != NAME_POLICY_VERSION {
        return Err(FoldDbError::Config(format!(
            "invalid_canonical_policy: '{name}' requires name policy {NAME_POLICY_VERSION}"
        )));
    }
    if field.description.trim().is_empty() || field.version == 0 {
        return Err(FoldDbError::Config(format!(
            "canonical_field_definition_conflict: '{name}' requires a description and a positive identity version"
        )));
    }
    crate::vocabulary_matching::validate_description(name, field)
}

pub fn validate_schema_vocabulary(
    schema: &Schema,
    vocabulary: &HashMap<String, CanonicalField>,
) -> FoldDbResult<()> {
    for name in schema_field_names(schema) {
        validate_new_field_name(name).map_err(FoldDbError::Config)?;
        let field = vocabulary.get(name).ok_or_else(|| FoldDbError::Config(format!(
            "unknown_canonical_field: '{name}'; submit a precise description and explicit type through schema or field declaration"
        )))?;
        validate_canonical_field(name, field)?;
        if schema
            .field_descriptions
            .get(name)
            .is_none_or(|description| description.trim() != field.description.trim())
        {
            return Err(FoldDbError::Config(format!(
                "canonical_field_definition_conflict: '{name}' must use the registered description"
            )));
        }
        if schema
            .field_types
            .get(name)
            .is_none_or(|value| value != &field.field_type)
            || schema.ref_fields.get(name).is_some_and(|target| {
                field.field_type != schema_types::FieldValueType::SchemaRef(target.clone())
            })
            || schema.field_versions.get(name).copied().unwrap_or(1) != field.version
        {
            return Err(FoldDbError::Config(format!(
                "canonical_field_definition_conflict: '{name}' must use the registered type and identity version"
            )));
        }
    }
    for name in schema_referenced_field_names(schema) {
        validate_vocabulary_reference(name, vocabulary)?;
    }
    Ok(())
}

impl SchemaServiceState {
    pub(super) fn validate_proposal_field_names(&self, schema: &mut Schema) -> FoldDbResult<()> {
        *schema = self.normalized_schema_field_vocabulary(schema)?;
        Ok(())
    }
}

pub(crate) fn fill_schema_field_defaults(
    schema: &mut Schema,
    vocabulary: &HashMap<String, CanonicalField>,
) {
    let names: Vec<_> = schema_field_names(schema).into_iter().cloned().collect();
    for name in &names {
        if let Some(field) = vocabulary.get(name) {
            schema
                .field_descriptions
                .entry(name.clone())
                .or_insert_with(|| field.description.clone());
            schema
                .field_types
                .entry(name.clone())
                .or_insert_with(|| field.field_type.clone());
            schema
                .field_versions
                .entry(name.clone())
                .or_insert(field.version);
        }
    }
}

/// The same name denotes one registered type/version, including owned declarations.
pub fn validate_declared_concept(
    vocabulary: &HashMap<String, CanonicalField>,
    name: &str,
    field_type: &schema_types::FieldValueType,
    description: &str,
    version: u32,
) -> FoldDbResult<()> {
    validate_new_field_name(name).map_err(FoldDbError::Config)?;
    let field = vocabulary.get(name).ok_or_else(|| {
        FoldDbError::Config(format!(
            "unknown_canonical_field: '{name}'; submit a precise description and explicit type"
        ))
    })?;
    validate_canonical_field(name, field)?;
    if field_type != &field.field_type
        || version != field.version
        || description.trim() != field.description.trim()
    {
        return Err(FoldDbError::Config(format!(
            "canonical_field_definition_conflict: '{name}' must use the registered description, type and identity version"
        )));
    }
    Ok(())
}

/// Called inside the durable backend's conditional update, not only a warm cache.
pub fn validate_canonical_insert(
    existing: &HashMap<String, CanonicalField>,
    name: &str,
    field: &CanonicalField,
) -> FoldDbResult<()> {
    validate_canonical_field(name, field)?;
    if existing.get(name).is_some_and(|stored| stored != field) {
        return Err(FoldDbError::Config(format!(
            "canonical_field_conflict: '{name}' already has another definition; reset it explicitly before replacement"
        )));
    }
    for (other_name, other) in existing {
        if other_name != name
            && crate::vocabulary_matching::same_description(&other.description, &field.description)
        {
            return Err(crate::vocabulary_matching::reuse_error(name, other_name));
        }
    }
    Ok(())
}

/// Validate against durable definitions and the earlier entries in the batch.
pub fn canonical_fields_after_insert(
    existing: &HashMap<String, CanonicalField>,
    fields: &[(String, CanonicalField)],
) -> FoldDbResult<HashMap<String, CanonicalField>> {
    let mut candidate = existing.clone();
    for (name, field) in fields {
        validate_canonical_insert(&candidate, name, field)?;
        candidate.insert(name.clone(), field.clone());
    }
    Ok(candidate)
}

/// Include transform outputs and metadata-only names; none can hide a new field.
pub(crate) fn schema_field_names(schema: &Schema) -> BTreeSet<&String> {
    schema
        .fields
        .iter()
        .flatten()
        .chain(schema.transform_fields.iter().flat_map(|map| map.keys()))
        .chain(schema.field_types.keys())
        .chain(schema.ref_fields.keys())
        .chain(schema.field_descriptions.keys())
        .chain(schema.field_classifications.keys())
        .chain(schema.field_data_classifications.keys())
        .chain(schema.field_interest_categories.keys())
        .chain(schema.field_hashes.keys())
        .chain(schema.field_versions.keys())
        .chain(schema.field_declarations.keys())
        .chain(schema.field_mappers.iter().flat_map(|map| map.keys()))
        .chain(
            schema
                .field_molecule_uuids
                .iter()
                .flat_map(|map| map.keys()),
        )
        .collect()
}

fn validate_vocabulary_reference(
    name: &str,
    vocabulary: &HashMap<String, CanonicalField>,
) -> FoldDbResult<()> {
    validate_new_field_name(name).map_err(FoldDbError::Config)?;
    let field = vocabulary.get(name).ok_or_else(|| {
        FoldDbError::Config(format!(
            "canonical_field_definition_conflict: '{name}' in a key or molecule mapper"
        ))
    })?;
    validate_canonical_field(name, field)
}

/// Referenced concepts participate in refresh, but are not local schema fields.
pub(crate) fn schema_referenced_field_names(schema: &Schema) -> BTreeSet<&str> {
    schema
        .field_mappers
        .iter()
        .flat_map(|map| map.values())
        .map(schema_types::FieldMapper::source_field)
        .chain(
            schema
                .key
                .iter()
                .flat_map(|key| [key.hash_field.as_deref(), key.range_field.as_deref()])
                .flatten()
                .map(|path| path.split('.').next().unwrap_or(path)),
        )
        .collect()
}
