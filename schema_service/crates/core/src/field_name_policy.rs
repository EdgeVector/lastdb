//! One name rule for canonical publication, declarations, schemas and imports.

use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use schema_types::{FoldDbError, FoldDbResult, Schema};
use std::collections::{BTreeSet, HashMap};

/// Administrative approval metadata; excluded from field identity hashes.
pub const NAME_POLICY_VERSION: u32 = 1;

const APP_ABBREVIATIONS: &[&str] = &[
    "bcl", "bes", "bfc", "bge", "bll", "bln", "blp", "btd", "bts", "clu", "ctd", "eph", "ie",
    "klh", "liv", "nh", "prk", "psi", "rle", "tmb",
];

/// Syntax is only the first gate. Only operator-approved vocabulary entries
/// can pass schema publication; a long invented name grants no approval.
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

pub fn validate_approved_field(name: &str, field: &CanonicalField) -> FoldDbResult<()> {
    validate_new_field_name(name).map_err(FoldDbError::Config)?;
    if field.name_policy_version != NAME_POLICY_VERSION {
        return Err(FoldDbError::Config(format!(
            "unapproved_canonical_field: '{name}' requires operator approval for name policy {NAME_POLICY_VERSION}"
        )));
    }
    if field.description.trim().is_empty() || field.version == 0 {
        return Err(FoldDbError::Config(format!(
            "unapproved_canonical_field: '{name}' requires a description and a positive identity version"
        )));
    }
    Ok(())
}

pub fn validate_schema_vocabulary(
    schema: &Schema,
    vocabulary: &HashMap<String, CanonicalField>,
) -> FoldDbResult<()> {
    for name in schema_field_names(schema) {
        validate_new_field_name(name).map_err(FoldDbError::Config)?;
        let field = vocabulary.get(name).ok_or_else(|| FoldDbError::Config(format!(
            "unapproved_canonical_field: '{name}'; reuse an approved concept or request operator publication"
        )))?;
        validate_approved_field(name, field)?;
        if schema
            .field_descriptions
            .get(name)
            .is_some_and(|description| description.trim() != field.description.trim())
        {
            return Err(FoldDbError::Config(format!(
                "unapproved_canonical_field: '{name}' must use the approved description"
            )));
        }
        if schema
            .field_types
            .get(name)
            .is_some_and(|value| value != &field.field_type)
            || schema.ref_fields.get(name).is_some_and(|target| {
                field.field_type != schema_types::FieldValueType::SchemaRef(target.clone())
            })
            || schema.field_versions.get(name).copied().unwrap_or(1) != field.version
        {
            return Err(FoldDbError::Config(format!(
                "unapproved_canonical_field: '{name}' must use the approved type and identity version"
            )));
        }
    }
    Ok(())
}

impl SchemaServiceState {
    pub(super) fn validate_proposal_field_names(&self, schema: &mut Schema) -> FoldDbResult<()> {
        let vocabulary = read_lock(&self.canonical_fields, "canonical_fields")?;
        validate_schema_vocabulary(schema, &vocabulary)?;
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
        Ok(())
    }
}

/// The same name denotes one approved type/version, including owned declarations.
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
            "unapproved_canonical_field: '{name}'; request operator publication first"
        ))
    })?;
    validate_approved_field(name, field)?;
    if field_type != &field.field_type
        || version != field.version
        || description.trim() != field.description.trim()
    {
        return Err(FoldDbError::Config(format!(
            "unapproved_canonical_field: '{name}' must use the approved description, type and identity version"
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
    validate_approved_field(name, field)?;
    if existing.get(name).is_some_and(|stored| stored != field) {
        return Err(FoldDbError::Config(format!(
            "canonical_field_conflict: '{name}' already has another definition; reset it explicitly before replacement"
        )));
    }
    Ok(())
}

/// Include transform outputs and metadata-only names; none can hide a new field.
fn schema_field_names(schema: &Schema) -> BTreeSet<&String> {
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
