//! Forward-only canonical name policy. Loading old artifacts never renames them.

use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;
use schema_types::{FoldDbError, FoldDbResult, Schema};
use std::collections::HashMap;

// App implementation abbreviations from the canonical-field inventory. Keep
// standard domain acronyms (URL, JSON, GPS, ISO, ISBN, etc.) out of this list.
// This is a deterministic rejection list, not an English-word recognizer.
const APP_ABBREVIATIONS: &[&str] = &[
    "bcl", "bes", "bfc", "bge", "bll", "bln", "blp", "btd", "bts", "clu", "ctd", "eph", "ie",
    "klh", "liv", "nh", "prk", "psi", "rle", "tmb",
];

/// Delimiters, digits, and camel-case boundaries do not hide an abbreviation.
fn app_abbreviation(name: &str) -> Option<&'static str> {
    let mut tokens = Vec::new();
    for word in name.split(|c: char| !c.is_ascii_alphabetic()) {
        if APP_ABBREVIATIONS
            .iter()
            .any(|abbreviation| word.eq_ignore_ascii_case(abbreviation))
        {
            tokens.push(word);
            continue;
        }
        let bytes = word.as_bytes();
        let mut start = 0;
        for end in 1..=bytes.len() {
            let boundary = end == bytes.len()
                || (bytes[end].is_ascii_uppercase()
                    && (bytes[end - 1].is_ascii_lowercase()
                        || bytes.get(end + 1).is_some_and(u8::is_ascii_lowercase)));
            if boundary {
                let token = word.get(start..end)?;
                tokens.push(token);
                start = end;
            }
        }
    }
    for (index, token) in tokens.iter().enumerate() {
        // PSI is also a standard pressure unit. Its explicit unit suffix is
        // not the PapercutStatusIndex app abbreviation.
        if token.eq_ignore_ascii_case("psi")
            && index + 1 == tokens.len()
            && index > 0
            && tokens[index - 1].eq_ignore_ascii_case("pressure")
        {
            continue;
        }
        if let Some(abbreviation) = APP_ABBREVIATIONS
            .iter()
            .copied()
            .find(|abbreviation| token.eq_ignore_ascii_case(abbreviation))
        {
            return Some(abbreviation);
        }
    }
    None
}

pub(crate) fn uses_app_abbreviation(name: &str) -> bool {
    app_abbreviation(name).is_some()
}

/// A description or an owner namespace cannot justify an abbreviated name.
pub(crate) fn validate_new_field_name(name: &str) -> Result<(), String> {
    if let Some(abbreviation) = app_abbreviation(name) {
        return Err(format!(
            "Field name '{name}' uses the app abbreviation '{abbreviation}'. \
             Spell out the concept and property, for example 'brain_record_list_hash_key'. \
             An app namespace or description does not make an abbreviation acceptable."
        ));
    }
    Ok(())
}

/// Match the identity that `ensure_field_hashes` will retain or compute.
fn field_identity(schema: &Schema, name: &str) -> String {
    schema.field_hashes.get(name).cloned().unwrap_or_else(|| {
        schema_types::compute_field_hash(
            name,
            schema
                .field_descriptions
                .get(name)
                .map_or("", String::as_str),
            schema.get_field_type(name),
            schema.field_versions.get(name).copied().unwrap_or(1),
        )
    })
}

impl SchemaServiceState {
    /// Grandfather only an unchanged field of the same stored schema family.
    /// A new app or descriptive name cannot reuse that exception, nor can a
    /// changed type, version, or description create another abbreviated field.
    pub(super) fn validate_proposal_field_names(&self, schema: &Schema) -> FoldDbResult<()> {
        let abbreviated: Vec<_> = schema
            .fields
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|name| uses_app_abbreviation(name))
            .collect();
        if abbreviated.is_empty() {
            return Ok(());
        }

        let legacy_types: HashMap<_, _> = {
            let canonical = read_lock(&self.canonical_fields, "canonical_fields")?;
            abbreviated
                .iter()
                .filter_map(|name| {
                    canonical
                        .get(*name)
                        .map(|field| (name.as_str(), field.field_type.clone()))
                })
                .collect()
        };
        let schemas = read_lock(&self.schemas, "schemas")?;
        for name in abbreviated {
            let identity = field_identity(schema, name);
            let inherited_type = schema
                .field_types
                .get(name)
                .or_else(|| legacy_types.get(name.as_str()));
            let unchanged = legacy_types.contains_key(name.as_str())
                && schemas.values().any(|existing| {
                    existing.owner_app_id == schema.owner_app_id
                        && existing.descriptive_name == schema.descriptive_name
                        && existing
                            .fields
                            .as_deref()
                            .unwrap_or_default()
                            .contains(name)
                        && inherited_type
                            .is_some_and(|field_type| existing.get_field_type(name) == field_type)
                        && existing.field_versions.get(name).copied().unwrap_or(1)
                            == schema.field_versions.get(name).copied().unwrap_or(1)
                        && existing.field_descriptions.get(name)
                            == schema.field_descriptions.get(name)
                        && field_identity(existing, name) == identity
                });
            if !unchanged {
                validate_new_field_name(name).map_err(FoldDbError::Config)?;
            }
        }
        Ok(())
    }
}
