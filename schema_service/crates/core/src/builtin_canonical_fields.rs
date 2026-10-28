//! Curated starting vocabulary. App proposals never extend this list implicitly.

use crate::field_name_policy::NAME_POLICY_VERSION;
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use schema_types::{DataClassification, FieldValueType, FoldDbError, FoldDbResult};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    name: String,
    description: String,
    field_type: FieldValueType,
    sensitivity: u8,
    domain: String,
    interest_category: Option<String>,
}

/// Build and validate the entire source vocabulary before any seed write.
pub fn definitions() -> FoldDbResult<Vec<(String, CanonicalField)>> {
    let entries: Vec<Entry> =
        serde_json::from_str(include_str!("builtin_canonical_fields.json"))
            .map_err(|error| FoldDbError::Config(format!("invalid canonical seed: {error}")))?;
    entries
        .into_iter()
        .map(|entry| {
            let classification = DataClassification::new(entry.sensitivity, &entry.domain)
                .map_err(|error| {
                    FoldDbError::Config(format!("invalid canonical seed classification: {error}"))
                })?;
            let field = CanonicalField {
                description: entry.description,
                field_type: entry.field_type,
                version: 1,
                name_policy_version: NAME_POLICY_VERSION,
                classification: Some(classification),
                interest_category: entry.interest_category,
            };
            crate::field_name_policy::validate_approved_field(&entry.name, &field)?;
            Ok((entry.name, field))
        })
        .collect()
}

/// One batch, with complete validation before the first durable write.
pub async fn seed(state: &SchemaServiceState) -> FoldDbResult<()> {
    let fields = definitions()?;
    state.add_canonical_fields(&fields).await
}
