//! Advisory Schema Service core: curated canonical vocabulary and deterministic
//! draft schema advice (`GET /v1/fields/vocabulary`, `POST /v1/advice/schema`).

pub mod advice;
pub mod builtin_canonical_fields;
pub mod field_name_policy;
pub mod types;
mod vocabulary_matching;

use schema_types::FoldDbResult;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

pub use types::{CanonicalField, DataClassification, MAX_SENSITIVITY_LEVEL};

/// Immutable in-memory advisory state constructed from the curated canonical vocabulary.
#[derive(Debug, Clone)]
pub struct SchemaServiceState {
    pub(crate) canonical_fields: HashMap<String, CanonicalField>,
    vocabulary_json: Value,
}

impl SchemaServiceState {
    /// Load and validate the built-in canonical field vocabulary.
    ///
    /// # Errors
    /// Returns an error if the embedded canonical seed violates vocabulary or naming rules.
    pub fn from_builtin() -> FoldDbResult<Self> {
        let definitions = builtin_canonical_fields::definitions()?;
        let canonical_fields =
            field_name_policy::canonical_fields_after_insert(&HashMap::new(), &definitions)?;
        let sorted: BTreeMap<&String, &CanonicalField> = canonical_fields.iter().collect();
        let vocabulary_json = json!({
            "name_policy_version": field_name_policy::NAME_POLICY_VERSION,
            "fields": sorted,
        });
        Ok(Self {
            canonical_fields,
            vocabulary_json,
        })
    }

    /// Public standard vocabulary document (`GET /v1/fields/vocabulary`).
    #[must_use]
    pub const fn canonical_vocabulary(&self) -> &Value {
        &self.vocabulary_json
    }
}

/// Standard single-field JSON error envelope.
#[must_use]
pub fn error_only_body(message: &str) -> Value {
    json!({ "error": message })
}
