//! Separate schema definitions during a rollout; app identities stay shared.
use super::S3BlobPersistence;
use schema_types::{FoldDbError, FoldDbResult};
use std::borrow::Cow;

pub const DEFINITION_NAMESPACE_ENV: &str = "SCHEMA_DEFINITION_NAMESPACE";

impl S3BlobPersistence {
    /// Bind the backend once. An absent setting preserves the existing layout.
    /// Never fall back to legacy definitions when the selected namespace is empty.
    pub fn with_definition_namespace_from_env(mut self) -> FoldDbResult<Self> {
        self.definition_namespace = match std::env::var(DEFINITION_NAMESPACE_ENV) {
            Ok(value) => {
                if value.len() <= "canonical-".len()
                    || value.len() > 64
                    || !value.starts_with("canonical-")
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
                {
                    return Err(FoldDbError::Config(format!(
                        "{DEFINITION_NAMESPACE_ENV} requires canonical- followed by lowercase letters, digits or hyphens (at most 64 bytes)"
                    )));
                }
                if !self.embeddings_table.ends_with(&format!("-{value}")) {
                    return Err(FoldDbError::Config(format!(
                        "SCHEMA_EMBEDDINGS_TABLE must end in '-{value}' for an isolated definition namespace"
                    )));
                }
                Some(value)
            }
            Err(std::env::VarError::NotPresent) => None,
            Err(_) => {
                return Err(FoldDbError::Config(format!(
                    "{DEFINITION_NAMESPACE_ENV} is not valid UTF-8"
                )))
            }
        };
        Ok(self)
    }

    /// The app registry and quota state retain their common identities.
    /// A key returned by a namespaced list already has the full prefix.
    pub(super) fn definition_key<'a>(&self, key: &'a str) -> Cow<'a, str> {
        let Some(namespace) = &self.definition_namespace else {
            return Cow::Borrowed(key);
        };
        let is_definition = matches!(
            key,
            "definitions.json"
                | "schemas.json"
                | "canonical_fields.json"
                | "near_misses.json"
                | "declared_fields.json"
        ) || key.starts_with("releases/")
            || key.starts_with("channels/");
        if is_definition {
            Cow::Owned(format!("{namespace}/{key}"))
        } else {
            Cow::Borrowed(key)
        }
    }
}
