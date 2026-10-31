//! Preserve typed storage failures at the schema boundary.
use super::SchemaError;

impl SchemaError {
    /// Stored bytes and storage requests are server failures. Preserve typed
    /// limits and access denials rather than flattening them into strings.
    pub fn read_failure(error: Self) -> Self {
        match error {
            Self::InvalidData(detail) | Self::InvalidField(detail) => Self::StorageRead { detail },
            other => other,
        }
    }
}
