//! Preserve typed storage failures at the schema boundary.
use super::SchemaError;

impl SchemaError {
    /// Stored bytes and storage requests are server failures. Preserve typed
    /// limits and access denials rather than flattening them into strings.
    pub(crate) fn read_failure(error: Self) -> Self {
        match error {
            Self::InvalidData(detail) | Self::InvalidField(detail) => Self::StorageRead { detail },
            other => other,
        }
    }
}

impl From<crate::storage::StorageError> for SchemaError {
    fn from(error: crate::storage::StorageError) -> Self {
        match error {
            crate::storage::StorageError::ReadResourceLimit { detail } => {
                Self::ReadResourceLimit { detail }
            }
            crate::storage::StorageError::WriteResourceLimit { detail }
            | crate::storage::StorageError::MutationLimit { detail } => {
                Self::WriteResourceLimit { detail }
            }
            crate::storage::StorageError::CatalogMembershipDenied {
                db_locator,
                schema_name,
            } => Self::CatalogMembershipDenied {
                db_locator,
                schema_name,
            },
            // A full disk keeps its identity across the boundary. Folding it
            // into `InvalidData` here is what made every storage path report a
            // host condition as a malformed request.
            crate::storage::StorageError::StorageFull { detail } => Self::StorageFull { detail },
            // `ENOSPC` also reaches this boundary as a plain IO error, from the
            // paths that build a `StorageError` straight off `std::io::Error`
            // instead of through a backend's `map_error`. It is the same host
            // condition either way, so the caller's status must not depend on
            // which path happened to report it.
            crate::storage::StorageError::IoError(io)
                if crate::storage::StorageError::is_storage_full_io(&io) =>
            {
                Self::StorageFull {
                    detail: io.to_string(),
                }
            }
            other => Self::InvalidData(other.to_string()),
        }
    }
}
