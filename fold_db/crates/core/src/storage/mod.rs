//! Native local storage, typed helpers, and physical maintenance adapters.

pub mod cloud_backup_plan;
pub(crate) mod content_key_proof;
pub(crate) mod current_keys;
pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub mod error;
pub(crate) mod format;
pub(crate) mod local_file_ref;

pub mod config {
    use std::path::PathBuf;

    /// Native local storage configuration. Callers build it in code; no
    /// configuration file decodes into it.
    #[derive(Clone, Debug)]
    pub struct DatabaseConfig {
        /// Path to the local database directory (Last Store root)
        pub path: PathBuf,
    }

    impl DatabaseConfig {
        /// Create a local-only config
        pub fn local(path: PathBuf) -> Self {
            Self { path }
        }
    }
}

pub mod traits {
    /// One ordered mutation in a mixed durable batch.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum KvMutation {
        /// Insert or replace one raw key/value row.
        Put { key: Vec<u8>, value: Vec<u8> },
        /// Remove one raw key if it exists.
        Delete { key: Vec<u8> },
    }

    impl KvMutation {
        /// Build a put mutation.
        pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
            Self::Put {
                key: key.into(),
                value: value.into(),
            }
        }

        /// Build a delete mutation.
        pub fn delete(key: impl Into<Vec<u8>>) -> Self {
            Self::Delete { key: key.into() }
        }

        /// Return the key affected by this mutation.
        pub fn key(&self) -> &[u8] {
            match self {
                Self::Put { key, .. } | Self::Delete { key } => key,
            }
        }
    }

    /// One read of a range that names where the next read continues.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct YieldedScan {
        /// The rows read, in key order.
        pub rows: Vec<(Vec<u8>, Vec<u8>)>,
        /// The last key this read examined (`None` means the range ended).
        pub resume: Option<Vec<u8>>,
    }
}

pub use config::DatabaseConfig;
pub use error::StorageError;
pub use traits::KvMutation;
