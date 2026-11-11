//! Native local storage, typed helpers, and physical maintenance adapters.

pub mod cloud_backup_plan;
pub(crate) use format::content_key_proof;
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

pub(crate) mod current_keys {
    use super::error::{StorageError, StorageResult};
    use super::format::{
        records::{applied_deletes, inline_files},
        CURRENT_FAMILIES, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
    };

    pub(crate) mod families {
        pub(crate) use crate::storage::format::{RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX};
        use lastdb_model::portable_mutation::Position;

        /// One immutable file version owned by a single record.
        pub(crate) fn record_file_path(
            record: &str,
            version: &Position,
        ) -> super::StorageResult<String> {
            use sha2::Digest as _;
            let space = record
                .strip_prefix(lastdb_model::record_space::RECORD_PREFIX)
                .and_then(|rest| rest.split(':').next())
                .filter(|space| !space.is_empty())
                .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
            let digest = lastdb_model::hex::hex_lower(sha2::Sha256::digest(record.as_bytes()));
            Ok(format!(
                "{RECORD_FILE_PREFIX}{space}:{digest}:{}:{:020}",
                version.source_stream, version.sequence
            ))
        }

        /// The cloud location of one record file version under the database identity.
        pub(crate) fn record_blob_path(
            database: &cloud_backup_protocol::Id,
            record: &str,
            version: &Position,
        ) -> super::StorageResult<cloud_backup_protocol::v2::blobs::BlobPath> {
            let path = record_file_path(record, version)?;
            let invalid = || super::unsupported("a record file path has no cloud blob path");
            let mut parts = path
                .strip_prefix(RECORD_FILE_PREFIX)
                .ok_or_else(invalid)?
                .splitn(3, ':');
            let (Some(space), Some(digest), Some(write_id)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Err(invalid());
            };
            let blob = cloud_backup_protocol::v2::blobs::BlobPath {
                database: database.clone(),
                space: space.to_owned().try_into().map_err(|_| invalid())?,
                record: digest.to_owned().try_into().map_err(|_| invalid())?,
                write_id: write_id.to_owned().try_into().map_err(|_| invalid())?,
            };
            blob.validate().map_err(|_| invalid())?;
            Ok(blob)
        }

        /// The "already in the cloud" marker of the record file row at `file_path`.
        pub(crate) fn record_file_uploaded_path(file_path: &[u8]) -> super::StorageResult<Vec<u8>> {
            let suffix = file_path
                .strip_prefix(RECORD_FILE_PREFIX.as_bytes())
                .filter(|suffix| !suffix.is_empty())
                .ok_or_else(|| super::unsupported("an uploaded marker needs a record file path"))?;
            Ok([RECORD_FILE_UPLOADED_PREFIX.as_bytes(), suffix].concat())
        }

        /// The record file family of a `cas_blobs` key: its family prefix and its schema space.
        pub(crate) fn record_file_space(key: &str) -> Option<(&'static str, &str)> {
            [RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX]
                .into_iter()
                .find_map(|prefix| {
                    let rest = key.strip_prefix(prefix)?;
                    Some((
                        prefix,
                        rest.split_once(':').map_or(rest, |(space, _)| space),
                    ))
                })
        }
    }

    /// Validate a `main` key family and coordinate.
    pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
        if namespace != MAIN_NAMESPACE {
            return Ok(());
        }
        let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
        if matches!(key, CURRENT_KEY_FORMAT_KEY | DELETE_REPLAY_CUTOFF_KEY) {
            return Ok(());
        }
        let family = key.split([':', '\0']).next().unwrap_or_default();
        if !CURRENT_FAMILIES.contains(&family) {
            return Err(unsupported("unknown key family"));
        }
        if let Some(record) =
            applied_deletes::record_key(key).or_else(|| inline_files::record_key(key))
        {
            let rest = record
                .strip_prefix(lastdb_model::record_space::RECORD_PREFIX)
                .ok_or_else(|| unsupported("invalid applied-delete or inline file record key"))?;
            validate_record(rest)?;
        } else if let Some(rest) = key
            .strip_prefix(lastdb_model::field_key_id::ALTERNATE_ENTRY_PREFIX)
            .or_else(|| key.strip_prefix(lastdb_model::record_space::RECORD_PREFIX))
        {
            validate_record(rest)?;
        }
        Ok(())
    }

    fn validate_record(rest: &str) -> StorageResult<()> {
        let (field_key, suffix) = rest
            .split_once(':')
            .ok_or_else(|| unsupported("invalid field key"))?;
        if !lastdb_model::field_key_id::is_canonical_field_key_id(field_key) {
            return Err(unsupported("invalid field key identity"));
        }
        lastdb_model::field_key_codec::decode_hash_range_suffix(suffix)
            .ok_or_else(|| unsupported("invalid field key layout"))?;
        Ok(())
    }

    fn unsupported(reason: &str) -> StorageError {
        StorageError::InvalidOperation(format!(
            "unsupported key format: {reason}; start with a fresh home"
        ))
    }
}

pub use config::DatabaseConfig;
pub use error::StorageError;
pub use traits::KvMutation;
