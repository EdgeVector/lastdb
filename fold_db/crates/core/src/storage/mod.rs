//! Native local storage, typed helpers, and physical maintenance adapters.

pub mod cloud_backup_plan;
pub(crate) use format::{content_key_proof, retired};
pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub(crate) mod format;
pub(crate) mod local_file_ref;

pub mod error;

pub mod config {
    use std::path::PathBuf;

    #[derive(Clone, Debug)]
    pub struct DatabaseConfig {
        pub path: PathBuf,
    }

    impl DatabaseConfig {
        pub fn local(path: PathBuf) -> Self {
            Self { path }
        }
    }
}

pub mod traits {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum KvMutation {
        Put { key: Vec<u8>, value: Vec<u8> },
        Delete { key: Vec<u8> },
    }

    impl KvMutation {
        pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
            Self::Put {
                key: key.into(),
                value: value.into(),
            }
        }
        pub fn delete(key: impl Into<Vec<u8>>) -> Self {
            Self::Delete { key: key.into() }
        }
        pub fn key(&self) -> &[u8] {
            let (Self::Put { key, .. } | Self::Delete { key }) = self;
            key
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct YieldedScan {
        pub rows: Vec<(Vec<u8>, Vec<u8>)>,
        pub resume: Option<Vec<u8>>,
    }
}

pub(crate) mod current_keys {
    use super::error::{StorageError, StorageResult};
    use super::format::{
        records::{applied_deletes, inline_files},
        CURRENT_FAMILIES, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
    };
    use lastdb_model::field_key_codec::decode_hash_range_suffix;
    use lastdb_model::field_key_id::{is_canonical_field_key_id, ALTERNATE_ENTRY_PREFIX};
    use lastdb_model::record_space::RECORD_PREFIX;

    pub(crate) mod families {
        pub(crate) use crate::storage::format::{RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX};
        use cloud_backup_protocol::{v2::blobs::BlobPath, Id};
        use lastdb_model::{
            hex::hex_lower, portable_mutation::Position, record_space::RECORD_PREFIX,
        };
        use sha2::{Digest, Sha256};

        pub(crate) fn record_file_path(
            record: &str,
            version: &Position,
        ) -> super::StorageResult<String> {
            let space = record
                .strip_prefix(RECORD_PREFIX)
                .and_then(|r| r.split(':').next())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
            Ok(format!(
                "{RECORD_FILE_PREFIX}{space}:{}:{}:{:020}",
                hex_lower(Sha256::digest(record.as_bytes())),
                version.source_stream,
                version.sequence
            ))
        }

        pub(crate) fn record_blob_path(
            database: &Id,
            record: &str,
            version: &Position,
        ) -> super::StorageResult<BlobPath> {
            let key = record_file_path(record, version)?;
            let err =
                || super::StorageError::InvalidOperation("record file key has no blob path".into());
            let [space, record, write_id]: [&str; 3] = key
                .strip_prefix(RECORD_FILE_PREFIX)
                .ok_or_else(err)?
                .splitn(3, ':')
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| err())?;
            let path = BlobPath {
                database: database.clone(),
                space: space.to_owned().try_into().map_err(|_| err())?,
                record: record.to_owned().try_into().map_err(|_| err())?,
                write_id: write_id.to_owned().try_into().map_err(|_| err())?,
            };
            path.validate().map_err(|_| err())?;
            Ok(path)
        }

        pub(crate) fn record_file_uploaded_path(file_path: &[u8]) -> super::StorageResult<Vec<u8>> {
            let suffix = file_path
                .strip_prefix(RECORD_FILE_PREFIX.as_bytes())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| super::unsupported("an uploaded marker needs a record file path"))?;
            Ok([RECORD_FILE_UPLOADED_PREFIX.as_bytes(), suffix].concat())
        }

        pub(crate) fn record_file_space(key: &str) -> Option<(&'static str, &str)> {
            [RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX]
                .into_iter()
                .find_map(|prefix| {
                    let rest = key.strip_prefix(prefix)?;
                    Some((prefix, rest.split_once(':').map_or(rest, |(s, _)| s)))
                })
        }
    }

    pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
        if super::retired::is_journal_key(namespace, key) {
            return Err(StorageError::RetiredFormat(super::retired::JOURNAL_RETIRED));
        }
        if namespace != MAIN_NAMESPACE {
            return Ok(());
        }
        let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
        if matches!(key, CURRENT_KEY_FORMAT_KEY | DELETE_REPLAY_CUTOFF_KEY) {
            return Ok(());
        }
        if !CURRENT_FAMILIES.contains(&key.split([':', '\0']).next().unwrap_or_default()) {
            return Err(unsupported("unknown key family"));
        }
        let rest =
            match applied_deletes::record_key(key).or_else(|| inline_files::record_key(key)) {
                Some(rec) => Some(rec.strip_prefix(RECORD_PREFIX).ok_or_else(|| {
                    unsupported("invalid applied-delete or inline file record key")
                })?),
                None => key
                    .strip_prefix(ALTERNATE_ENTRY_PREFIX)
                    .or_else(|| key.strip_prefix(RECORD_PREFIX)),
            };
        if let Some(rest) = rest {
            let (fk, suffix) = rest
                .split_once(':')
                .ok_or_else(|| unsupported("invalid field key"))?;
            if !is_canonical_field_key_id(fk) || decode_hash_range_suffix(suffix).is_none() {
                return Err(unsupported("invalid field key"));
            }
        }
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
