//! Native local FoldDB construction, at-rest encryption binding, and record file reads.

use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::FoldDB;
use crate::schema::types::{KeyValue, SchemaError};
use crate::security::Ed25519KeyPair;
use crate::storage::config::DatabaseConfig;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::PackNamespacedStore;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Local construction inputs.
#[derive(Default)]
pub struct FoldDbOptions {
    pub offline_maintenance: bool,
    pub search_outbox_inbox: Option<PathBuf>,
    /// Accepted publication stream for an owned shareable named database.
    pub capture_accepted: bool,
}

/// Creates a fully initialized FoldDB instance based on the database configuration.
pub async fn create_fold_db_with_options(
    config: &DatabaseConfig,
    e2e_keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    options: FoldDbOptions,
) -> FoldDbResult<Arc<FoldDB>> {
    let key = e2e_keys.encryption_key();
    let store =
        PackNamespacedStore::open_keyed(&config.path, Some(&key), options.capture_accepted)?;
    let db_ops = DbOperations::from_namespaced_store_with_content_and_field_keys(store, key)
        .await
        .map_err(|e| FoldDbError::Config(e.to_string()))?;
    let fold_db = FoldDB::initialize(Arc::new(db_ops), signer, options)
        .await
        .map_err(|e| map_boot_decrypt_error(e.to_string()))?;
    Ok(Arc::new(fold_db))
}

fn map_boot_decrypt_error(msg: String) -> FoldDbError {
    let lower = msg.to_ascii_lowercase();
    let decrypt_fail = [
        "aead",
        "aes-gcm",
        "decrypt",
        "wrong encryption key",
        "wrong master key",
        "encryption error",
        "key mismatch",
        "undecryptable",
    ]
    .iter()
    .any(|pat| lower.contains(pat));
    if decrypt_fail {
        FoldDbError::Database(format!(
            "cannot decrypt existing store — wrong master key or incompatible build; \
             refusing to start fresh: {msg}"
        ))
    } else {
        FoldDbError::Config(msg)
    }
}

impl FoldDB {
    pub async fn remember_local_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        path: &Path,
        marker: &FileMarker,
    ) -> Result<(), FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                let record = record.ok_or_else(|| {
                    FoldDbError::Database("local file reference has no record".into())
                })?;
                let (stored, ver) = record
                    .file
                    .as_ref()
                    .zip(record.file_version.as_ref())
                    .ok_or_else(|| {
                        FoldDbError::Database("local file reference has no file version".into())
                    })?;
                if stored != marker {
                    return Err(FoldDbError::Database(
                        "local file reference does not match the committed marker".into(),
                    ));
                }
                crate::storage::local_file_ref::remember(
                    &self.db_ops.native_runtime(),
                    &record_file_path(&rk, ver)?,
                    path,
                    &marker.sha256,
                    marker.size_bytes,
                )
                .map_err(FoldDbError::from)
            })
            .await
    }

    async fn read_file_record(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<(String, Option<Arc<StoredRecord>>), FoldDbError> {
        let schema = self.schema_manager.require_schema_metadata(schema_name)?;
        let records = self.db_ops.records();
        let rk = records.record_key(&schema, key)?;
        let rec = records
            .read_records(std::slice::from_ref(&rk))
            .await?
            .pop()
            .flatten();
        Ok((rk, rec))
    }

    pub async fn read_record_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some((marker, ver)) = record
                    .as_ref()
                    .and_then(|r| r.file.as_ref().zip(r.file_version.as_ref()))
                else {
                    return Ok(None);
                };
                let bytes = self
                    .db_ops
                    .records()
                    .format
                    .open_record_file(&rk, ver, marker)
                    .await?
                    .ok_or_else(|| {
                        FoldDbError::InProgress(
                            "the record's file is not on this device and could not be downloaded: no cloud backup is configured on this node".into(),
                        )
                    })?;
                Ok(Some((marker.clone(), bytes)))
            })
            .await
    }

    pub async fn read_record_inline_file(
        &self,
        schema_name: &str,
        key: &KeyValue,
        revision: Option<&str>,
    ) -> Result<Option<(FileMarker, Vec<u8>)>, FoldDbError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some(marker) = record.as_ref().and_then(|r| r.inline_file.as_ref()) else {
                    return Ok(None);
                };
                let bytes = self
                    .db_ops
                    .records()
                    .format
                    .open_inline_file(&rk, marker)
                    .await?
                    .ok_or_else(|| {
                        FoldDbError::Database("record inline file bytes are missing".into())
                    })?;
                Ok(Some((marker.clone(), bytes)))
            })
            .await
    }
}

fn require_revision(
    schema_name: &str,
    key: &KeyValue,
    record: Option<&StoredRecord>,
    revision: Option<&str>,
) -> Result<(), FoldDbError> {
    if let Some(expected) = revision {
        let actual = record.map(|r| r.stamp.revision());
        if actual.as_deref() != Some(expected) {
            return Err(SchemaError::CasConflict {
                schema: schema_name.to_owned(),
                key: key.to_storage_key(),
                expected: expected.to_owned(),
                actual,
            }
            .into());
        }
    }
    Ok(())
}
