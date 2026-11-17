//! FoldDB Core — embedded database coordinator, file readers, and lifecycle.

use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

use crate::db_operations::{DbOperations, SchemaRecordKey, SchemaRecordKeysReport};
use crate::error::FoldDbError;
use crate::schema::types::{KeyValue, SchemaError};
use crate::schema::SchemaCore;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::StorageError;

pub mod factory {
    //! Native local FoldDB construction and at-rest encryption binding.

    use crate::crypto::ContentCipherKey;
    use crate::db_operations::DbOperations;
    use crate::error::{FoldDbError, FoldDbResult};
    use crate::fold_db_core::FoldDB;
    use crate::security::Ed25519KeyPair;
    use crate::storage::config::DatabaseConfig;
    use crate::storage::disk_pack::PackNamespacedStore;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Local construction inputs.
    #[derive(Default)]
    pub struct FoldDbOptions {
        pub offline_maintenance: bool,
        pub search_outbox_inbox: Option<PathBuf>,
        /// Capture the accepted publication stream for an owned database.
        pub capture_accepted: bool,
        /// Preserve the original native root before a personal home starts cloud capture.
        pub capture_initial_snapshot: bool,
        /// Remember the publisher's mutation identity beside each whole-database share record.
        pub track_shared_record_revisions: bool,
    }

    /// Creates a fully initialized FoldDB instance based on the database configuration.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "host and share callers pass Arc<Ed25519KeyPair>"
    )]
    pub async fn create_fold_db_with_options(
        config: &DatabaseConfig,
        e2e_keys: &ContentCipherKey,
        signer: Arc<Ed25519KeyPair>,
        options: FoldDbOptions,
    ) -> FoldDbResult<Arc<FoldDB>> {
        let key = e2e_keys.encryption_key();
        let store = PackNamespacedStore::open_keyed(
            &config.path,
            Some(&key),
            options.capture_accepted,
            options.capture_initial_snapshot,
        )?;
        let db_ops = DbOperations::from_namespaced_store_with_content_and_field_keys(store, key)
            .await
            .map_err(|e| FoldDbError::Config(e.to_string()))?;
        let fold_db = FoldDB::initialize(Arc::new(db_ops), signer.public_key_base64(), options)
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
}

pub mod file_paths;
pub mod mutation_manager;
pub mod query;

pub use mutation_manager::MutationManager;
pub use query::QueryExecutor;

pub struct FoldDB {
    pub(crate) schema_manager: Arc<SchemaCore>,
    pub(crate) db_ops: Arc<DbOperations>,
    pub(crate) query_executor: QueryExecutor,
    pub(crate) mutation_manager: Arc<MutationManager>,
}

impl FoldDB {
    pub(crate) async fn initialize(
        db_ops: Arc<DbOperations>,
        writer_id: String,
        options: factory::FoldDbOptions,
    ) -> Result<Self, StorageError> {
        let schema_manager = Arc::new(
            SchemaCore::new(Arc::clone(&db_ops))
                .await
                .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?,
        );
        if !options.offline_maintenance {
            db_ops.native_runtime().begin_serving();
        }
        let query_executor = QueryExecutor::new(&db_ops, Arc::clone(&schema_manager));
        info!("Created QueryExecutor for query operations");
        db_ops
            .metadata()
            .author_clock(&writer_id)
            .await
            .map_err(|e| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {e}"
                ))
            })?;
        let mutation_manager = Arc::new(MutationManager::new(
            Arc::clone(&db_ops),
            Arc::clone(&schema_manager),
            writer_id,
            options.search_outbox_inbox,
            options.track_shared_record_revisions,
        ));
        info!("Created MutationManager for mutation operations");
        if !options.offline_maintenance {
            info!(target: "fold_node::database", "native writes complete on disk before acknowledgment");
        }
        Ok(Self {
            schema_manager,
            db_ops,
            query_executor,
            mutation_manager,
        })
    }

    pub fn db_ops(&self) -> &Arc<DbOperations> {
        &self.db_ops
    }

    pub fn query_executor(&self) -> &QueryExecutor {
        &self.query_executor
    }

    pub fn schema_manager(&self) -> Arc<SchemaCore> {
        Arc::clone(&self.schema_manager)
    }

    pub fn mutation_manager(&self) -> &MutationManager {
        &self.mutation_manager
    }

    pub async fn applied_delete_revision(
        &self,
        schema_name: &str,
        key: &KeyValue,
    ) -> Result<Option<String>, SchemaError> {
        let Some(schema) = self.schema_manager.resolve_schema(schema_name).await? else {
            return Ok(None);
        };
        self.schema_manager.require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        self.db_ops
            .records()
            .applied_delete_revision(&schema, key)
            .await
    }

    pub async fn absent_erase_targets(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Result<Vec<KeyValue>, SchemaError> {
        let schema = self.schema_manager.require_schema_metadata(schema_name)?;
        let store = self.db_ops.records();
        let rks = keys
            .iter()
            .map(|k| store.record_key(&schema, k))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(keys
            .iter()
            .zip(store.read_records(&rks).await?)
            .filter(|(_, r)| !r.as_deref().is_some_and(StoredRecord::is_live))
            .map(|(k, _)| k.clone())
            .collect())
    }

    pub async fn shutdown(&self) -> Result<(), StorageError> {
        info!(target: "fold_node::database", "Shutting down FoldDB: flushing storage");
        self.flush().await
    }

    pub async fn flush(&self) -> Result<(), StorageError> {
        self.db_ops
            .flush()
            .await
            .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
    }

    pub async fn list_schema_record_keys(
        &self,
        requested: &str,
        schema_id: &str,
        limit: usize,
        cursor: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        self.db_ops
            .read_scope()
            .await
            .map_err(SchemaError::from)?
            .run_result(async {
                let Some(schema) = self.schema_manager.resolve_schema(schema_id).await? else {
                    return Err(SchemaError::NotFound(format!(
                        "schema not found: {schema_id}"
                    )));
                };
                self.schema_manager.require_queryable_schema(&schema.name)?;
                crate::schema::require_logical_layout(&schema.schema_type)?;
                let key_definition = schema.require_record_keys()?.primary.clone();
                let records = self.db_ops.records();
                let ctx = records.key_comparison_context(&schema)?;
                let (pairs, next_cursor, has_more) = records
                    .list_record_keys(&schema, &ctx, limit, cursor, hash_filter)
                    .await?;
                Ok(SchemaRecordKeysReport {
                    schema: requested.to_string(),
                    schema_id: schema.name.clone(),
                    key_definition,
                    hash_filter: hash_filter.map(str::to_owned),
                    field_key: record_space_uuid(&schema.name),
                    keys: pairs
                        .into_iter()
                        .map(|(hash, range)| SchemaRecordKey { hash, range })
                        .collect(),
                    next_cursor,
                    has_more,
                    truncated: has_more,
                })
            })
            .await
            .map_err(FoldDbError::Schema)
    }

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
                let (rk, record) = self.read_file_record(schema_name, key, None).await?;
                let record = record.ok_or_else(|| {
                    FoldDbError::Database("local file reference has no record".into())
                })?;
                let (Some(stored), Some(ver)) = (&record.file, &record.file_version) else {
                    return Err(FoldDbError::Database(
                        "local file reference has no file version".into(),
                    ));
                };
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
        revision: Option<&str>,
    ) -> Result<(String, Option<Arc<StoredRecord>>), FoldDbError> {
        let schema = self.schema_manager.require_schema_metadata(schema_name)?;
        let records = self.db_ops.records();
        let rk = records.record_key(&schema, key)?;
        let rec = records
            .read_records(std::slice::from_ref(&rk))
            .await?
            .pop()
            .flatten();
        if let Some(expected) = revision {
            let actual = rec.as_deref().map(|r| r.stamp.revision());
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
                let (rk, record) = self.read_file_record(schema_name, key, revision).await?;
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
                let (rk, record) = self.read_file_record(schema_name, key, revision).await?;
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
