//! FoldDB Core — embedded database coordinator, lifecycle, and record files.

use async_trait::async_trait;
use cloud_backup_protocol::v2::blobs::BlobPath;
use lastdb_model::portable_mutation::Position;
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
use crate::storage::format::records::RecordFormat;
use crate::storage::StorageError;

#[path = "factory/local/mod.rs"]
pub mod factory;
pub mod mutation_manager;
pub mod query;

pub use mutation_manager::MutationManager;
pub use query::QueryExecutor;

#[async_trait]
trait BlobSource: Send + Sync {
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> Option<Vec<u8>>;
}

pub struct FoldDB {
    pub(crate) schema_manager: Arc<SchemaCore>,
    pub(crate) db_ops: Arc<DbOperations>,
    pub(crate) query_executor: QueryExecutor,
    pub(crate) mutation_manager: Arc<MutationManager>,
    blob_source: Option<Arc<dyn BlobSource>>,
}

impl FoldDB {
    pub(crate) async fn initialize(
        db_ops: Arc<DbOperations>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        options: factory::FoldDbOptions,
    ) -> Result<Self, StorageError> {
        let schema_manager = Arc::new(
            SchemaCore::new(Arc::clone(&db_ops))
                .await
                .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?,
        );
        if !options.offline_maintenance {
            db_ops.records().freeze_replay_cutoff().await?;
        }
        let query_executor = QueryExecutor::new(&db_ops, Arc::clone(&schema_manager));
        info!("Created QueryExecutor for query operations");
        let author_clock_state = db_ops
            .metadata()
            .author_clock(&signer.public_key_base64())
            .await
            .map_err(|e| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {e}"
                ))
            })?
            .unwrap_or_default();
        let mutation_manager = Arc::new(MutationManager::new(
            Arc::clone(&db_ops),
            Arc::clone(&schema_manager),
            signer,
            options.search_outbox_inbox,
            author_clock_state,
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
            blob_source: None,
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
                let schema = self
                    .schema_manager
                    .resolve_schema(schema_id)
                    .await?
                    .ok_or_else(|| {
                        SchemaError::NotFound(format!("schema not found: {schema_id}"))
                    })?;
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
                let (record_key, record) = self.read_file_record(schema_name, key).await?;
                let record = record.ok_or_else(|| {
                    FoldDbError::Database("local file reference has no record".into())
                })?;
                let (stored, version) = record
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
                    &record_file_path(&record_key, version)?,
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
        let local = self
            .db_ops
            .read_scope()
            .await?
            .run_result(async {
                let (rk, record) = self.read_file_record(schema_name, key).await?;
                require_revision(schema_name, key, record.as_deref(), revision)?;
                let Some((marker, ver)) = record
                    .as_ref()
                    .and_then(|r| r.file.as_ref().zip(r.file_version.as_ref()))
                else {
                    return Ok::<_, FoldDbError>(None);
                };
                let state = self
                    .db_ops
                    .records()
                    .format
                    .open_record_file(&rk, ver, marker)
                    .await?
                    .ok_or_else(|| (rk, ver.clone()));
                Ok(Some((marker.clone(), state)))
            })
            .await?;
        let Some((marker, state)) = local else {
            return Ok(None);
        };
        let bytes = match state {
            Ok(bytes) => bytes,
            Err((rk, ver)) => self.fetch_missing_file(&rk, &ver, &marker).await?,
        };
        Ok(Some((marker, bytes)))
    }

    async fn fetch_missing_file(
        &self,
        rk: &str,
        ver: &Position,
        marker: &FileMarker,
    ) -> Result<Vec<u8>, FoldDbError> {
        let src = self.blob_source.as_ref().ok_or_else(|| {
            FoldDbError::InProgress(
                "the record's file is not on this device and could not be downloaded: no cloud backup is configured on this node".into(),
            )
        })?;
        let fmt = &self.db_ops.records().format;
        let row = src
            .fetch(
                &fmt.record_blob_path(rk, ver)?,
                RecordFormat::sealed_file_bytes(marker)?,
            )
            .await
            .ok_or_else(|| SchemaError::NotFound("the record's file is not in the cloud".into()))?;
        fmt.install_record_file(rk, ver, marker, row).await
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
