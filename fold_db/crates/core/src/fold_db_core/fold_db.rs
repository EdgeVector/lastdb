//! FoldDB Core — main database coordinator, lifecycle, and record key listing.

use std::sync::{Arc, PoisonError, RwLock};

use tracing::info;

use super::factory::FoldDbOptions;
use super::mutation_manager::MutationManager;
use super::query::QueryExecutor;
use super::RecordBlobSource;
use crate::db_operations::{DbOperations, SchemaRecordKey, SchemaRecordKeysReport};
use crate::error::FoldDbError;
use crate::schema::types::SchemaError;
use crate::schema::SchemaCore;
use crate::storage::StorageError;
use lastdb_model::record_space::record_space_uuid;

mod record_files;

/// Main database coordinator managing schemas, queries, mutations, and blob access.
pub struct FoldDB {
    pub(crate) schema_manager: Arc<SchemaCore>,
    pub(crate) db_ops: Arc<DbOperations>,
    pub(crate) query_executor: QueryExecutor,
    pub(crate) mutation_manager: Arc<MutationManager>,
    pub(crate) record_blob_source: RwLock<Option<Arc<dyn RecordBlobSource>>>,
}

impl FoldDB {
    pub(super) async fn initialize(
        db_ops: Arc<DbOperations>,
        signer: Arc<crate::security::Ed25519KeyPair>,
        options: FoldDbOptions,
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
            .map_err(|error| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {error}"
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
            info!(
                target: "fold_node::database",
                "native writes complete on disk before acknowledgment"
            );
        }

        Ok(Self {
            schema_manager,
            db_ops,
            query_executor,
            mutation_manager,
            record_blob_source: RwLock::new(None),
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

    pub fn set_record_blob_source(&self, source: Option<Arc<dyn RecordBlobSource>>) {
        *self
            .record_blob_source
            .write()
            .unwrap_or_else(PoisonError::into_inner) = source;
    }

    pub(crate) fn record_blob_source(&self) -> Option<Arc<dyn RecordBlobSource>> {
        self.record_blob_source
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn mutation_manager(&self) -> &MutationManager {
        &self.mutation_manager
    }

    pub async fn shutdown(&self) -> Result<(), StorageError> {
        info!(
            target: "fold_node::database",
            "Shutting down FoldDB: flushing storage"
        );
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
        let scope = self
            .db_ops()
            .read_scope()
            .await
            .map_err(SchemaError::from)?;
        scope
            .run_result(async {
                let schema = self
                    .schema_manager()
                    .resolve_schema(schema_id)
                    .await?
                    .ok_or_else(|| {
                        SchemaError::NotFound(format!("schema not found: {schema_id}"))
                    })?;
                self.schema_manager()
                    .require_queryable_schema(&schema.name)?;
                crate::schema::require_logical_layout(&schema.schema_type)?;
                let key_definition = schema.require_record_keys()?.primary.clone();
                let records = self.db_ops().records();
                let comparison = records.key_comparison_context(&schema)?;
                let (pairs, next_cursor, has_more) = records
                    .list_record_keys(&schema, &comparison, limit, cursor, hash_filter)
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
}
