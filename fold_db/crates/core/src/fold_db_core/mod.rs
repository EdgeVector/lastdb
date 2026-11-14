//! FoldDB Core — embedded database coordinator and lifecycle.

use async_trait as _;
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;
use tracing::info;

use crate::db_operations::{DbOperations, SchemaRecordKey, SchemaRecordKeysReport};
use crate::error::FoldDbError;
use crate::schema::types::{KeyValue, SchemaError};
use crate::schema::SchemaCore;
use crate::storage::StorageError;

#[path = "factory/local/mod.rs"]
pub mod factory;
mod files;
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
        db_ops
            .metadata()
            .author_clock(&signer.public_key_base64())
            .await
            .map_err(|e| {
                StorageError::BackendError(format!(
                    "failed to load mutation author clock at boot: {e}"
                ))
            })?;
        let mutation_manager = Arc::new(MutationManager::new(
            Arc::clone(&db_ops),
            Arc::clone(&schema_manager),
            signer,
            options.search_outbox_inbox,
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
}
