//! FoldDB Core — main database coordinator, initialization, lifecycle, and schema drops.

use std::path::PathBuf;
use std::sync::{Arc, PoisonError, RwLock};
use tracing::info;

use super::mutation_manager::MutationManager;
use super::query::QueryExecutor;
use super::RecordBlobSource;
use crate::db_operations::DbOperations;
use crate::error::FoldDbResult;
use crate::schema::SchemaCore;
use crate::storage::StorageError;

mod admin_db;
mod record_files;

/// Bundled inputs for [`FoldDB::initialize_from_init`].
pub struct FoldDbInit {
    pub(crate) offline_maintenance: bool,
    pub db_ops: Arc<DbOperations>,
    pub signer: Arc<crate::security::Ed25519KeyPair>,
    pub search_outbox_inbox: Option<PathBuf>,
}

/// Main database coordinator managing schemas, queries, mutations, and blob access.
pub struct FoldDB {
    pub(crate) schema_manager: Arc<SchemaCore>,
    pub(crate) db_ops: Arc<DbOperations>,
    pub(crate) query_executor: QueryExecutor,
    pub(crate) mutation_manager: Arc<MutationManager>,
    pub(crate) record_blob_source: RwLock<Option<Arc<dyn RecordBlobSource>>>,
}

impl FoldDB {
    pub async fn initialize_from_init(init: FoldDbInit) -> Result<Self, StorageError> {
        let FoldDbInit {
            db_ops,
            signer,
            search_outbox_inbox,
            offline_maintenance,
        } = init;

        let schema_manager = Arc::new(
            SchemaCore::new(Arc::clone(&db_ops))
                .await
                .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))?,
        );
        if !offline_maintenance {
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
            search_outbox_inbox,
            author_clock_state,
        ));
        info!("Created MutationManager for mutation operations");

        if !offline_maintenance {
            info!(
                target: "fold_node::database",
                "native writes complete on disk before acknowledgment"
            );
        }

        Ok(Self {
            schema_manager,
            db_ops: Arc::clone(&db_ops),
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

    pub async fn drop_schema_and_bindings(&self, schema_name: &str) -> FoldDbResult<bool> {
        Ok(self.schema_manager().drop_schema(schema_name).await?)
    }

    pub async fn drop_schemas_owned_by_with_bindings(
        &self,
        owner_app: &str,
    ) -> FoldDbResult<Vec<String>> {
        let mut dropped = Vec::new();
        for name in self.schema_manager().schema_names_owned_by(owner_app)? {
            if self.drop_schema_and_bindings(&name).await? {
                dropped.push(name);
            }
        }
        dropped.sort();
        Ok(dropped)
    }
}
