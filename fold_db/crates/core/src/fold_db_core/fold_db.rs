//! FoldDB Core — main database coordinator, lifecycle, schema drops, and owner admin reports.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use serde_json::{json, Value};
use tracing::info;

use super::factory::FoldDbOptions;
use super::mutation_manager::MutationManager;
use super::query::QueryExecutor;
use super::RecordBlobSource;
use crate::db_operations::{
    DbOperations, RecordDeleteLedgerEntry, SchemaRecordKey, SchemaRecordKeysReport,
    SchemaRecordKeysRequest, SchemaStorageReport,
};
use crate::error::{FoldDbError, FoldDbResult};
use crate::schema::types::{schema::DeclarativeSchemaType, KeyValue, SchemaError};
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

    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, FoldDbError> {
        self.db_ops()
            .records()
            .list_record_delete_ledger(limit)
            .await
            .map_err(|e| FoldDbError::Database(format!("list_record_delete_ledger: {e}")))
    }

    pub async fn backfill_schema_retention_page(
        &self,
        name: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Value, FoldDbError> {
        let schema = self
            .schema_manager()
            .get_schema_metadata(name)?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {name}")))?;
        let range_keyed = match schema.schema_type {
            DeclarativeSchemaType::Hash => false,
            DeclarativeSchemaType::HashRange => true,
            _ => {
                return Err(SchemaError::InvalidData(
                    "retention backfill supports Hash and HashRange schemas".into(),
                )
                .into())
            }
        };
        let bound = crate::fold_db_core::purge::budget::max_erase_keys(
            false,
            crate::fold_db_core::purge::budget::EraseShape::of(&schema),
            4096,
        );
        if bound == 0 {
            return Err(SchemaError::InvalidData(
                "schema is too wide for one bounded retention page".into(),
            )
            .into());
        }
        let page = self
            .list_schema_record_keys(SchemaRecordKeysRequest {
                requested: name,
                schema_id: name,
                limit: limit.clamp(1, 64).min(bound),
                cursor,
                hash_filter: None,
            })
            .await?;
        if page.schema_id != schema.name {
            return Err(SchemaError::InvalidData(
                "retention backfill refuses a redirected schema; use its current name".into(),
            )
            .into());
        }
        let keys: Vec<_> = page
            .keys
            .iter()
            .map(|key| {
                KeyValue::new(
                    Some(key.hash.clone()),
                    range_keyed.then(|| key.range.clone()),
                )
            })
            .collect();
        let count = self
            .db_ops()
            .records()
            .in_background_scope(async {
                let clocks = if range_keyed {
                    keys.into_iter().map(|key| (key, 0)).collect()
                } else {
                    crate::fold_db_core::purge::retention_tip_clocks(
                        self.db_ops(),
                        &self.schema_manager(),
                        &schema.name,
                        &keys,
                    )
                    .await?
                };
                self.db_ops()
                    .schemas()
                    .backfill_retention_rows(name, clocks, range_keyed)
                    .await
            })
            .await?;
        Ok(json!({
            "schema": name,
            "scope": "personal",
            "tracked": count,
            "next_cursor": page.next_cursor,
            "has_more": page.has_more,
            "complete": !page.has_more,
            "existing_records_may_expire": true
        }))
    }

    pub async fn list_schema_record_keys(
        &self,
        request: SchemaRecordKeysRequest<'_>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        let scope = self
            .db_ops()
            .read_scope()
            .await
            .map_err(SchemaError::from)?;
        scope
            .run_result(self.list_schema_record_keys_captured(request))
            .await
            .map_err(FoldDbError::Schema)
    }

    async fn list_schema_record_keys_captured(
        &self,
        request: SchemaRecordKeysRequest<'_>,
    ) -> Result<SchemaRecordKeysReport, SchemaError> {
        let SchemaRecordKeysRequest {
            requested,
            schema_id,
            limit,
            cursor,
            hash_filter,
        } = request;
        let schema = self
            .schema_manager()
            .resolve_schema(schema_id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {schema_id}")))?;
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
    }

    pub async fn db_schema_storage(&self) -> Result<SchemaStorageReport, FoldDbError> {
        let schemas = self
            .db_ops()
            .schemas()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let names: Vec<String> = schemas.values().map(|schema| schema.name.clone()).collect();
        let mut breakdown = self
            .db_ops()
            .records()
            .record_storage_breakdown(&names)
            .await
            .map_err(|e| FoldDbError::Database(format!("schema record storage: {e}")))?;
        let mut display = HashMap::new();
        for (key, schema) in &schemas {
            let label = schema
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(schema.name.trim());
            if label.is_empty() {
                continue;
            }
            display.insert(key.clone(), label.to_string());
            display.insert(schema.name.clone(), label.to_string());
            if let Some(hash) = schema.identity_hash.as_ref() {
                display.insert(hash.clone(), label.to_string());
            }
        }
        breakdown.apply_display_names(&display);
        Ok(SchemaStorageReport::from_breakdown(breakdown))
    }
}
