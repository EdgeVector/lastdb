//! FoldDB Core — embedded database coordinator, lifecycle, and alternate-key queries.

use async_trait as _;
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::StoredRecord;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::info;

use crate::alternate_keys::{self, AlternateEntry};
use crate::db_operations::{DbOperations, SchemaRecordKey, SchemaRecordKeysReport};
use crate::error::FoldDbError;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::SortOrder;
use crate::schema::types::{KeyValue, Query, QueryCursor, Schema, SchemaError};
use crate::schema::SchemaCore;
use crate::storage::StorageError;

#[path = "factory/local/mod.rs"]
pub mod factory;
pub mod mutation_manager;
pub mod query;

pub use mutation_manager::MutationManager;
pub use query::QueryExecutor;
use query::{AlternatePage, QueryRecords};

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

impl QueryExecutor {
    pub async fn count_query_rows(&self, query: &Query) -> Result<Option<usize>, SchemaError> {
        if query.key_name.is_some() {
            return self.count_alternate_rows(query).await.map(Some);
        }
        self.in_read_scope(async {
            let Some(schema) = self.prepare_query_schema(query).await? else {
                return Ok(None);
            };
            if query.include_tombstones {
                return Ok(None);
            }
            Ok(Some(
                self.select_filtered(&schema, query.filter.clone(), None)
                    .await?
                    .len(),
            ))
        })
        .await
    }

    pub async fn count_alternate_rows(&self, query: &Query) -> Result<usize, SchemaError> {
        self.in_read_scope(async {
            let (schema, name) = self.require_declared_alternate(query).await?;
            let filter = query
                .filter
                .as_ref()
                .ok_or_else(|| alternate_keys::invalid("alternate count requires a key"))?;
            let store = self.db_ops.records();
            let (mut after, mut count) = (None, 0usize);
            loop {
                let entries = store
                    .select_alternate_entries(&schema, name, filter, after.as_ref(), 128)
                    .await?;
                let addrs = entries
                    .iter()
                    .map(|e| store.record_key(&schema, &e.primary))
                    .collect::<Result<Vec<_>, _>>()?;
                for (e, r) in entries.iter().zip(store.read_records(&addrs).await?) {
                    if !r.is_some_and(|r| {
                        r.is_live()
                            && r.alternates.get(name).and_then(Option::as_ref) == Some(&e.matched)
                    }) {
                        return Err(alternate_keys::invalid(
                            "alternate count found an entry without its current primary record",
                        ));
                    }
                }
                count = count
                    .checked_add(entries.len())
                    .ok_or_else(|| alternate_keys::invalid("alternate count overflow"))?;
                if entries.len() < 128 {
                    return Ok(count);
                }
                after = entries.last().cloned();
            }
        })
        .await
    }

    pub async fn query_alternate_page(
        &self,
        query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
    ) -> Result<AlternatePage, SchemaError> {
        self.in_read_scope(self.read_alternate_page(query, limit, cursor))
            .await
    }

    async fn read_alternate_page(
        &self,
        mut query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
    ) -> Result<AlternatePage, SchemaError> {
        let (schema, name) = self.require_declared_alternate(&query).await?;
        let name = name.to_owned();
        query.key_name = None;
        let filter = query
            .filter
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate filter is absent"))?;
        let spec = (
            &schema.name,
            schema.alternate_key_revision(),
            &name,
            &filter,
        );
        let comparison = format!(
            "ak2:{:x}",
            Sha256::digest(
                serde_json::to_vec(&spec)
                    .map_err(|_| alternate_keys::invalid("invalid alternate comparison"))?,
            )
        );
        let after = parse_alternate_cursor(cursor, &comparison)?;
        let mut entries = self
            .db_ops
            .records()
            .select_alternate_entries(
                &schema,
                &name,
                &filter,
                after.as_ref(),
                limit.saturating_add(1),
            )
            .await?;
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let values = if entries.is_empty() {
            QueryRecords::new()
        } else {
            query.filter = Some(HashRangeFilter::HashRangeKeys(
                entries
                    .iter()
                    .map(|e| {
                        (
                            e.primary.hash.clone().unwrap_or_default(),
                            e.primary.range.clone().unwrap_or_default(),
                        )
                    })
                    .collect(),
            ));
            self.query_current(query, None).await?
        };
        for e in &entries {
            let rec = values.get(&e.primary).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "alternate page is missing a primary record ({} entries, {} records)",
                    entries.len(),
                    values.len()
                ))
            })?;
            if rec.alternates.get(&name).and_then(Option::as_ref) != Some(&e.matched) {
                return Err(alternate_keys::invalid(
                    "alternate entry does not match its primary record",
                ));
            }
        }
        Ok(AlternatePage {
            values,
            entries,
            has_more,
            comparison,
        })
    }

    async fn require_declared_alternate<'a>(
        &self,
        query: &'a Query,
    ) -> Result<(Schema, &'a str), SchemaError> {
        let schema = self
            .prepare_query_schema(query)
            .await?
            .ok_or_else(|| alternate_keys::invalid("alternate schema is absent"))?;
        if query.include_tombstones
            || query
                .sort_order
                .as_ref()
                .is_some_and(|o| *o != SortOrder::Asc)
            || query.expected_total_count.is_some()
        {
            return Err(alternate_keys::invalid(
                "unsupported alternate query controls",
            ));
        }
        let name = query
            .key_name
            .as_deref()
            .ok_or_else(|| alternate_keys::invalid("alternate name is absent"))?;
        schema
            .require_record_keys()?
            .alternates
            .contains_key(name)
            .then_some((schema, name))
            .ok_or_else(|| alternate_keys::invalid("alternate key is not declared"))
    }
}

fn parse_alternate_cursor(
    cursor: Option<QueryCursor>,
    comparison: &str,
) -> Result<Option<AlternateEntry>, SchemaError> {
    let Some(c) = cursor else {
        return Ok(None);
    };
    if c.comparison.as_deref() != Some(comparison) {
        return Err(alternate_keys::invalid(
            "alternate cursor does not match this query",
        ));
    }
    let primary = c.alternate_primary.clone().ok_or_else(|| {
        alternate_keys::invalid("alternate cursor requires its primary tie-breaker")
    })?;
    Ok(Some(AlternateEntry {
        primary,
        matched: c.into_key(),
    }))
}
