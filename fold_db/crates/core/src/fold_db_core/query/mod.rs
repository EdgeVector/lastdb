//! Query executor, record projection types, and primary keyed selection.

mod alternate;
mod range;

use crate::alternate_keys::AlternateEntry;
use crate::db_operations::record_store::KeyedRecord;
use crate::db_operations::DbOperations;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::{KeyValue, Query, Schema, SchemaError};
use crate::schema::SchemaCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub struct AlternatePage {
    pub values: QueryRecords,
    pub entries: Vec<AlternateEntry>,
    pub has_more: bool,
    pub comparison: String,
}

pub struct QueryExecutor {
    pub(super) db_ops: Arc<DbOperations>,
    pub(super) schema_manager: Arc<SchemaCore>,
}

pub mod query_executor {
    pub use super::{AlternatePage, QueryExecutor};
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<lastdb_model::record_value::FileMarker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<lastdb_model::record_value::FileMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub revision: String,
    pub alternates: BTreeMap<String, Option<KeyValue>>,
    pub fields: HashMap<String, Value>,
    pub metadata: Option<RecordMetadata>,
}

pub type QueryRecords = HashMap<KeyValue, Record>;

impl Record {
    pub fn from_stored(r: &lastdb_model::record_value::StoredRecord) -> Self {
        let meta = r
            .meta
            .as_ref()
            .filter(|m| m.source_file_name.is_some() || !m.metadata.is_empty());
        let metadata = (meta.is_some() || r.file.is_some() || r.inline_file.is_some()).then(|| {
            RecordMetadata {
                source_file_name: meta.and_then(|m| m.source_file_name.clone()),
                metadata: meta
                    .filter(|m| !m.metadata.is_empty())
                    .map(|m| m.metadata.clone().into_iter().collect()),
                file: r.file.clone(),
                inline_file: r.inline_file.clone(),
            }
        });
        Self {
            revision: r.stamp.revision(),
            alternates: r.alternates.clone(),
            fields: r.fields.clone().into_iter().collect(),
            metadata,
        }
    }
}

impl QueryExecutor {
    pub fn new(db_ops: &Arc<DbOperations>, schema_manager: Arc<SchemaCore>) -> Self {
        Self {
            db_ops: Arc::clone(db_ops),
            schema_manager,
        }
    }

    pub(super) async fn in_read_scope<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T, SchemaError>>,
    ) -> Result<T, SchemaError> {
        self.db_ops
            .read_scope()
            .await?
            .run_result(Box::pin(fut))
            .await
    }

    pub async fn query(&self, query: Query) -> Result<QueryRecords, SchemaError> {
        self.in_read_scope(self.query_current(query, None)).await
    }

    pub async fn query_window(
        &self,
        query: Query,
        window: KeyWindow,
    ) -> Result<QueryRecords, SchemaError> {
        self.in_read_scope(self.query_current(query, Some(window)))
            .await
    }

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

    pub(super) async fn query_current(
        &self,
        mut query: Query,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        if query.key_name.is_some() {
            return Err(SchemaError::InvalidData(
                "use the named alternate page reader".into(),
            ));
        }
        let schema = self.prepare_query_schema(&query).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{}' not found", query.schema_name))
        })?;
        Ok(self
            .select_filtered(&schema, query.filter.take(), window)
            .await?
            .into_iter()
            .map(|(k, r)| (k, Record::from_stored(&r)))
            .collect())
    }

    pub(super) async fn prepare_query_schema(
        &self,
        query: &Query,
    ) -> Result<Option<Schema>, SchemaError> {
        let Some(schema) = self
            .schema_manager
            .resolve_schema(&query.schema_name)
            .await?
        else {
            return Ok(None);
        };
        self.schema_manager.require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(query.filter.as_ref())?;
        Ok(Some(schema))
    }

    pub(super) async fn select_filtered(
        &self,
        schema: &Schema,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        crate::schema::require_logical_filter(filter.as_ref())?;
        let cmp = self.db_ops.records().key_comparison_context(schema)?;
        cmp.verify_active()?;
        let filter = cmp.canonical_filter(filter)?.ok_or_else(|| {
            SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
        })?;
        let window = cmp.canonical_window(window)?;
        self.select_rows(schema, &cmp, &filter, window).await
    }

    async fn select_rows(
        &self,
        schema: &Schema,
        cmp: &KeyComparisonContext,
        filter: &HashRangeFilter,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        if let Some((hash, bound)) = range::range_under_hash(schema, filter) {
            return self.range_rows(schema, cmp, hash, bound, window).await;
        }
        let rows = self
            .db_ops
            .records()
            .select_records(schema, cmp, filter)
            .await?;
        match window {
            Some(KeyWindow::Offset { offset, limit }) => {
                Ok(rows.into_iter().skip(offset).take(limit).collect())
            }
            Some(KeyWindow::After { after, limit }) => {
                let mut kept = Vec::new();
                for (k, r) in rows {
                    if cmp.cmp_with_context(&k, &after)?.is_gt() {
                        kept.push((k, r));
                        if kept.len() == limit {
                            break;
                        }
                    }
                }
                Ok(kept)
            }
            None => Ok(rows),
        }
    }
}
