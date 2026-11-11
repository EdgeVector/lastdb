//! Captured query entry points: keyed reads, counts, and alternate key pages.

use super::QueryRecords;
use crate::alternate_keys::{self, AlternateEntry};
use crate::db_operations::DbOperations;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::operations::SortOrder;
use crate::schema::types::{Query, QueryCursor, Schema, SchemaError};
use crate::schema::SchemaCore;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub struct AlternatePage {
    pub values: QueryRecords,
    pub entries: Vec<AlternateEntry>,
    pub has_more: bool,
    pub comparison: String,
}

/// Main query executor for keyed reads, counts, and alternate pages.
pub struct QueryExecutor {
    pub(super) db_ops: Arc<DbOperations>,
    schema_manager: Arc<SchemaCore>,
}

impl QueryExecutor {
    pub fn new(db_ops: &Arc<DbOperations>, schema_manager: Arc<SchemaCore>) -> Self {
        Self {
            db_ops: Arc::clone(db_ops),
            schema_manager,
        }
    }

    pub async fn query(&self, query: Query) -> Result<QueryRecords, SchemaError> {
        self.query_internal(query, None).await
    }

    pub async fn query_window(
        &self,
        query: Query,
        window: KeyWindow,
    ) -> Result<QueryRecords, SchemaError> {
        self.query_internal(query, Some(window)).await
    }

    async fn query_internal(
        &self,
        query: Query,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(self.query_current(query, window)))
            .await
    }

    async fn query_current(
        &self,
        mut query: Query,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        if query.key_name.is_some() {
            return Err(SchemaError::InvalidData(
                "use the named alternate page reader".into(),
            ));
        }
        let Some(schema) = self.prepare_query_schema(&query).await? else {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                query.schema_name
            )));
        };
        self.query_with_filter(&schema, query.filter.take(), window)
            .await
    }

    async fn prepare_query_schema(&self, query: &Query) -> Result<Option<Schema>, SchemaError> {
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

    pub async fn count_query_rows(&self, query: &Query) -> Result<Option<usize>, SchemaError> {
        if query.key_name.is_some() {
            return self.count_alternate_rows(query).await.map(Some);
        }
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(self.count_query_rows_scoped(query)))
            .await
    }

    async fn count_query_rows_scoped(&self, query: &Query) -> Result<Option<usize>, SchemaError> {
        let Some(schema) = self.prepare_query_schema(query).await? else {
            return Ok(None);
        };
        if query.include_tombstones {
            return Ok(None);
        }
        let count_filter = query.filter.as_ref().filter(|f| f.is_key_restricted());
        Ok(Some(self.count_rows(&schema, count_filter).await?))
    }

    pub async fn count_alternate_rows(&self, query: &Query) -> Result<usize, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(self.count_alternate_scoped(query)))
            .await
    }

    async fn count_alternate_scoped(&self, query: &Query) -> Result<usize, SchemaError> {
        let schema = self.alternate_schema(query).await?;
        let name = query
            .key_name
            .as_deref()
            .ok_or_else(|| alternate_keys::invalid("alternate name is absent"))?;
        if !schema.require_record_keys()?.alternates.contains_key(name) {
            return Err(alternate_keys::invalid("alternate key is not declared"));
        }
        let filter = query
            .filter
            .as_ref()
            .ok_or_else(|| alternate_keys::invalid("alternate count requires a key"))?;
        let mut after = None;
        let mut count = 0usize;
        loop {
            let store = self.db_ops.records();
            let entries = store
                .select_alternate_entries(&schema, name, filter, after.as_ref(), 128)
                .await?;
            let addresses = entries
                .iter()
                .map(|entry| store.record_key(&schema, &entry.primary))
                .collect::<Result<Vec<_>, _>>()?;
            let records = store.read_records(&addresses).await?;
            for (entry, record) in entries.iter().zip(records) {
                if !record.as_ref().is_some_and(|record| {
                    record.is_live()
                        && record.alternates.get(name).and_then(Option::as_ref)
                            == Some(&entry.matched)
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
    }

    pub async fn query_alternate_page(
        &self,
        query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
    ) -> Result<AlternatePage, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(self.read_alternate_page(query, limit, cursor)))
            .await
    }

    async fn read_alternate_page(
        &self,
        mut query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
    ) -> Result<AlternatePage, SchemaError> {
        let schema = self.alternate_schema(&query).await?;
        let name = query
            .key_name
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate name is absent"))?;
        if !schema.require_record_keys()?.alternates.contains_key(&name) {
            return Err(alternate_keys::invalid("alternate key is not declared"));
        }
        let filter = query
            .filter
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate filter is absent"))?;
        let comparison = comparison_binding(&schema, &name, &filter)?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.comparison.as_deref() != Some(&comparison))
        {
            return Err(alternate_keys::invalid(
                "alternate cursor does not match this query",
            ));
        }
        let after = cursor
            .map(|cursor| -> Result<AlternateEntry, SchemaError> {
                let primary = cursor.alternate_primary.clone().ok_or_else(|| {
                    alternate_keys::invalid("alternate cursor requires its primary tie-breaker")
                })?;
                Ok(AlternateEntry {
                    primary,
                    matched: cursor.into_key(),
                })
            })
            .transpose()?;
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
        let keys: Vec<_> = entries.iter().map(|entry| entry.primary.clone()).collect();
        query.filter = Some(HashRangeFilter::HashRangeKeys(
            keys.iter()
                .map(|key| {
                    (
                        key.hash.clone().unwrap_or_default(),
                        key.range.clone().unwrap_or_default(),
                    )
                })
                .collect(),
        ));
        let values = if keys.is_empty() {
            QueryRecords::new()
        } else {
            self.query_current(query, None).await?
        };
        validate_entries(&name, &entries, &values)?;
        Ok(AlternatePage {
            values,
            entries,
            has_more,
            comparison,
        })
    }

    async fn alternate_schema(&self, query: &Query) -> Result<Schema, SchemaError> {
        let schema = self
            .prepare_query_schema(query)
            .await?
            .ok_or_else(|| alternate_keys::invalid("alternate schema is absent"))?;
        if query.include_tombstones
            || query
                .sort_order
                .as_ref()
                .is_some_and(|order| *order != SortOrder::Asc)
            || query.expected_total_count.is_some()
        {
            return Err(alternate_keys::invalid(
                "unsupported alternate query controls",
            ));
        }
        Ok(schema)
    }
}

fn comparison_binding(
    schema: &Schema,
    name: &str,
    filter: &HashRangeFilter,
) -> Result<String, SchemaError> {
    Ok(format!(
        "ak2:{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                &schema.name,
                schema.alternate_key_revision(),
                &name,
                &filter
            ))
            .map_err(|_| alternate_keys::invalid("invalid alternate comparison"))?,
        )
    ))
}

fn validate_entries(
    name: &str,
    entries: &[AlternateEntry],
    values: &QueryRecords,
) -> Result<(), SchemaError> {
    for entry in entries {
        let record = values.get(&entry.primary).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "alternate page is missing a primary record ({} entries, {} records)",
                entries.len(),
                values.len()
            ))
        })?;
        if record.alternates.get(name).and_then(Option::as_ref) != Some(&entry.matched) {
            return Err(alternate_keys::invalid(
                "alternate entry does not match its primary record",
            ));
        }
    }
    Ok(())
}
