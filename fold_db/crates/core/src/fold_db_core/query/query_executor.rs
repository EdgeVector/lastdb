//! Captured query entry points: keyed reads, counts, and alternate key pages.

use super::{QueryRecords, Record};
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

    async fn in_read_scope<T>(
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
                for (entry, record) in entries.iter().zip(store.read_records(&addrs).await?) {
                    if !record.is_some_and(|r| {
                        r.is_live()
                            && r.alternates.get(name).and_then(Option::as_ref)
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
        if cursor
            .as_ref()
            .is_some_and(|c| c.comparison.as_deref() != Some(&comparison))
        {
            return Err(alternate_keys::invalid(
                "alternate cursor does not match this query",
            ));
        }
        let after = cursor
            .map(|c| {
                Ok::<_, SchemaError>(AlternateEntry {
                    primary: c.alternate_primary.clone().ok_or_else(|| {
                        alternate_keys::invalid("alternate cursor requires its primary tie-breaker")
                    })?,
                    matched: c.into_key(),
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
        let values = if entries.is_empty() {
            QueryRecords::new()
        } else {
            let pairs = entries
                .iter()
                .map(|e| {
                    (
                        e.primary.hash.clone().unwrap_or_default(),
                        e.primary.range.clone().unwrap_or_default(),
                    )
                })
                .collect();
            query.filter = Some(HashRangeFilter::HashRangeKeys(pairs));
            self.query_current(query, None).await?
        };
        for e in &entries {
            let record = values.get(&e.primary).ok_or_else(|| {
                SchemaError::InvalidData(format!(
                    "alternate page is missing a primary record ({} entries, {} records)",
                    entries.len(),
                    values.len()
                ))
            })?;
            if record.alternates.get(&name).and_then(Option::as_ref) != Some(&e.matched) {
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
