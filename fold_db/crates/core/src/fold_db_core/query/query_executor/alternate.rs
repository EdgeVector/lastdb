//! Alternate reads select primary keys, then use the ordinary record reader.

use super::*;
use crate::alternate_keys::{self, AlternateEntry};
use crate::schema::types::QueryCursor;
use sha2::{Digest, Sha256};

pub struct AlternatePage {
    /// The records at the primary keys of `entries`.
    pub values: QueryRecords,
    pub entries: Vec<AlternateEntry>,
    pub has_more: bool,
    pub comparison: String,
}

impl QueryExecutor {
    /// Count one explicitly keyed alternate selection in one captured root.
    /// Pages retain bounded memory; native read admission refuses excess work.
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
            let entries = self
                .db_ops
                .records()
                .select_alternate_entries(&schema, name, filter, after.as_ref(), 128)
                .await?;
            let keys: Vec<_> = entries.iter().map(|entry| entry.primary.clone()).collect();
            let store = self.db_ops.records();
            let addresses = keys
                .iter()
                .map(|key| store.record_key(&schema, key))
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
        query.filter = Some(crate::schema::types::field::HashRangeFilter::HashRangeKeys(
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

    /// The prepared schema of an alternate read.
    async fn alternate_schema(&self, query: &Query) -> Result<crate::schema::Schema, SchemaError> {
        let schema = self
            .prepare_query_schema(query)
            .await?
            .ok_or_else(|| alternate_keys::invalid("alternate schema is absent"))?;
        validate_controls(query)?;
        Ok(schema)
    }
}

fn validate_controls(query: &Query) -> Result<(), SchemaError> {
    if query.requires_post_key_work()
        || query.rehydrate_depth.is_some()
        || query.include_tombstones
        || query
            .sort_order
            .as_ref()
            .is_some_and(|order| *order != crate::schema::types::operations::SortOrder::Asc)
        || query.expected_total_count.is_some()
    {
        return Err(alternate_keys::invalid(
            "unsupported alternate query controls",
        ));
    }
    Ok(())
}

fn comparison_binding(
    schema: &crate::schema::Schema,
    name: &str,
    filter: &crate::schema::types::field::HashRangeFilter,
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
