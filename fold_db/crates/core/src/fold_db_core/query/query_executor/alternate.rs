//! Alternate reads select primary keys, then use the ordinary field reader.

use super::*;
use crate::alternate_keys::{self, AlternateEntry};
use crate::schema::types::QueryCursor;
use sha2::{Digest, Sha256};

pub struct AlternatePage {
    pub values: HashMap<String, HashMap<KeyValue, FieldValue>>,
    pub entries: Vec<AlternateEntry>,
    pub has_more: bool,
    pub comparison: String,
}

impl QueryExecutor {
    pub async fn query_alternate_page(
        &self,
        query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
        context: &AccessContext,
    ) -> Result<AlternatePage, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(
                crate::storage::disk_pack::logical_paths::field::query_scope(
                    self.read_alternate_page(query, limit, cursor, context),
                ),
            ))
            .await
    }

    async fn read_alternate_page(
        &self,
        mut query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
        context: &AccessContext,
    ) -> Result<AlternatePage, SchemaError> {
        let schema = self
            .prepare_query_schema(&query)
            .await?
            .ok_or_else(|| alternate_keys::invalid("alternate schema is absent"))?;
        let scope = self
            .db_ops
            .db_catalog()
            .resolve_storage_prefix(
                context.db_locator.as_deref(),
                &schema.name,
                context.storage_prefix.as_deref(),
            )
            .await?;
        if scope.is_some() {
            return Err(alternate_keys::invalid(
                "alternate reads require the personal database scope",
            ));
        }
        validate_controls(&query)?;
        let name = query
            .key_name
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate name is absent"))?;
        if !schema.alternate_keys.contains_key(&name) {
            return Err(alternate_keys::invalid("alternate key is not declared"));
        }
        let filter = query
            .filter
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate filter is absent"))?;
        let comparison = format!(
            "ak1:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    &schema.name,
                    schema.alternate_key_revision(),
                    &name,
                    &filter
                ))
                .map_err(|_| alternate_keys::invalid("invalid alternate comparison"))?,
            )
        );
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.comparison.as_deref() != Some(&comparison))
        {
            return Err(alternate_keys::invalid(
                "alternate cursor does not match this query",
            ));
        }
        let after = cursor.map(QueryCursor::into_key);
        let mut entries = self
            .db_ops
            .atoms()
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
        let fields = self
            .db_ops
            .atoms()
            .alternate_fields(&schema, &keys, None)
            .await?;
        for (entry, fields) in entries.iter().zip(fields) {
            if alternate_keys::derived(&schema, &fields)?.get(&name) != Some(&entry.matched) {
                return Err(alternate_keys::invalid(
                    "alternate entry does not match its primary record",
                ));
            }
        }
        query.filter = Some(crate::schema::types::field::HashRangeFilter::HashRangeKeys(
            keys.iter()
                .map(|key| (key.hash.clone().unwrap_or_default(), String::new()))
                .collect(),
        ));
        let values = if keys.is_empty() {
            HashMap::new()
        } else {
            self.query_current(query, None, Some(context)).await?
        };
        Ok(AlternatePage {
            values,
            entries,
            has_more,
            comparison,
        })
    }
}

fn validate_controls(query: &Query) -> Result<(), SchemaError> {
    if query.requires_post_key_work()
        || query.as_of.is_some()
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
