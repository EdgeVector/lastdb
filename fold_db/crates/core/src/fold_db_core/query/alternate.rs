//! Named alternate-key count and paged query execution on [`QueryExecutor`].

use super::{AlternatePage, QueryExecutor, QueryRecords};
use crate::alternate_keys::{self, AlternateEntry};
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::SortOrder;
use crate::schema::types::{Query, QueryCursor, Schema, SchemaError};
use sha2::{Digest, Sha256};

impl QueryExecutor {
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
