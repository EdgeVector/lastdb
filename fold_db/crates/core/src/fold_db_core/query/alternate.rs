//! Named alternate-key count and paged query execution on [`QueryExecutor`].

use super::{AlternateKeyPage, AlternatePage, QueryExecutor, Record};
use crate::alternate_keys::{self, AlternateEntry};
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::SortOrder;
use crate::schema::types::{KeyValue, Query, QueryCursor, Schema, SchemaError};
use lastdb_model::hex::sha256_hex;
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;
use std::sync::Arc;

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
        self.in_read_scope(async {
            let checked = self.read_alternate_entries(query, limit, cursor).await?;
            Ok(AlternatePage {
                values: checked
                    .records
                    .into_iter()
                    .map(|(k, r)| (k, Record::from_stored(&r)))
                    .collect(),
                entries: checked.entries,
                has_more: checked.has_more,
                comparison: checked.comparison,
            })
        })
        .await
    }

    /// The same page as `query_alternate_page`, with the same checks of each
    /// entry against its primary record, without a copy of the records.
    pub async fn query_alternate_key_page(
        &self,
        query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
    ) -> Result<AlternateKeyPage, SchemaError> {
        self.in_read_scope(async {
            let checked = self.read_alternate_entries(query, limit, cursor).await?;
            Ok(AlternateKeyPage {
                entries: checked.entries,
                has_more: checked.has_more,
                comparison: checked.comparison,
            })
        })
        .await
    }

    /// One page of alternate entries and their primary records. Each entry must
    /// name a live primary record whose stored alternate address is the entry.
    async fn read_alternate_entries(
        &self,
        mut query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
    ) -> Result<CheckedEntries, SchemaError> {
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
        let spec_bytes = serde_json::to_vec(&spec)
            .map_err(|_| alternate_keys::invalid("invalid alternate comparison"))?;
        let comparison = format!("ak2:{}", sha256_hex(spec_bytes));
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
        let records = self.primary_records(query, &entries).await?;
        check_entries(&entries, &records, &name)?;
        Ok(CheckedEntries {
            entries,
            records,
            has_more,
            comparison,
        })
    }

    async fn primary_records(
        &self,
        mut query: Query,
        entries: &[AlternateEntry],
    ) -> Result<HashMap<KeyValue, Arc<StoredRecord>>, SchemaError> {
        if entries.is_empty() {
            return Ok(HashMap::new());
        }
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
        Ok(self
            .query_current_stored(query, None)
            .await?
            .into_iter()
            .collect())
    }

    async fn require_declared_alternate<'a>(
        &self,
        query: &'a Query,
    ) -> Result<(Schema, &'a str), SchemaError> {
        let schema = self
            .prepare_query_schema(query)
            .await?
            .ok_or_else(|| alternate_keys::invalid("alternate schema is absent"))?;
        if query
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

struct CheckedEntries {
    entries: Vec<AlternateEntry>,
    records: HashMap<KeyValue, Arc<StoredRecord>>,
    has_more: bool,
    comparison: String,
}

/// An alternate entry is a link; the primary record is the truth.
fn check_entries(
    entries: &[AlternateEntry],
    records: &HashMap<KeyValue, Arc<StoredRecord>>,
    name: &str,
) -> Result<(), SchemaError> {
    for e in entries {
        let rec = records.get(&e.primary).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "alternate page is missing a primary record ({} entries, {} records)",
                entries.len(),
                records.len()
            ))
        })?;
        if rec.alternates.get(name).and_then(Option::as_ref) != Some(&e.matched) {
            return Err(alternate_keys::invalid(
                "alternate entry does not match its primary record",
            ));
        }
    }
    Ok(())
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
