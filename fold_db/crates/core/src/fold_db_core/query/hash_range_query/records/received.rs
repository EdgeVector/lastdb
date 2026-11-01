//! The record read of a format 7 home while it receives data from other users.
//!
//! Each active sender's records live in its own `from:{sender}` scope. A read
//! merges the own scope and the received scopes by key, in that order: at each
//! key the first scope with a live record gives the whole row, as the molecule
//! path takes each row from one source. A delete in one scope shows the record
//! of the next scope. A record names no writer (decision D7), so a received
//! row does not name its sender.
//!
//! A keyed selector reads each key in each scope in one batch read. A range
//! under one hash merges paged key reads (`received_range.rs`). A page walks
//! each scope, as the own-scope read does. A sample is one merged walk of the
//! scopes (`HashRangeQueryProcessor::sample_rows`), so it does not come here.

use super::{bound, has_any};
use crate::db_operations::record_store::{KeyedRecord, RecordBound};
use crate::fold_db_core::query::hash_range_query::HashRangeQueryProcessor;
use crate::schema::types::field::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder};
use crate::schema::{Schema, SchemaError};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// One merge candidate: an API key and its physical record keys, one for
/// each scope that may hold it, in scope order.
pub(super) type Candidate = (KeyValue, Vec<String>);

impl HashRangeQueryProcessor {
    /// The rows of one selector over `scopes` (the own scope first), in page
    /// order and bounded by the window. The caller reads a range under one
    /// hash through `received_range`.
    pub(super) async fn select_received(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
        window: Option<KeyWindow>,
        selected: &[String],
        scopes: &[Option<String>],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        use HashRangeFilter as F;
        let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
        let mut rows = match filter {
            Some(F::HashRangeKey { hash, range }) => {
                self.received_at(schema, &[api_key(ranged, hash, range)], scopes)
                    .await?
            }
            Some(F::HashRangeKeys(keys)) => {
                let keys: Vec<KeyValue> = keys
                    .iter()
                    .map(|(hash, range)| api_key(ranged, hash, range))
                    .collect();
                self.received_at(schema, &keys, scopes).await?
            }
            // A Hash schema has one record for each hash.
            Some(
                F::HashKey(hash) | F::HashRangePrefix { hash, .. } | F::HashRangeRange { hash, .. },
            ) => {
                let key = KeyValue::new(Some(hash.clone()), None);
                self.received_at(schema, &[key], scopes).await?
            }
            Some(F::Page { .. }) | None => {
                self.received_walk(schema, comparison, filter, scopes)
                    .await?
            }
            Some(_) => {
                return Err(SchemaError::InvalidData(
                    "a format 7 home reads records only by key, by one hash, or by page".into(),
                ))
            }
        };
        rows.retain(|(_, record)| has_any(record, selected));
        Ok(match window {
            Some(window) => bound(rows, window),
            None => rows,
        })
    }

    /// The winning record at each distinct key, read in each scope.
    async fn received_at(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        scopes: &[Option<String>],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let records = self.db_ops.records();
        let mut seen = HashSet::new();
        let mut candidates = Vec::with_capacity(keys.len());
        for key in keys.iter().filter(|key| seen.insert(*key)) {
            let physical = scopes
                .iter()
                .map(|scope| records.record_key(schema, key, scope.as_deref()))
                .collect::<Result<Vec<_>, _>>()?;
            candidates.push((key.clone(), physical));
        }
        self.received_winners(candidates).await
    }

    /// One batch read of every candidate key; at each key the first live
    /// record in scope order wins. The rows come back in page order.
    pub(super) async fn received_winners(
        &self,
        candidates: Vec<Candidate>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let physical: Vec<String> = candidates
            .iter()
            .flat_map(|(_, keys)| keys.iter().cloned())
            .collect();
        let records = self.db_ops.records().read_records(&physical).await?;
        let mut rows = Vec::with_capacity(candidates.len());
        let mut at = 0;
        for (key, keys) in candidates {
            let found = &records[at..at + keys.len()];
            at += keys.len();
            if let Some(record) = found.iter().flatten().find(|record| record.is_live()) {
                rows.push((key, Arc::clone(record)));
            }
        }
        rows.sort_by(|a, b| a.0.cmp_page_order(&b.0));
        Ok(rows)
    }

    /// A page over every scope. Each scope walks its own live records; the
    /// first scope with a key keeps it. The page applies to the merged rows.
    async fn received_walk(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
        scopes: &[Option<String>],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let walk = whole_walk();
        let (offset, limit) = match filter {
            Some(HashRangeFilter::Page { offset, limit }) => (*offset, *limit),
            _ => (0, DEFAULT_UNFILTERED_PAGE_LIMIT),
        };
        let records = self.db_ops.records();
        let mut merged = HashMap::new();
        for scope in scopes {
            for (key, record) in records
                .select_records(schema, comparison, Some(&walk), scope.as_deref())
                .await?
            {
                merged.entry(key).or_insert(record);
            }
        }
        let mut rows: Vec<KeyedRecord> = merged.into_iter().collect();
        rows.sort_by(|a, b| a.0.cmp_page_order(&b.0));
        Ok(rows.into_iter().skip(offset).take(limit).collect())
    }
}

/// Every live record of one scope, in page order.
fn whole_walk() -> HashRangeFilter {
    HashRangeFilter::Page {
        offset: 0,
        limit: usize::MAX,
    }
}

/// The API key of a keyed selector. A Hash schema has no range.
fn api_key(ranged: bool, hash: &str, range: &str) -> KeyValue {
    KeyValue::new(Some(hash.to_owned()), ranged.then(|| range.to_owned()))
}

/// The API hash and the range bound of a selector that reads one range under
/// one hash. A Hash schema has one record for each hash, so its selectors
/// read by key.
pub(super) fn range_under_hash<'a>(
    schema: &Schema,
    filter: Option<&'a HashRangeFilter>,
) -> Option<(&'a str, RecordBound<'a>)> {
    if schema.schema_type != DeclarativeSchemaType::HashRange {
        return None;
    }
    match filter? {
        HashRangeFilter::HashKey(hash) => Some((hash.as_str(), RecordBound::All)),
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            Some((hash.as_str(), RecordBound::Prefix(prefix)))
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Some((hash.as_str(), RecordBound::Between(start, end)))
        }
        _ => None,
    }
}
