//! Record reads of a home: by key, or by one walk of the record
//! space of a schema. A range under one hash reads its keys in pages
//! (`record_spans.rs`). An unfiltered page walks in record key order, one
//! bounded page at a time (`record_walk.rs`).
//!
//! The stored record carries its API address independently of its fields.
//! Physical addresses retain their blinded hashes and typed range ordering.

use super::record_walk::{WalkWindow, PAGE_READ_KEYS};
use super::RecordStore;
use crate::db_operations::{note_page_key_forms, note_page_resume, page_can_resume, PageResume};
use crate::schema::types::field::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::schema::types::field::{FilterUtils, HashRangeFilter, SAMPLE_PEEK_CAP};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Schema};
use crate::schema::SchemaError;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use std::collections::HashSet;
use std::sync::Arc;

/// One live record and its API key. A read by key can share the record with
/// the record warm set.
pub(crate) type KeyedRecord = (KeyValue, Arc<StoredRecord>);

/// One page of record keys: the API keys, the exclusive physical cursor of
/// the next page, and whether more keys follow.
pub(crate) type RecordKeyPage = (Vec<(String, String)>, Option<String>, bool);

impl RecordStore {
    /// The live records that one canonical key selector names, in page
    /// order. `None` reads the first page of the default size in record key
    /// order (`record_walk.rs`). A range under one hash of a HashRange schema
    /// reads its keys in pages (`record_spans.rs`), so this read refuses it.
    pub(crate) async fn select_records(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        use HashRangeFilter as F;
        let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
        let mut page = None;
        let mut rows = match filter {
            Some(F::HashRangeKey { hash, range }) => {
                self.records_at(schema, &[api_key(ranged, hash, range)])
                    .await?
            }
            Some(F::HashRangeKeys(keys)) => {
                let keys: Vec<_> = keys
                    .iter()
                    .map(|(hash, range)| api_key(ranged, hash, range))
                    .collect();
                self.records_at(schema, &keys).await?
            }
            // A Hash schema has one record for each hash.
            Some(
                F::HashKey(hash) | F::HashRangePrefix { hash, .. } | F::HashRangeRange { hash, .. },
            ) if !ranged => {
                self.records_at(schema, &[KeyValue::new(Some(hash.clone()), None)])
                    .await?
            }
            // A sample is the first live records in record key order. The
            // walk reads past deleted records in bounded pages, so deleted
            // records do not use up the sample.
            Some(F::SampleN(count)) => {
                let take = (*count).min(SAMPLE_PEEK_CAP);
                self.walk_first(schema, comparison, take).await?
            }
            // A `Page` is a window over every live record in page order: the
            // count and the condition reads need the complete set. The read
            // allowance of the request bounds it.
            Some(F::Page { offset, limit }) => {
                page = Some((*offset, *limit));
                self.walk_records(schema, comparison).await?
            }
            // The first page of the default size, in record key order.
            None => {
                self.walk_first(schema, comparison, DEFAULT_UNFILTERED_PAGE_LIMIT)
                    .await?
            }
            Some(_) => {
                return Err(SchemaError::InvalidData(
                    "records are read only by key, by one hash, or by page".into(),
                ))
            }
        };
        rows.retain(|(_, record)| record.is_live());
        rows.sort_by(|a, b| a.0.cmp_page_order(&b.0));
        if let Some((offset, limit)) = page {
            rows = rows.into_iter().skip(offset).take(limit).collect();
        }
        Ok(rows)
    }

    /// The records at these API keys. A repeated key reads once.
    pub(crate) async fn records_at(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let mut seen = HashSet::new();
        let keys: Vec<KeyValue> = keys
            .iter()
            .filter(|key| seen.insert(*key))
            .cloned()
            .collect();
        let storage_keys = keys
            .iter()
            .map(|key| self.record_key(schema, key))
            .collect::<Result<Vec<_>, _>>()?;
        let records = self.read_records(&storage_keys).await?;
        Ok(keys
            .into_iter()
            .zip(records)
            .filter_map(|(key, record)| record.map(|record| (key, record)))
            .collect())
    }

    /// The first `take` live records of a schema in record key order
    /// (`record_walk.rs`).
    ///
    /// A run of deleted records can use the read allowance before `take`
    /// records are found. A caller that continues a page that ends early
    /// gets the short page and the record key position where the walk
    /// stopped (`PageResume::Record`); any other caller gets a read limit
    /// error, never a short page that looks complete.
    async fn walk_first(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        take: usize,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let window = WalkWindow {
            after: None,
            skip: 0,
            take,
        };
        let mut page = self
            .walk_record_window(schema, comparison, window, |_| true)
            .await?;
        if let Some(stopped) = page.stopped.take() {
            if !page_can_resume() {
                return Err(SchemaError::ReadResourceLimit {
                    detail: format!(
                        "a sample passed {PAGE_READ_KEYS} deleted records before it was full; \
                         read the schema in pages with next_cursor"
                    ),
                });
            }
            note_page_resume(PageResume::Record(stopped));
        }
        Ok(page.into_keyed())
    }

    /// One walk of every record in the record space of a schema. The store's
    /// read allowance bounds the walk.
    async fn walk_records(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let prefix = space_prefix(schema);
        let rows = self
            .main_store
            .inner()
            .scan_prefix(prefix.as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("walk records: {error}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let record = decode(&value)?;
            if record.is_live() {
                let key = String::from_utf8(key).unwrap_or_default();
                let suffix = key.strip_prefix(prefix.as_str()).unwrap_or_default();
                out.push((
                    walk_key(schema, comparison, &record, suffix)?,
                    Arc::new(record),
                ));
            }
        }
        Ok(out)
    }

    /// One page of the live record keys of a schema, optionally under one
    /// API hash. The cursor is the exclusive physical key of the last record
    /// of the previous page.
    pub(crate) async fn list_record_keys(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        limit: usize,
        after: Option<&str>,
        hash: Option<&str>,
    ) -> Result<RecordKeyPage, SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let space = record_space_uuid(&schema.name);
        let base = match hash {
            Some(hash) => {
                let codec = self.key_codec_for_field_key(&space)?;
                record_hash_prefix(
                    &space,
                    &codec.storage_hash(&space, hash).map_err(codec_error)?,
                )
            }
            None => space_prefix(schema),
        };
        let start = match after {
            Some(cursor) if !cursor.starts_with(&base) => {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's records".into(),
                ))
            }
            // The smallest key after the cursor.
            Some(cursor) => format!("{cursor}\0"),
            None => base.clone(),
        };
        let end = FilterUtils::create_prefix_end(&base)?;
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit.saturating_add(1))
            .await
            .map_err(|error| SchemaError::InvalidData(format!("list record keys: {error}")))?;
        let has_more = rows.len() > limit;
        let mut keys = Vec::with_capacity(limit);
        let mut cursor = None;
        for (key, value) in rows.into_iter().take(limit) {
            let record = decode(&value)?;
            let physical = String::from_utf8(key)
                .map_err(|_| SchemaError::InvalidData("a record key is not text".into()))?;
            if record.is_live() {
                let key = recovered_key(schema, comparison, &record)?;
                keys.push((key.hash.unwrap_or_default(), key.range.unwrap_or_default()));
            }
            cursor = Some(physical);
        }
        Ok((keys, cursor.filter(|_| has_more), has_more))
    }

    /// Whether the record space of one schema may hold a live
    /// record. One bounded read of at most `RECORD_PROBE_ROWS` records, never
    /// a walk of the space. When that page holds only deleted records and
    /// more follow, the answer is `true`: the callers guard against data
    /// loss, so an unproven absence must not read as an empty schema.
    pub async fn schema_may_hold_records(&self, schema: &Schema) -> Result<bool, SchemaError> {
        self.record_read_scope(async {
            let prefix = space_prefix(schema);
            let end = FilterUtils::create_prefix_end(&prefix)?;
            let rows = self
                .main_store
                .inner()
                .scan_range_paged(prefix.as_bytes(), end.as_bytes(), RECORD_PROBE_ROWS + 1)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("probe records: {error}")))?;
            if rows.len() > RECORD_PROBE_ROWS {
                return Ok(true);
            }
            for (_, value) in &rows {
                if decode(value)?.is_live() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }
}

/// The record count one existence probe reads.
const RECORD_PROBE_ROWS: usize = 16;

/// `rk:{S}:`: the prefix of every record of one schema.
pub(super) fn space_prefix(schema: &Schema) -> String {
    format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name))
}

/// The API key of a keyed selector. A Hash schema has no range.
fn api_key(ranged: bool, hash: &str, range: &str) -> KeyValue {
    KeyValue::new(Some(hash.to_owned()), ranged.then(|| range.to_owned()))
}

/// A complete record retains its address independently of its payload fields.
fn recovered_key(
    _schema: &Schema,
    comparison: &KeyComparisonContext,
    record: &StoredRecord,
) -> Result<KeyValue, SchemaError> {
    comparison.canonical_key(record.key.clone())
}

pub(super) fn walk_key(
    schema: &Schema,
    comparison: &KeyComparisonContext,
    record: &StoredRecord,
    _suffix: &str,
) -> Result<KeyValue, SchemaError> {
    note_page_key_forms(1, 0);
    recovered_key(schema, comparison, record)
}

pub(super) fn decode(bytes: &[u8]) -> Result<StoredRecord, SchemaError> {
    StoredRecord::decode(bytes)
        .map_err(|error| SchemaError::InvalidData(format!("a stored record: {error}")))
}

fn codec_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
