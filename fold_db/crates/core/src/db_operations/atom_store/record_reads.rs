//! Record reads of a format 7 home: by key, by one range under one hash, or
//! by one walk of the record space of a schema.
//!
//! A keyed selector echoes the caller's API hash. A range under one hash
//! decodes each range from its stored key. A walk cannot decode a blinded
//! hash, so it recovers each API key from the key fields of the record; the
//! write path puts those fields in every record.

use super::AtomStore;
use crate::schema::types::field::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::schema::types::field::{
    build_storage_key, FilterUtils, HashRangeFilter, SAMPLE_PEEK_CAP,
};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder, Schema};
use crate::schema::SchemaError;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// One live record and its API key. A read by key can share the record with
/// the record warm set.
pub(crate) type KeyedRecord = (KeyValue, Arc<StoredRecord>);

/// One page of record keys: the API keys, the exclusive physical cursor of
/// the next page, and whether more keys follow.
pub(crate) type RecordKeyPage = (Vec<(String, String)>, Option<String>, bool);

/// The ranges under one hash that one selector reads.
enum RangeBound<'a> {
    All,
    Prefix(&'a str),
    Between(&'a str, &'a str),
}

impl AtomStore {
    /// True when this store serves a format 7 home.
    pub(crate) fn record_home(&self) -> bool {
        self.namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
            .is_some_and(|runtime| runtime.record_home())
    }

    /// The live records that one canonical key selector names, in page
    /// order. `None` reads the first page of the default size.
    pub(crate) async fn select_records(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        filter: Option<&HashRangeFilter>,
        scope: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        use HashRangeFilter as F;
        let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
        let mut page = None;
        let mut rows = match filter {
            Some(F::HashRangeKey { hash, range }) => {
                self.records_at(schema, &[api_key(ranged, hash, range)], scope)
                    .await?
            }
            Some(F::HashRangeKeys(keys)) => {
                let keys: Vec<_> = keys
                    .iter()
                    .map(|(hash, range)| api_key(ranged, hash, range))
                    .collect();
                self.records_at(schema, &keys, scope).await?
            }
            // A Hash schema has one record for each hash.
            Some(
                F::HashKey(hash) | F::HashRangePrefix { hash, .. } | F::HashRangeRange { hash, .. },
            ) if !ranged => {
                self.records_at(schema, &[KeyValue::new(Some(hash.clone()), None)], scope)
                    .await?
            }
            Some(F::HashKey(hash)) => {
                self.records_under_hash(schema, hash, RangeBound::All, scope)
                    .await?
            }
            Some(F::HashRangePrefix { hash, prefix }) => {
                self.records_under_hash(schema, hash, RangeBound::Prefix(prefix), scope)
                    .await?
            }
            Some(F::HashRangeRange { hash, start, end }) => {
                self.records_under_hash(schema, hash, RangeBound::Between(start, end), scope)
                    .await?
            }
            Some(F::SampleN(count)) => {
                self.walk_records(
                    schema,
                    comparison,
                    Some((*count).min(SAMPLE_PEEK_CAP)),
                    scope,
                )
                .await?
            }
            Some(F::Page { offset, limit }) => {
                page = Some((*offset, *limit));
                self.walk_records(schema, comparison, None, scope).await?
            }
            None => {
                page = Some((0, DEFAULT_UNFILTERED_PAGE_LIMIT));
                self.walk_records(schema, comparison, None, scope).await?
            }
            Some(_) => {
                return Err(SchemaError::InvalidData(
                    "a format 7 home reads records only by key, by one hash, or by page".into(),
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
        scope: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let mut seen = HashSet::new();
        let keys: Vec<KeyValue> = keys
            .iter()
            .filter(|key| seen.insert(*key))
            .cloned()
            .collect();
        let storage_keys = keys
            .iter()
            .map(|key| self.record_key(schema, key, scope))
            .collect::<Result<Vec<_>, _>>()?;
        let records = self.read_records(&storage_keys).await?;
        Ok(keys
            .into_iter()
            .zip(records)
            .filter_map(|(key, record)| record.map(|record| (key, record)))
            .collect())
    }

    /// One ordered range read of the records under one API hash.
    async fn records_under_hash(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RangeBound<'_>,
        scope: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let space = record_space_uuid(&schema.name);
        let codec = self.key_codec_for_molecule(&space)?;
        let storage_hash = codec.storage_hash(&space, hash).map_err(codec_error)?;
        let base = build_storage_key(scope, &record_hash_prefix(&space, &storage_hash));
        let store = self.main_store.inner();
        let rows = match bound {
            RangeBound::All => store.scan_prefix(base.as_bytes()).await,
            RangeBound::Prefix(prefix) => {
                codec.require_text_range_prefix().map_err(codec_error)?;
                let prefix = codec.storage_range(&space, prefix).map_err(codec_error)?;
                store
                    .scan_prefix(format!("{base}{prefix}").as_bytes())
                    .await
            }
            RangeBound::Between(start, end) => {
                let start = codec.storage_range(&space, start).map_err(codec_error)?;
                let end = codec.storage_range(&space, end).map_err(codec_error)?;
                if start >= end {
                    return Ok(Vec::new());
                }
                store
                    .scan_range(
                        format!("{base}{start}").as_bytes(),
                        format!("{base}{end}").as_bytes(),
                    )
                    .await
            }
        }
        .map_err(|error| SchemaError::InvalidData(format!("read records under a hash: {error}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let stored = key
                .get(base.len()..)
                .and_then(|range| std::str::from_utf8(range).ok())
                .ok_or_else(|| SchemaError::InvalidData("a record key has no range".into()))?;
            let range = codec.api_range(stored).map_err(codec_error)?;
            out.push((
                KeyValue::new(Some(hash.to_owned()), Some(range)),
                Arc::new(decode(&value)?),
            ));
        }
        Ok(out)
    }

    /// One walk of the record space of a schema: every record, or the first
    /// `limit` in storage order. The store's read allowance bounds the walk.
    async fn walk_records(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        limit: Option<usize>,
        scope: Option<&str>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let prefix = build_storage_key(scope, &space_prefix(schema));
        let store = self.main_store.inner();
        let rows = match limit {
            Some(limit) => store.scan_prefix_paged(prefix.as_bytes(), limit).await,
            None => store.scan_prefix(prefix.as_bytes()).await,
        }
        .map_err(|error| SchemaError::InvalidData(format!("walk records: {error}")))?;
        let mut out = Vec::with_capacity(rows.len());
        for (_, value) in rows {
            let record = decode(&value)?;
            if record.is_live() {
                out.push((
                    recovered_key(schema, comparison, &record)?,
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
        scope: Option<&str>,
    ) -> Result<RecordKeyPage, SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let space = record_space_uuid(&schema.name);
        let base = match hash {
            Some(hash) => {
                let codec = self.key_codec_for_molecule(&space)?;
                record_hash_prefix(
                    &space,
                    &codec.storage_hash(&space, hash).map_err(codec_error)?,
                )
            }
            None => space_prefix(schema),
        };
        let scope_prefix = build_storage_key(scope, "");
        let prefix = build_storage_key(scope, &base);
        let start = match after.map(|cursor| cursor.strip_prefix(&scope_prefix).unwrap_or(cursor)) {
            Some(cursor) if !cursor.starts_with(&base) => {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's records".into(),
                ))
            }
            // The smallest key after the cursor.
            Some(cursor) => build_storage_key(scope, &format!("{cursor}\0")),
            None => prefix.clone(),
        };
        let end = FilterUtils::create_prefix_end(&prefix)?;
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
}

/// `rk:{S}:`: the prefix of every record of one schema.
fn space_prefix(schema: &Schema) -> String {
    format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name))
}

/// The API key of a keyed selector. A Hash schema has no range.
fn api_key(ranged: bool, hash: &str, range: &str) -> KeyValue {
    KeyValue::new(Some(hash.to_owned()), ranged.then(|| range.to_owned()))
}

/// The canonical API key from the key fields of a live record.
fn recovered_key(
    schema: &Schema,
    comparison: &KeyComparisonContext,
    record: &StoredRecord,
) -> Result<KeyValue, SchemaError> {
    let config = schema.key.as_ref().ok_or_else(|| {
        SchemaError::InvalidData(format!("schema '{}' has no key configuration", schema.name))
    })?;
    let fields: HashMap<String, serde_json::Value> = record
        .fields
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let key = KeyValue::from_mutation(&fields, config);
    if key.hash.is_none() {
        return Err(SchemaError::InvalidData(
            "a stored record lacks its hash key field".into(),
        ));
    }
    comparison.canonical_key(key)
}

fn decode(bytes: &[u8]) -> Result<StoredRecord, SchemaError> {
    StoredRecord::decode(bytes)
        .map_err(|error| SchemaError::InvalidData(format!("a stored record: {error}")))
}

fn codec_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
