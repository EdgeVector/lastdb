//! Record selection, warm-set span pages, alternate index scans, key listing, and probes.

use super::{invalid, RecordSpan, RecordStore, SpanKey, SpanPage};
use crate::schema::types::field::{FilterUtils, HashRangeFilter};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use crate::storage::format::records::{
    alternate_bounds, AlternateAddress, AlternateEntry, MAX_ALTERNATE_ENTRIES,
};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::field_key_id::alternate_field_key_id;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid, RECORD_PREFIX};
use lastdb_model::record_value::StoredRecord;
use std::collections::HashSet;
use std::sync::Arc;

pub(super) const MIN_ROWS: usize = 16;

pub(crate) type KeyedRecord = (KeyValue, Arc<StoredRecord>);

impl RecordStore {
    pub(crate) async fn select_alternate_entries(
        &self,
        schema: &Schema,
        name: &str,
        filter: &HashRangeFilter,
        after: Option<&AlternateEntry>,
        limit: usize,
    ) -> Result<Vec<AlternateEntry>, SchemaError> {
        if limit > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate page exceeds 4096 entries"));
        }
        let def = schema
            .require_record_keys()?
            .alternates
            .get(name)
            .ok_or_else(|| invalid("alternate key is not declared"))?;
        let id = alternate_field_key_id(schema, name);
        let codec = FieldKeyCodec::from_node_key(
            &self.node_key,
            &id,
            def.range_profile().map_err(invalid)?,
        );
        let storage_key = |k: &KeyValue, primary: &KeyValue| {
            AlternateAddress::new(&codec, &id, k, &self.record_address(schema, primary)?)
                .map(AlternateAddress::into_string)
        };
        let (mut start, end) = alternate_bounds(&codec, &id, filter)?;
        if let Some(after) = after {
            let cursor = storage_key(&after.matched, &after.primary)?;
            if cursor < start || cursor >= end {
                return Err(invalid(
                    "alternate cursor is outside the requested key range",
                ));
            }
            start = format!("{cursor}\0");
        }
        self.reader()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
            .await?
            .into_iter()
            .map(|(k, v)| {
                let e: AlternateEntry =
                    serde_json::from_slice(&v).map_err(|_| invalid("invalid alternate entry"))?;
                (storage_key(&e.matched, &e.primary)?.as_bytes() == k)
                    .then_some(e)
                    .ok_or_else(|| {
                        invalid("alternate entry coordinates differ from their storage key")
                    })
            })
            .collect()
    }

    pub(crate) async fn record_span_page(
        &self,
        span: &RecordSpan,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SpanPage, SchemaError> {
        let start = after.map_or_else(|| span.start.clone(), |c| format!("{c}\0"));
        self.record_read_scope(async move {
            let page = self
                .reader()
                .scan_range_yielding(start.as_bytes(), span.end.as_bytes(), limit, 2)
                .await
                .map_err(|e| invalid(format!("read record keys: {e}")))?;
            let text =
                |k: Vec<u8>| String::from_utf8(k).map_err(|_| invalid("a record key is not text"));
            let mut rows = Vec::with_capacity(page.rows.len());
            let mut physicals = Vec::with_capacity(page.rows.len());
            for (raw_key, value) in page.rows {
                physicals.push(text(raw_key)?);
                rows.push(value);
            }
            let mut warm = self
                .runtime
                .warm_records(&physicals)
                .map_err(|e| invalid(format!("read record keys: {e}")))?;
            let found = match &mut warm {
                Some(r) => r.take_found(),
                None => vec![None; physicals.len()],
            };
            let warm = warm.map(Arc::new);
            let mut keys = Vec::with_capacity(physicals.len());
            for (at, ((physical, value), hit)) in
                physicals.into_iter().zip(rows).zip(found).enumerate()
            {
                let key = self.span_key(span, &physical)?;
                let range = physical
                    .get(span.base.len()..)
                    .ok_or_else(|| invalid("a record key has no range"))?
                    .to_owned();
                let (record, cold) = if let Some(cached) = hit {
                    (cached, None)
                } else {
                    let bytes = value.len();
                    (
                        Arc::new(decode(&value)?),
                        warm.as_ref().map(|w| (Arc::clone(w), at, bytes)),
                    )
                };
                keys.push(SpanKey {
                    range,
                    key,
                    record,
                    cold,
                });
            }
            Ok(SpanPage {
                keys,
                next: page.resume.map(text).transpose()?,
            })
        })
        .await
    }

    pub(crate) fn span_key(
        &self,
        span: &RecordSpan,
        physical: &str,
    ) -> Result<KeyValue, SchemaError> {
        let range = physical
            .get(span.base.len()..)
            .ok_or_else(|| invalid("a record key has no range"))?;
        Ok(KeyValue::new(
            Some(span.hash.clone()),
            Some(span.codec.api_range(range).map_err(invalid)?),
        ))
    }

    pub(crate) async fn select_records(
        &self,
        schema: &Schema,
        cmp: &KeyComparisonContext,
        filter: &HashRangeFilter,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        use HashRangeFilter as F;
        let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
        let mk = |h: &str, r: &str| KeyValue::new(Some(h.to_owned()), ranged.then(|| r.to_owned()));
        let requested: Vec<KeyValue> = match filter {
            F::HashRangeKey { hash, range } => vec![mk(hash, range)],
            F::HashRangeKeys(keys) => keys.iter().map(|(h, r)| mk(h, r)).collect(),
            F::HashKey(hash) | F::HashRangePrefix { hash, .. } | F::HashRangeRange { hash, .. }
                if !ranged =>
            {
                vec![KeyValue::new(Some(hash.clone()), None)]
            }
            F::HashKey(_) | F::HashRangePrefix { .. } | F::HashRangeRange { .. } => {
                return Err(invalid(
                    "HashRange range selectors are read through range_rows",
                ));
            }
        };
        let mut seen = HashSet::new();
        let keys: Vec<KeyValue> = requested
            .into_iter()
            .filter(|k| seen.insert(k.clone()))
            .collect();
        let storage_keys = keys
            .iter()
            .map(|k| self.record_key(schema, k))
            .collect::<Result<Vec<_>, _>>()?;
        let mut rows: Vec<KeyedRecord> = keys
            .into_iter()
            .zip(self.read_records(&storage_keys).await?)
            .filter_map(|(k, r)| r.filter(|r| r.is_live()).map(|r| (k, r)))
            .collect();
        cmp.sort_by_key_context(&mut rows, |(k, _)| k)?;
        Ok(rows)
    }

    pub(crate) async fn list_record_keys(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        limit: usize,
        after: Option<&str>,
        hash: Option<&str>,
    ) -> Result<(Vec<(String, String)>, Option<String>, bool), SchemaError> {
        if limit == 0 {
            return Ok((Vec::new(), None, false));
        }
        let space = record_space_uuid(&schema.name);
        let base = match hash {
            Some(h) => record_hash_prefix(
                &space,
                &self
                    .primary_codec(schema)?
                    .storage_hash(&space, h)
                    .map_err(invalid)?,
            ),
            None => format!("{RECORD_PREFIX}{space}:"),
        };
        let start = match after {
            Some(c) if !c.starts_with(&base) => {
                return Err(SchemaError::InvalidCursor(
                    "cursor is not in this schema's records".into(),
                ));
            }
            Some(c) => format!("{c}\0"),
            None => base.clone(),
        };
        let end = FilterUtils::create_prefix_end(&base)?;
        let rows = self
            .reader()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit.saturating_add(1))
            .await
            .map_err(|e| invalid(format!("list record keys: {e}")))?;
        let has_more = rows.len() > limit;
        let (mut keys, mut cursor) = (Vec::with_capacity(limit), None);
        for (key, value) in rows.into_iter().take(limit) {
            let record = decode(&value)?;
            let physical =
                String::from_utf8(key).map_err(|_| invalid("a record key is not text"))?;
            if record.is_live() {
                let k = comparison.canonical_key(record.key)?;
                keys.push((k.hash.unwrap_or_default(), k.range.unwrap_or_default()));
            }
            cursor = Some(physical);
        }
        Ok((keys, cursor.filter(|_| has_more), has_more))
    }

    pub async fn schema_may_hold_records(&self, schema: &Schema) -> Result<bool, SchemaError> {
        self.record_read_scope(async {
            let prefix = format!("{RECORD_PREFIX}{}:", record_space_uuid(&schema.name));
            let end = FilterUtils::create_prefix_end(&prefix)?;
            let rows = self
                .reader()
                .scan_range_paged(prefix.as_bytes(), end.as_bytes(), MIN_ROWS + 1)
                .await
                .map_err(|e| invalid(format!("probe records: {e}")))?;
            if rows.len() > MIN_ROWS {
                return Ok(true);
            }
            for (_, v) in &rows {
                if decode(v)?.is_live() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<StoredRecord, SchemaError> {
    StoredRecord::decode(bytes).map_err(|e| invalid(format!("a stored record: {e}")))
}
