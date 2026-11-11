//! Paged record reads under one hash partition.

use super::reads::decode;
use super::RecordStore;
use crate::schema::types::field::FilterUtils;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use lastdb_model::field_key_codec::FieldKeyCodec;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid};
use lastdb_model::record_value::StoredRecord;

#[derive(Clone, Copy)]
pub(crate) enum RecordBound<'a> {
    All,
    Prefix(&'a str),
    Between(&'a str, &'a str),
}

pub(crate) struct RecordSpan {
    hash: String,
    codec: FieldKeyCodec,
    base: String,
    start: String,
    end: String,
}

pub(crate) struct SpanKey {
    pub(crate) range: String,
    pub(crate) key: KeyValue,
    pub(crate) physical: String,
    pub(crate) record: StoredRecord,
}

pub(crate) struct SpanPage {
    pub(crate) keys: Vec<SpanKey>,
    pub(crate) next: Option<String>,
}

impl RecordSpan {
    pub(crate) fn storage_range<'a>(&self, physical: &'a str) -> &'a str {
        physical.get(self.base.len()..).unwrap_or_default()
    }
}

impl RecordStore {
    pub(crate) fn record_span(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        from_range: Option<&str>,
    ) -> Result<Option<RecordSpan>, SchemaError> {
        let space = record_space_uuid(&schema.name);
        let codec = self.primary_codec(schema)?;
        let storage_hash = codec.storage_hash(&space, hash).map_err(codec_error)?;
        let base = record_hash_prefix(&space, &storage_hash);
        let stored = |range: &str| {
            codec
                .storage_range(&space, range)
                .map(|range| format!("{base}{range}"))
                .map_err(codec_error)
        };
        let (mut start, end) = match bound {
            RecordBound::All => (base.clone(), FilterUtils::create_prefix_end(&base)?),
            RecordBound::Prefix(prefix) => {
                codec.require_text_range_prefix().map_err(codec_error)?;
                let start = stored(prefix)?;
                let end = FilterUtils::create_prefix_end(&start)?;
                (start, end)
            }
            RecordBound::Between(start, end) => (stored(start)?, stored(end)?),
        };
        if let Some(from) = from_range.and_then(|range| stored(range).ok()) {
            start = start.max(from);
        }
        if start >= end {
            return Ok(None);
        }
        Ok(Some(RecordSpan {
            hash: hash.to_owned(),
            codec,
            base,
            start,
            end,
        }))
    }

    pub(crate) async fn record_span_page(
        &self,
        span: &RecordSpan,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SpanPage, SchemaError> {
        let start = after.map_or_else(|| span.start.clone(), |cursor| format!("{cursor}\0"));
        let page = self
            .main_store
            .scan_range_yielding(start.as_bytes(), span.end.as_bytes(), limit, 2)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read record keys: {error}")))?;
        let mut keys = Vec::with_capacity(page.rows.len());
        for (physical, value) in page.rows {
            let physical = text(physical)?;
            let range = physical
                .get(span.base.len()..)
                .ok_or_else(|| SchemaError::InvalidData("a record key has no range".into()))?
                .to_owned();
            let api_range = span.codec.api_range(&range).map_err(codec_error)?;
            keys.push(SpanKey {
                range,
                key: KeyValue::new(Some(span.hash.clone()), Some(api_range)),
                physical,
                record: decode(&value)?,
            });
        }
        let next = page.resume.map(text).transpose()?;
        Ok(SpanPage { keys, next })
    }

    pub(crate) fn span_key(
        &self,
        span: &RecordSpan,
        physical: &str,
    ) -> Result<KeyValue, SchemaError> {
        let range = physical
            .get(span.base.len()..)
            .ok_or_else(|| SchemaError::InvalidData("a record key has no range".into()))?;
        let api_range = span.codec.api_range(range).map_err(codec_error)?;
        Ok(KeyValue::new(Some(span.hash.clone()), Some(api_range)))
    }
}

fn text(key: Vec<u8>) -> Result<String, SchemaError> {
    String::from_utf8(key).map_err(|_| SchemaError::InvalidData("a record key is not text".into()))
}

fn codec_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
