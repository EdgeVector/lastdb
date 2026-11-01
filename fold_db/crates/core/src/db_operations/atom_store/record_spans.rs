//! Paged record reads under one hash in one storage scope.
//!
//! A read that merges the records of several scopes (the own scope and the
//! scopes of data received from other users) takes the records of each scope
//! one bounded page at a time, in storage order. The typed range codec keeps
//! the storage order of one hash in page order, so the merge needs no read
//! past the keys of its window. A page carries each record with its key, so
//! a deleted record costs one examined key. The merge reads the records it
//! keeps by key once more, through the record warm set (decision D2), so a
//! warm read answers from memory.

use super::record_reads::decode;
use super::AtomStore;
use crate::atom::MoleculeKeyCodec;
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid};
use lastdb_model::record_value::StoredRecord;

/// The ranges under one hash that one selector reads.
#[derive(Clone, Copy)]
pub(crate) enum RecordBound<'a> {
    All,
    Prefix(&'a str),
    Between(&'a str, &'a str),
}

/// The physical key interval `[start, end)` of one bound under one hash in
/// one scope.
pub(crate) struct RecordSpan {
    hash: String,
    codec: MoleculeKeyCodec,
    base: String,
    start: String,
    end: String,
}

/// One record of a span: the storage range (the merge order), the API key,
/// the physical key and the stored record, live or deleted.
pub(crate) struct SpanKey {
    pub(crate) range: String,
    pub(crate) key: KeyValue,
    pub(crate) physical: String,
    pub(crate) record: StoredRecord,
}

/// One page of a span: its records in storage order, and the physical key
/// of the last key the page examined while keys remain (`None` at the end of
/// the span). A page can hold fewer records than it asked for and still not
/// end the span: the request ran low on read keys in a run of deleted keys.
pub(crate) struct SpanPage {
    pub(crate) keys: Vec<SpanKey>,
    pub(crate) next: Option<String>,
}

impl RecordSpan {
    /// The storage range of one physical key of the span.
    pub(crate) fn storage_range<'a>(&self, physical: &'a str) -> &'a str {
        physical.get(self.base.len()..).unwrap_or_default()
    }
}

impl AtomStore {
    /// The span of one bound under one API hash in one scope. `from_range`
    /// moves the start to that API range (inclusive), when the codec can
    /// encode it. `None` when the span is empty.
    pub(crate) fn record_span(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        from_range: Option<&str>,
        scope: Option<&str>,
    ) -> Result<Option<RecordSpan>, SchemaError> {
        let space = record_space_uuid(&schema.name);
        let codec = self.key_codec_for_molecule(&space)?;
        let storage_hash = codec.storage_hash(&space, hash).map_err(codec_error)?;
        let base = build_storage_key(scope, &record_hash_prefix(&space, &storage_hash));
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
        // A cursor that the codec cannot encode starts at the bound; the
        // caller still filters each row by page order.
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

    /// At most `limit` (one or more) records of a span in storage order,
    /// after the exclusive physical cursor `after`. Deleted records are
    /// records too: the caller keeps the live ones.
    pub(crate) async fn record_span_page(
        &self,
        span: &RecordSpan,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SpanPage, SchemaError> {
        // The smallest key after the cursor.
        let start = after.map_or_else(|| span.start.clone(), |cursor| format!("{cursor}\0"));
        // The merge reads a record that it keeps once more, by key.
        let page = self
            .main_store
            .inner()
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

    /// The API key of the physical key `physical` of a span, for a cursor.
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
