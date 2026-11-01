//! Paged key reads of the records under one hash in one storage scope.
//!
//! A read that merges the records of several scopes (the own scope and the
//! scopes of data received from other users) takes the keys of each scope one
//! bounded page at a time, in storage order. The typed range codec keeps the
//! storage order of one hash in page order, so the merge needs no read past
//! the keys of its window. A page carries only keys: the merge reads the
//! records it keeps by key, through the record warm set.

use super::AtomStore;
use crate::atom::MoleculeKeyCodec;
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use lastdb_model::record_space::{record_hash_prefix, record_space_uuid};

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

/// One record key of a span: the storage range (the merge order), the API
/// key and the physical key of the record.
pub(crate) struct SpanKey {
    pub(crate) range: String,
    pub(crate) key: KeyValue,
    pub(crate) physical: String,
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

    /// At most `limit` (one or more) record keys of a span in storage order,
    /// after the exclusive physical cursor `after`. The next cursor is the
    /// last key of a full page; `None` when the span has no more keys.
    /// Tombstones are keys too: the caller reads each record and keeps the
    /// live ones.
    pub(crate) async fn record_span_page(
        &self,
        span: &RecordSpan,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<SpanKey>, Option<String>), SchemaError> {
        // The smallest key after the cursor.
        let start = after.map_or_else(|| span.start.clone(), |cursor| format!("{cursor}\0"));
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), span.end.as_bytes(), limit)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read record keys: {error}")))?;
        let full = rows.len() == limit;
        let mut keys = Vec::with_capacity(rows.len());
        for (physical, _) in rows {
            let physical = String::from_utf8(physical)
                .map_err(|_| SchemaError::InvalidData("a record key is not text".into()))?;
            let range = physical
                .get(span.base.len()..)
                .ok_or_else(|| SchemaError::InvalidData("a record key has no range".into()))?
                .to_owned();
            let api_range = span.codec.api_range(&range).map_err(codec_error)?;
            keys.push(SpanKey {
                range,
                key: KeyValue::new(Some(span.hash.clone()), Some(api_range)),
                physical,
            });
        }
        let next = keys.last().filter(|_| full).map(|key| key.physical.clone());
        Ok((keys, next))
    }
}

fn codec_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
