//! Single-pass range query execution over `HashRange` partitions.

use super::QueryExecutor;
use crate::db_operations::record_store::{
    KeyedRecord, RecordBound, RecordSpan, RecordStore, SpanKey,
};
use crate::db_operations::{note_page_resume, page_can_resume, PageResume};
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use std::collections::VecDeque;
use std::sync::Arc;

const PAGE_READ_KEYS: usize = 8_192;
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

struct RangeKeys {
    span: Option<RecordSpan>,
    keys: VecDeque<SpanKey>,
    cursor: Option<String>,
    done: bool,
}

impl RangeKeys {
    fn new(span: Option<RecordSpan>) -> Self {
        Self {
            done: span.is_none(),
            span,
            keys: VecDeque::new(),
            cursor: None,
        }
    }

    async fn fill(
        &mut self,
        records: &RecordStore,
        wanted: usize,
        room: usize,
    ) -> Result<usize, SchemaError> {
        let Some(span) = self.span.as_ref().filter(|_| !self.done) else {
            return Ok(0);
        };
        let ask = wanted.saturating_sub(self.keys.len()).min(room);
        if ask == 0 {
            return Ok(0);
        }
        let page = records
            .record_span_page(span, self.cursor.as_deref(), ask)
            .await?;
        let read = page.keys.len();
        self.keys.extend(page.keys);
        self.done = page.next.is_none();
        if page.next.is_some() {
            self.cursor = page.next;
        }
        Ok(read)
    }

    fn next_live(&mut self, step: usize) -> Option<Vec<SpanKey>> {
        let bound = self
            .cursor
            .as_deref()
            .zip(self.span.as_ref().filter(|_| !self.done))
            .map(|(c, s)| s.storage_range(c).to_owned());
        let n = self
            .keys
            .iter()
            .take(step)
            .take_while(|k| bound.as_deref().is_none_or(|b| k.range.as_str() <= b))
            .count();
        (n > 0).then(|| {
            self.keys
                .drain(..n)
                .filter(|k| k.record.is_live())
                .collect()
        })
    }
}

impl QueryExecutor {
    pub(super) async fn range_rows(
        &self,
        schema: &Schema,
        cmp: &KeyComparisonContext,
        hash: &str,
        bound: RecordBound<'_>,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let resumable = window.is_some() && page_can_resume();
        let (after, mut skip, limit) = match window {
            None => (None, 0, usize::MAX),
            Some(KeyWindow::Offset { offset, limit }) => (None, offset, limit),
            Some(KeyWindow::After { after, limit }) => (Some(after), 0, limit),
        };
        let records = self.db_ops.records();
        let from = after.as_ref().and_then(|a| a.range.as_deref());
        let mut source = RangeKeys::new(records.record_span(schema, hash, bound, from)?);
        let (mut rows, mut cold, mut room, mut wide) =
            (Vec::new(), Vec::new(), PAGE_READ_KEYS, 1usize);
        while rows.len() < limit {
            let step = skip
                .saturating_add(limit - rows.len())
                .saturating_mul(wide)
                .clamp(MIN_STEP, MAX_STEP);
            room = room.saturating_sub(source.fill(records, step, room).await?);
            let Some(taken) = source.next_live(step) else {
                if source.done {
                    break;
                }
                if let Some(stop) = range_stop(
                    records,
                    cmp,
                    &source,
                    resumable && skip == 0,
                    after.as_ref(),
                )? {
                    note_page_resume(PageResume::Key(stop));
                    break;
                }
                return Err(range_limit_err(skip));
            };
            let mark = (rows.len(), skip);
            let mut kept = 0usize;
            for w in taken {
                if let Some(a) = after.as_ref() {
                    if cmp.cmp_with_context(&w.key, a)?.is_le() {
                        continue;
                    }
                }
                if skip > 0 {
                    skip -= 1;
                } else {
                    if let Some(slot) = w.cold {
                        cold.push((slot, Arc::clone(&w.record)));
                    }
                    rows.push((w.key, w.record));
                    kept += 1;
                    if rows.len() == limit {
                        break;
                    }
                }
            }
            room = room.saturating_sub(kept);
            if (rows.len(), skip) == mark {
                wide = wide.saturating_mul(2);
            }
        }
        records.admit_span_records(cold);
        Ok(rows)
    }
}

fn range_stop(
    records: &RecordStore,
    cmp: &KeyComparisonContext,
    source: &RangeKeys,
    allow: bool,
    after: Option<&KeyValue>,
) -> Result<Option<KeyValue>, SchemaError> {
    if !allow {
        return Ok(None);
    }
    let Some(stop) = source
        .span
        .as_ref()
        .zip(source.cursor.as_deref())
        .map(|(span, c)| records.span_key(span, c))
        .transpose()?
    else {
        return Ok(None);
    };
    if let Some(a) = after {
        if !cmp.cmp_with_context(&stop, a)?.is_gt() {
            return Ok(None);
        }
    }
    Ok(Some(stop))
}

fn range_limit_err(skip: usize) -> SchemaError {
    let how = if skip > 0 {
        "read it with next_cursor pages, not an offset"
    } else {
        "read it in pages with next_cursor; a count or a condition needs the complete range"
    };
    SchemaError::ReadResourceLimit {
        detail: format!(
            "the range under one hash holds more deleted records than one request reads ({PAGE_READ_KEYS} keys); {how}"
        ),
    }
}

pub(super) fn range_under_hash<'a>(
    schema: &Schema,
    filter: &'a HashRangeFilter,
) -> Option<(&'a str, RecordBound<'a>)> {
    (schema.schema_type == DeclarativeSchemaType::HashRange)
        .then_some(match filter {
            HashRangeFilter::HashKey(h) => Some((h.as_str(), RecordBound::All)),
            HashRangeFilter::HashRangePrefix { hash, prefix } => {
                Some((hash.as_str(), RecordBound::Prefix(prefix)))
            }
            HashRangeFilter::HashRangeRange { hash, start, end } => {
                Some((hash.as_str(), RecordBound::Between(start, end)))
            }
            HashRangeFilter::HashRangeKey { .. } | HashRangeFilter::HashRangeKeys(_) => None,
        })
        .flatten()
}
