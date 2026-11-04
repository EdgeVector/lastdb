//! A range read under one hash.
//!
//! The read takes the records of the range one bounded page at a time, in
//! storage order. A step takes the keys up to the last key the read examined;
//! each live record gives a row. The step size follows the window, and the
//! read ends when the window has its rows. A long range costs its window, not
//! its length.
//!
//! The rows that the window keeps are read once more by key, through the
//! record warm set (decision D2): a warm read answers from memory, and a cold
//! read puts the records in memory. A deleted record and a row that the offset
//! skips cost only their examined key.
//!
//! A deleted record is a key too. One page reads at most `PAGE_READ_KEYS`
//! keys, so a run of deleted records cannot use up the read budget of the
//! request. A page that reaches that allowance before its window is full ends
//! early when its caller continues it (`page_can_resume`): it names the last
//! key it examined, and the next page starts after that key. A read that needs
//! the complete range (a count, a condition pass) fails with a read limit.

use crate::db_operations::record_store::{
    KeyedRecord, RecordBound, RecordSpan, RecordStore, SpanKey, PAGE_READ_KEYS,
};
use crate::db_operations::{note_page_resume, page_can_resume, PageResume};
use crate::fold_db_core::query::hash_range_query::HashRangeQueryProcessor;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, KeyValuePageOrder};
use crate::schema::{Schema, SchemaError};
use std::collections::VecDeque;

/// The smallest and the largest number of keys one step reads.
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

/// The unread records of the range.
struct RangeKeys {
    span: Option<RecordSpan>,
    keys: VecDeque<SpanKey>,
    /// The physical key of the last key the read examined.
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

    /// Hold at least `wanted` records, or every record that remains, with a
    /// read of at most `room` keys. Returns the number of records read.
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

    /// The storage range of the last key the read examined while it has
    /// unread keys: no key at or below it is unread. `Some(None)` when the
    /// read examined no key yet, `None` when it has no unread key.
    fn examined(&self) -> Option<Option<&str>> {
        let span = self.span.as_ref().filter(|_| !self.done)?;
        Some(
            self.cursor
                .as_deref()
                .map(|cursor| span.storage_range(cursor)),
        )
    }
}

impl HashRangeQueryProcessor {
    /// The rows of one range under one hash, in page order and
    /// bounded by the window.
    pub(super) async fn range_rows(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let resumable = window.is_some() && page_can_resume();
        let (after, mut skip, limit) = match window {
            None => (None, 0, usize::MAX),
            Some(KeyWindow::Offset { offset, limit }) => (None, offset, limit),
            Some(KeyWindow::After { after, limit }) => (Some(after), 0, limit),
            Some(KeyWindow::AfterRecord { .. }) => return Err(record_cursor_error()),
        };
        let records = self.db_ops.records();
        let from_range = after.as_ref().and_then(|after| after.range.as_deref());
        let mut source = RangeKeys::new(records.record_span(schema, hash, bound, from_range)?);
        let mut rows = Vec::new();
        let mut room = PAGE_READ_KEYS;
        // A step that finds no row (a run of deleted records) doubles the next.
        let mut wide = 1usize;
        while rows.len() < limit {
            let step = skip
                .saturating_add(limit - rows.len())
                .saturating_mul(wide)
                .clamp(MIN_STEP, MAX_STEP);
            let read = source.fill(records, step, room).await?;
            room = room.saturating_sub(read);
            let Some(taken) = next_keys(&mut source, step) else {
                if source.done {
                    break;
                }
                // Unread keys remain but nothing is left to read in this
                // request: end the page after the last key examined. The
                // next page must start past the cursor of this one.
                let moved = |stop: &KeyValue| {
                    after
                        .as_ref()
                        .is_none_or(|after| stop.cmp_page_order(after).is_gt())
                };
                match stop_key(records, &source)? {
                    Some(stop) if resumable && skip == 0 && moved(&stop) => {
                        note_page_resume(PageResume::Key(stop));
                        break;
                    }
                    _ => return Err(range_limit_error(skip > 0)),
                }
            };
            let mark = (rows.len(), skip);
            let mut kept = Vec::new();
            for winner in taken {
                // The span starts at the range of the cursor. The cursor is a
                // position in page order: a cursor of another hash can tie
                // on the range.
                if after
                    .as_ref()
                    .is_some_and(|after| winner.key.cmp_page_order(after).is_le())
                    || !winner.record.is_live()
                {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                kept.push(winner);
                if rows.len() + kept.len() == limit {
                    break;
                }
            }
            room = room.saturating_sub(kept.len());
            rows.extend(read_kept(records, kept).await?);
            if (rows.len(), skip) == mark {
                wide = wide.saturating_mul(2);
            }
        }
        Ok(rows)
    }
}

/// The live keys of the next step: at most `step` keys, each at or below the
/// last key the read examined, so no unread key comes before a key taken. A
/// deleted record gives no row, so a step of deleted records takes keys and
/// gives no row. The taken keys leave the source. `None` when no key can be
/// taken.
fn next_keys(source: &mut RangeKeys, step: usize) -> Option<Vec<SpanKey>> {
    let bound = match source.examined() {
        Some(None) => return None,
        Some(Some(examined)) => Some(examined.to_owned()),
        None => None,
    };
    let count = source
        .keys
        .iter()
        .take(step)
        .take_while(|key| {
            bound
                .as_deref()
                .is_none_or(|bound| key.range.as_str() <= bound)
        })
        .count();
    if count == 0 {
        return None;
    }
    Some(
        source
            .keys
            .drain(..count)
            .filter(|key| key.record.is_live())
            .collect(),
    )
}

/// The rows that one step keeps, read once more by key through the record
/// warm set: memory answers first, and a record that the read finds on disk
/// enters memory. At each key the read gives the current record; a record
/// that is no longer live gives no row.
async fn read_kept(
    record_store: &RecordStore,
    kept: Vec<SpanKey>,
) -> Result<Vec<KeyedRecord>, SchemaError> {
    if kept.is_empty() {
        return Ok(Vec::new());
    }
    let (keys, physical): (Vec<KeyValue>, Vec<String>) = kept
        .into_iter()
        .map(|kept| (kept.key, kept.physical))
        .unzip();
    let records = record_store.read_records(&physical).await?;
    Ok(keys
        .into_iter()
        .zip(records)
        .filter_map(|(key, record)| {
            record
                .filter(|record| record.is_live())
                .map(|record| (key, record))
        })
        .collect())
}

/// The API key of the last key the read examined while unread keys remain.
/// `None` when the read examined no key or has no unread key.
fn stop_key(records: &RecordStore, source: &RangeKeys) -> Result<Option<KeyValue>, SchemaError> {
    let Some(span) = source.span.as_ref().filter(|_| !source.done) else {
        return Ok(None);
    };
    source
        .cursor
        .as_deref()
        .map(|cursor| records.span_key(span, cursor))
        .transpose()
}

fn range_limit_error(offset: bool) -> SchemaError {
    let continuation = if offset {
        "read it with next_cursor pages, not an offset"
    } else {
        "read it in pages with next_cursor; a count or a condition needs the complete range"
    };
    SchemaError::ReadResourceLimit {
        detail: format!(
            "the range under one hash holds more deleted records than one request reads \
             ({PAGE_READ_KEYS} keys); {continuation}"
        ),
    }
}

fn record_cursor_error() -> SchemaError {
    SchemaError::InvalidCursor(
        "a record walk cursor continues only an unfiltered read; continue a read of one hash \
         with the next_cursor of that read"
            .into(),
    )
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
