//! A range read under one hash that merges the own scope with the received
//! scopes. A node with no sender reads the own scope alone through the same
//! pages.
//!
//! Each scope reads its records one bounded page at a time, in storage order.
//! A merge step takes the keys that no unread key can precede: every key up to
//! the least key that a scope with unread keys examined last. At each key the
//! first scope with a live record gives the row. The step size follows the
//! window, and the read ends when the window has its rows. A long range costs
//! its window, not its length.
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

use super::has_any;
use crate::db_operations::atom_store::{
    AtomStore, KeyedRecord, RecordBound, RecordSpan, SpanKey, PAGE_READ_KEYS,
};
use crate::db_operations::{note_page_resume, page_can_resume, PageResume};
use crate::fold_db_core::query::hash_range_query::HashRangeQueryProcessor;
use crate::schema::types::field::KeyWindow;
use crate::schema::types::{KeyValue, KeyValuePageOrder};
use crate::schema::{Schema, SchemaError};
use std::collections::{BTreeMap, VecDeque};

/// The smallest and the largest number of keys one merge step reads from
/// each scope.
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

/// The unread records of one scope.
struct ScopeKeys {
    span: Option<RecordSpan>,
    keys: VecDeque<SpanKey>,
    /// The physical key of the last key this scope examined.
    cursor: Option<String>,
    done: bool,
}

impl ScopeKeys {
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
        atoms: &AtomStore,
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
        let page = atoms
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

    /// The storage range of the last key this scope examined while it has
    /// unread keys: no key at or below it is unread. `Some(None)` when the
    /// scope examined no key yet, `None` when it has no unread key.
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
    /// The rows of one range under one hash over `scopes`, in page order and
    /// bounded by the window.
    pub(super) async fn received_range(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        scopes: &[Option<String>],
        selected: &[String],
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let resumable = window.is_some() && page_can_resume();
        let (after, mut skip, limit) = match window {
            None => (None, 0, usize::MAX),
            Some(KeyWindow::Offset { offset, limit }) => (None, offset, limit),
            Some(KeyWindow::After { after, limit }) => (Some(after), 0, limit),
            Some(KeyWindow::AfterRecord { .. }) => return Err(record_cursor_error()),
        };
        let atoms = self.db_ops.atoms();
        let from_range = after.as_ref().and_then(|after| after.range.as_deref());
        let mut sources = Vec::with_capacity(scopes.len());
        for scope in scopes {
            let span = atoms.record_span(schema, hash, bound, from_range, scope.as_deref())?;
            sources.push(ScopeKeys::new(span));
        }
        let mut rows = Vec::new();
        let mut room = PAGE_READ_KEYS;
        // A step that finds no row (a run of deleted records) doubles the next.
        let mut wide = 1usize;
        while rows.len() < limit {
            let step = skip
                .saturating_add(limit - rows.len())
                .saturating_mul(wide)
                .clamp(MIN_STEP, MAX_STEP);
            let share = room / sources.iter().filter(|s| !s.done).count().max(1);
            let read = futures::future::try_join_all(
                sources
                    .iter_mut()
                    .map(|source| source.fill(atoms, step, share)),
            )
            .await?;
            room = room.saturating_sub(read.into_iter().sum());
            let Some(taken) = next_keys(&mut sources, step) else {
                if sources.iter().all(|source| source.done) {
                    break;
                }
                // A scope with unread keys has nothing left to read in this
                // request: end the page after the least key it examined. The
                // next page must start past the cursor of this one.
                let moved = |stop: &KeyValue| {
                    after
                        .as_ref()
                        .is_none_or(|after| stop.cmp_page_order(after).is_gt())
                };
                match stop_key(atoms, &sources)? {
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
                    || !has_any(&winner.record, selected)
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
            rows.extend(read_kept(atoms, kept, selected).await?);
            if (rows.len(), skip) == mark {
                wide = wide.saturating_mul(2);
            }
        }
        Ok(rows)
    }
}

/// The live winners of the next keys of the merged order: at most `step`
/// keys, each at or below the least key that a scope with unread keys
/// examined, so no unread key comes before a key taken. At each key the first
/// scope with a live record wins; a key with no live record gives no row, so a
/// step of deleted records takes keys and gives no row. The taken keys leave
/// the scopes. `None` when no key can be taken.
fn next_keys(sources: &mut [ScopeKeys], step: usize) -> Option<Vec<SpanKey>> {
    let mut bound: Option<&str> = None;
    for examined in sources.iter().filter_map(ScopeKeys::examined) {
        let examined = examined?;
        bound = Some(bound.map_or(examined, |bound| bound.min(examined)));
    }
    let mut ranges: Vec<&str> = sources
        .iter()
        .flat_map(|source| source.keys.iter().map(|key| key.range.as_str()))
        .filter(|range| bound.is_none_or(|bound| *range <= bound))
        .collect();
    ranges.sort_unstable();
    ranges.dedup();
    let last = ranges
        .into_iter()
        .take(step)
        .next_back()
        .map(str::to_owned)?;
    let mut merged: BTreeMap<String, Option<SpanKey>> = BTreeMap::new();
    for source in sources.iter_mut() {
        while source.keys.front().is_some_and(|key| key.range <= last) {
            let Some(key) = source.keys.pop_front() else {
                break;
            };
            let slot = merged.entry(key.range.clone()).or_default();
            if slot.is_none() && key.record.is_live() {
                *slot = Some(key);
            }
        }
    }
    Some(merged.into_values().flatten().collect())
}

/// The rows that one step keeps, read once more by key through the record
/// warm set: memory answers first, and a record that the read finds on disk
/// enters memory. At each key the read gives the current record; a record
/// that is no longer live or holds no selected field gives no row.
async fn read_kept(
    atoms: &AtomStore,
    kept: Vec<SpanKey>,
    selected: &[String],
) -> Result<Vec<KeyedRecord>, SchemaError> {
    if kept.is_empty() {
        return Ok(Vec::new());
    }
    let (keys, physical): (Vec<KeyValue>, Vec<String>) = kept
        .into_iter()
        .map(|kept| (kept.key, kept.physical))
        .unzip();
    let records = atoms.read_records(&physical).await?;
    Ok(keys
        .into_iter()
        .zip(records)
        .filter_map(|(key, record)| {
            record
                .filter(|record| record.is_live() && has_any(record, selected))
                .map(|record| (key, record))
        })
        .collect())
}

/// The API key of the least key that a scope with unread keys examined last.
/// `None` when such a scope examined no key.
fn stop_key(atoms: &AtomStore, sources: &[ScopeKeys]) -> Result<Option<KeyValue>, SchemaError> {
    let mut least: Option<(&str, &RecordSpan, &str)> = None;
    for source in sources {
        let Some(span) = source.span.as_ref().filter(|_| !source.done) else {
            continue;
        };
        let Some(cursor) = source.cursor.as_deref() else {
            return Ok(None);
        };
        let range = span.storage_range(cursor);
        if least.is_none_or(|(least, _, _)| range < least) {
            least = Some((range, span, cursor));
        }
    }
    least
        .map(|(_, span, cursor)| atoms.span_key(span, cursor))
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
