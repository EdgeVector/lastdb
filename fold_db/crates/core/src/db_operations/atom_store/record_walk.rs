//! An unfiltered read of a format 7 home: a walk of the records of one
//! schema in record key order, one bounded page at a time.
//!
//! A record key is `rk:{S}:{esc(blind(hash))}\0{range}`, so the record space
//! is ordered by the blinded hash, then by the typed range under one hash.
//! That is not the API page order (`cmp_page_order`: range, then hash). The
//! blind is an HMAC, so the first records in page order can sit anywhere in
//! the record space, and only a read of every record finds them. One request
//! may examine at most 10,000 keys, so such a read cannot serve a large
//! schema, and a drain that repeats it for each page costs the square of the
//! schema size.
//!
//! An unfiltered page therefore follows record key order. An offset page
//! reads its offset and its rows; a cursor page starts after a record key
//! position: the record key suffix of the last row of the previous page, or of
//! the last key it examined. A page costs its rows and the deleted records
//! among them, not the schema.
//!
//! A deleted record is a key too. One page reads at most `PAGE_READ_KEYS`
//! keys, so a run of deleted records cannot use up the read budget of the
//! request. A walk that reaches that allowance before its window is full
//! stops and names the last key it examined (`WalkPage::stopped`): a deleted
//! record has no API key, so the next page starts after that record key.
//!
//! The walk merges the own scope with the received scopes by record key, as a
//! range under one hash does (`records/received_range.rs`): at each key the
//! first scope with a live record gives the row. Every scope uses the codec of
//! one record space, so one API key has one record key suffix in every scope.

use super::record_reads::{decode, space_prefix, walk_key};
use super::{AtomStore, KeyedRecord};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use lastdb_model::record_value::StoredRecord;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// The most rows (the offset and the page with its probe row) that one
/// unfiltered walk takes. One request may examine 10,000 keys; the rest
/// covers deleted records and the conflict reads of the page.
pub const RECORD_WALK_ROWS: usize = 8_192;

/// The most record keys, live and deleted, that one page reads in one
/// request. One request may examine 10,000 keys; the rest covers the reads
/// by key around the page, such as the conflict markers of its rows.
pub(crate) const PAGE_READ_KEYS: usize = RECORD_WALK_ROWS;

/// The smallest and the largest number of keys one merge step reads from
/// each scope.
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

/// One window of an unfiltered walk: the live rows after the record key
/// suffix `after` (or from the first record), past `skip` rows, at most
/// `take` rows.
#[derive(Clone, Copy)]
pub(crate) struct WalkWindow<'a> {
    pub(crate) after: Option<&'a str>,
    pub(crate) skip: usize,
    pub(crate) take: usize,
}

/// One live row of a walk and its record key suffix.
pub(crate) struct WalkRow {
    pub(crate) key: KeyValue,
    pub(crate) record: Arc<StoredRecord>,
    pub(crate) at: String,
}

/// One page of a walk, in record key order.
#[derive(Default)]
pub(crate) struct WalkPage {
    pub(crate) rows: Vec<WalkRow>,
    /// The walk read its allowance before the window was full while records
    /// remained: the record key suffix of the last key it examined.
    pub(crate) stopped: Option<String>,
}

impl WalkPage {
    /// The rows with their API keys.
    pub(crate) fn into_keyed(self) -> Vec<KeyedRecord> {
        self.rows
            .into_iter()
            .map(|row| (row.key, row.record))
            .collect()
    }

    /// Where the next page starts: the last key examined when the walk
    /// stopped, or the last row of a page of `take` rows. `None` at the end.
    pub(crate) fn next(&self, take: usize) -> Option<String> {
        self.stopped.clone().or_else(|| {
            self.rows
                .last()
                .filter(|_| self.rows.len() >= take)
                .map(|row| row.at.clone())
        })
    }
}

/// The unread records of one scope, in record key order.
struct ScopeWalk {
    /// `{scope}:rk:{S}:`, the part of each key before its record key suffix.
    base: String,
    /// The next key to read (inclusive).
    start: String,
    end: String,
    rows: VecDeque<(String, StoredRecord)>,
    /// The suffix of the last key this scope examined.
    examined: Option<String>,
    done: bool,
}

impl ScopeWalk {
    fn new(space: &str, scope: Option<&str>, after: Option<&str>) -> Result<Self, SchemaError> {
        let base = build_storage_key(scope, space);
        let end = FilterUtils::create_prefix_end(&base)?;
        // The smallest key after the cursor.
        let start = after.map_or_else(|| base.clone(), |suffix| format!("{base}{suffix}\0"));
        Ok(Self {
            base,
            start,
            end,
            rows: VecDeque::new(),
            examined: None,
            done: false,
        })
    }

    /// Hold at least `wanted` rows, or every row that remains, with a read
    /// of at most `room` keys. Deleted records are rows too; the merge drops
    /// them. Returns the number of rows read.
    async fn fill(
        &mut self,
        atoms: &AtomStore,
        wanted: usize,
        room: usize,
    ) -> Result<usize, SchemaError> {
        let ask = wanted.saturating_sub(self.rows.len()).min(room);
        if self.done || ask == 0 {
            return Ok(0);
        }
        // The walk reads nothing more for a row of the page.
        let page = atoms
            .main_store
            .inner()
            .scan_range_yielding(self.start.as_bytes(), self.end.as_bytes(), ask, 1)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("walk records: {error}")))?;
        let read = page.rows.len();
        for (key, value) in page.rows {
            let suffix = self.suffix(key)?;
            self.rows.push_back((suffix, decode(&value)?));
        }
        match page.resume {
            Some(resume) => {
                let suffix = self.suffix(resume)?;
                self.start = format!("{}{suffix}\0", self.base);
                self.examined = Some(suffix);
            }
            None => self.done = true,
        }
        Ok(read)
    }

    fn suffix(&self, key: Vec<u8>) -> Result<String, SchemaError> {
        let key = String::from_utf8(key)
            .map_err(|_| SchemaError::InvalidData("a record key is not text".into()))?;
        key.strip_prefix(self.base.as_str())
            .map(str::to_owned)
            .ok_or_else(|| SchemaError::InvalidData("a record key is outside its space".into()))
    }

    /// The suffix of the last key this scope examined while it has unread
    /// keys. `Some(None)` when it examined no key yet, `None` when done.
    fn bound(&self) -> Option<Option<&str>> {
        (!self.done).then_some(self.examined.as_deref())
    }
}

impl AtomStore {
    /// One window of the live records of a schema over `scopes` (the own
    /// scope first), in record key order. `keep` drops a row before the
    /// window counts it. A walk that stops at its read allowance returns a
    /// short page and the last key it examined; it cannot stop inside the
    /// offset of an offset page.
    pub(crate) async fn walk_record_window(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        window: WalkWindow<'_>,
        scopes: &[Option<String>],
        keep: impl Fn(&StoredRecord) -> bool,
    ) -> Result<WalkPage, SchemaError> {
        let WalkWindow {
            after,
            mut skip,
            take,
        } = window;
        if take == 0 {
            return Ok(WalkPage::default());
        }
        if skip.saturating_add(take) > RECORD_WALK_ROWS {
            return Err(SchemaError::InvalidData(format!(
                "an unfiltered page reads at most {RECORD_WALK_ROWS} records from its start; \
                 continue with the query cursor (next_cursor), not a deeper offset"
            )));
        }
        let space = space_prefix(schema);
        let mut walks = scopes
            .iter()
            .map(|scope| ScopeWalk::new(&space, scope.as_deref(), after))
            .collect::<Result<Vec<_>, _>>()?;
        let mut page = WalkPage::default();
        let mut room = PAGE_READ_KEYS;
        // A step that finds no row (a run of deleted records) doubles the next.
        let mut wide = 1usize;
        while page.rows.len() < take {
            let step = skip
                .saturating_add(take - page.rows.len())
                .saturating_mul(wide)
                .clamp(MIN_STEP, MAX_STEP);
            let share = room / walks.iter().filter(|walk| !walk.done).count().max(1);
            let read = futures::future::try_join_all(
                walks.iter_mut().map(|walk| walk.fill(self, step, share)),
            )
            .await?;
            room = room.saturating_sub(read.into_iter().sum());
            let Some(records) = next_rows(&mut walks, step) else {
                if walks.iter().all(|walk| walk.done) {
                    break;
                }
                page.stopped = least_examined(&walks).filter(|stop| Some(stop.as_str()) > after);
                if skip > 0 || page.stopped.is_none() {
                    return Err(walk_limit_error());
                }
                break;
            };
            let mark = (page.rows.len(), skip);
            for (at, record) in records {
                if !keep(&record) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                let key = walk_key(schema, comparison, &record, &at)?;
                page.rows.push(WalkRow {
                    key,
                    record: Arc::new(record),
                    at,
                });
                if page.rows.len() == take {
                    break;
                }
            }
            if (page.rows.len(), skip) == mark {
                wide = wide.saturating_mul(2);
            }
        }
        Ok(page)
    }

    /// The record key suffix (`{esc(blind(hash))}\0{range}`) of an API key:
    /// the walk position of a cursor that names a row.
    pub(crate) fn walk_position(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        let record = self.record_key(schema, key, None).map_err(|error| {
            SchemaError::InvalidCursor(format!(
                "the query cursor is not a key of this schema: {error}"
            ))
        })?;
        record
            .strip_prefix(&space_prefix(schema))
            .map(str::to_owned)
            .ok_or_else(|| {
                SchemaError::InvalidCursor("the query cursor is not in this schema".into())
            })
    }
}

/// The live winners of the next record keys of the merged order: at most
/// `step` keys, each at or below the last key that every scope with unread
/// keys examined, so no unread key comes before a key taken. At each key the
/// first scope with a live record wins; a key with no live record gives no
/// row, so a step of deleted records takes keys and gives no row. The taken
/// rows leave the scopes. `None` when no key can be taken.
fn next_rows(walks: &mut [ScopeWalk], step: usize) -> Option<Vec<(String, StoredRecord)>> {
    let mut bound: Option<&str> = None;
    for examined in walks.iter().filter_map(ScopeWalk::bound) {
        let examined = examined?;
        bound = Some(bound.map_or(examined, |bound| bound.min(examined)));
    }
    let mut keys: Vec<&str> = walks
        .iter()
        .flat_map(|walk| walk.rows.iter().map(|(suffix, _)| suffix.as_str()))
        .filter(|suffix| bound.is_none_or(|bound| *suffix <= bound))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let last = keys.into_iter().take(step).next_back().map(str::to_owned)?;
    let mut merged: BTreeMap<String, Option<StoredRecord>> = BTreeMap::new();
    for walk in &mut *walks {
        while walk
            .rows
            .front()
            .is_some_and(|(suffix, _)| suffix.as_str() <= last.as_str())
        {
            let Some((suffix, record)) = walk.rows.pop_front() else {
                break;
            };
            let slot = merged.entry(suffix).or_insert(None);
            if slot.is_none() && record.is_live() {
                *slot = Some(record);
            }
        }
    }
    Some(
        merged
            .into_iter()
            .filter_map(|(suffix, record)| record.map(|record| (suffix, record)))
            .collect(),
    )
}

/// The least last-examined suffix of the scopes with unread keys.
fn least_examined(walks: &[ScopeWalk]) -> Option<String> {
    walks
        .iter()
        .filter_map(ScopeWalk::bound)
        .map(|examined| examined.map(str::to_owned))
        .min()
        .flatten()
}

fn walk_limit_error() -> SchemaError {
    SchemaError::ReadResourceLimit {
        detail: format!(
            "the walk passed {PAGE_READ_KEYS} deleted or skipped records before its page was \
             full; continue an unfiltered read with the query cursor (next_cursor), not a \
             deeper offset"
        ),
    }
}
