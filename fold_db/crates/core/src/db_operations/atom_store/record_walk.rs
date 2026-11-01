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
//! reads its offset and its rows; a cursor page starts after the record key
//! of its cursor, and the cursor stays an API key. A page costs its rows and
//! the deleted records among them, not the schema.
//!
//! The walk merges the own scope with the received scopes by record key, as a
//! range under one hash does (`records/received_range.rs`): at each key the
//! first scope with a live record gives the row. Every scope uses the codec of
//! one record space, so one API key has one record key suffix in every scope.

use super::record_reads::{decode, recovered_key, space_prefix};
use super::{AtomStore, KeyedRecord};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use lastdb_model::record_value::StoredRecord;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

/// The most rows (the offset and the page with its probe row) that one
/// unfiltered walk takes. One request may examine 10,000 keys; the rest
/// covers deleted records and the conflict reads of the page.
pub const RECORD_WALK_ROWS: usize = 8_192;

/// The smallest and the largest number of keys one merge step reads from
/// each scope.
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

/// One window of an unfiltered walk: the live rows after the record key of
/// `after` (or from the first record), past `skip` rows, at most `take` rows.
#[derive(Clone, Copy)]
pub(crate) struct WalkWindow<'a> {
    pub(crate) after: Option<&'a KeyValue>,
    pub(crate) skip: usize,
    pub(crate) take: usize,
}

/// The unread records of one scope, in record key order.
struct ScopeWalk {
    /// `{scope}:rk:{S}:`, the part of each key before its record key suffix.
    base: String,
    /// The next key to read (inclusive).
    start: String,
    end: String,
    rows: VecDeque<(String, StoredRecord)>,
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
            done: false,
        })
    }

    /// Hold at least `wanted` rows, or every row that remains. Deleted
    /// records are rows too; the merge drops them.
    async fn fill(&mut self, atoms: &AtomStore, wanted: usize) -> Result<(), SchemaError> {
        if self.done || self.rows.len() >= wanted {
            return Ok(());
        }
        let want = wanted - self.rows.len();
        let rows = atoms
            .main_store
            .inner()
            .scan_range_paged(self.start.as_bytes(), self.end.as_bytes(), want)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("walk records: {error}")))?;
        self.done = rows.len() < want;
        for (key, value) in rows {
            let key = String::from_utf8(key)
                .map_err(|_| SchemaError::InvalidData("a record key is not text".into()))?;
            let suffix = key
                .strip_prefix(self.base.as_str())
                .ok_or_else(|| {
                    SchemaError::InvalidData("a record key is outside its space".into())
                })?
                .to_owned();
            self.start = format!("{key}\0");
            self.rows.push_back((suffix, decode(&value)?));
        }
        Ok(())
    }
}

impl AtomStore {
    /// One window of the live records of a schema over `scopes` (the own
    /// scope first), in record key order. `keep` drops a row before the
    /// window counts it.
    pub(crate) async fn walk_record_window(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        window: WalkWindow<'_>,
        scopes: &[Option<String>],
        keep: impl Fn(&StoredRecord) -> bool,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let WalkWindow {
            after,
            mut skip,
            take,
        } = window;
        if take == 0 {
            return Ok(Vec::new());
        }
        if skip.saturating_add(take) > RECORD_WALK_ROWS {
            return Err(SchemaError::InvalidData(format!(
                "an unfiltered page reads at most {RECORD_WALK_ROWS} records from its start; \
                 continue with the query cursor (next_cursor), not a deeper offset"
            )));
        }
        let space = space_prefix(schema);
        let after = after
            .map(|key| self.cursor_suffix(schema, &space, key))
            .transpose()?;
        let mut walks = scopes
            .iter()
            .map(|scope| ScopeWalk::new(&space, scope.as_deref(), after.as_deref()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut rows = Vec::with_capacity(take.min(MAX_STEP));
        while rows.len() < take {
            let step = skip
                .saturating_add(take - rows.len())
                .clamp(MIN_STEP, MAX_STEP);
            futures::future::try_join_all(walks.iter_mut().map(|walk| walk.fill(self, step)))
                .await?;
            let Some(records) = next_rows(&mut walks, step) else {
                break;
            };
            for record in records {
                if !keep(&record) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                rows.push((
                    recovered_key(schema, comparison, &record)?,
                    Arc::new(record),
                ));
                if rows.len() == take {
                    break;
                }
            }
        }
        Ok(rows)
    }

    /// The record key suffix (`{esc(blind(hash))}\0{range}`) of a cursor.
    fn cursor_suffix(
        &self,
        schema: &Schema,
        space: &str,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        let record = self.record_key(schema, key, None).map_err(|error| {
            SchemaError::InvalidCursor(format!(
                "the query cursor is not a key of this schema: {error}"
            ))
        })?;
        record
            .strip_prefix(space)
            .map(str::to_owned)
            .ok_or_else(|| {
                SchemaError::InvalidCursor("the query cursor is not in this schema".into())
            })
    }
}

/// The live winners of the first `step` record keys of the merged order, in
/// record key order. Each scope that is not done holds at least `step` rows,
/// so no unread key comes before the last key taken. At each key the first
/// scope with a live record wins; a key with no live record gives no row. The
/// taken rows leave the scopes. `None` when no scope holds a row.
fn next_rows(walks: &mut [ScopeWalk], step: usize) -> Option<Vec<StoredRecord>> {
    let keys: BTreeSet<&str> = walks
        .iter()
        .flat_map(|walk| walk.rows.iter().map(|(suffix, _)| suffix.as_str()))
        .collect();
    let last = keys.into_iter().take(step).last()?.to_owned();
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
    Some(merged.into_values().flatten().collect())
}
