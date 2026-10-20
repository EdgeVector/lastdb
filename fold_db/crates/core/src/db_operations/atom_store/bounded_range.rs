//! A range walk that always makes progress under the read budget.
//!
//! One native read call may examine at most 10,000 keys, and a range read
//! counts every key it passes, deleted keys included. A page of live rows is
//! therefore not enough to bound a call: after many purges a range can hold
//! more than 10,000 deleted keys between two live rows, and a page that has to
//! pass them all fails with `read resource limit exceeded` however small the
//! page is.
//!
//! [`RangeWalk`] bounds each call by key space instead. It asks for a page. If
//! the call hits the read limit it asks for fewer rows, and if that is still
//! too much it cuts the key range in two and reads the lower half first. The
//! pieces are kept on a stack, so the upper half is not retried at full width.
//! Every call is its own read call with its own budget. The walk is a loop of
//! bounded calls with a cursor, and the cursor is the least key not read yet.
//!
//! A scan can also return fewer rows than it read. The encrypting store drops a
//! row that has no at-rest envelope, after the row limit applied, so a short
//! page does not mean the end of the range. The walk counts the dropped rows
//! (`count_unsealed_discards`) and adds them to the rows it got: a page is full
//! when the sum reaches the rows asked for. A page of dropped rows only cannot
//! name the next key, so the walk asks for more rows from the same start.
//!
//! `RangeWalk` does no I/O. [`AtomStore::walk_step`] drives it for a caller
//! that reads outside a captured read scope. A caller that reads inside a scope
//! (the verifier) drives the same state machine itself, because a read-limit
//! error ends the scope and the retry needs a new one.

use super::AtomStore;
use crate::schema::SchemaError;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// The least page the walk asks for after a read hit the limit.
const MIN_ROWS: usize = 16;
/// The most rows one read asks for. The native scan refuses more.
const MAX_ROWS: usize = 10_000;

/// Counts of the calls one walk made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkStats {
    /// Range read calls, including calls that hit the read limit.
    pub calls: u64,
    /// Calls that hit the read limit and were retried with fewer rows.
    pub shrinks: u64,
    /// Times a key range was cut in two because fewer rows did not help.
    pub splits: u64,
    /// Reads that dropped a row with no at-rest envelope. The row reads as
    /// absent. A row next to the end of a page can count again in the next page.
    #[serde(default)]
    pub unsealed_rows: u64,
}

impl WalkStats {
    /// Add the counts of another walk.
    pub fn absorb(&mut self, other: Self) {
        self.calls += other.calls;
        self.shrinks += other.shrinks;
        self.splits += other.splits;
        self.unsealed_rows += other.unsealed_rows;
    }
}

/// Totals of the bounded range reads since the process started.
#[derive(Debug, Default)]
pub(crate) struct WalkTotals {
    calls: AtomicU64,
    shrinks: AtomicU64,
    splits: AtomicU64,
    unsealed_rows: AtomicU64,
}

impl WalkTotals {
    pub(crate) fn add(&self, stats: WalkStats) {
        self.calls.fetch_add(stats.calls, Ordering::Relaxed);
        self.shrinks.fetch_add(stats.shrinks, Ordering::Relaxed);
        self.splits.fetch_add(stats.splits, Ordering::Relaxed);
        self.unsealed_rows
            .fetch_add(stats.unsealed_rows, Ordering::Relaxed);
    }

    /// The totals now.
    pub(crate) fn snapshot(&self) -> WalkStats {
        WalkStats {
            calls: self.calls.load(Ordering::Relaxed),
            shrinks: self.shrinks.load(Ordering::Relaxed),
            splits: self.splits.load(Ordering::Relaxed),
            unsealed_rows: self.unsealed_rows.load(Ordering::Relaxed),
        }
    }
}

impl WalkStats {
    /// The counts above `earlier`.
    #[must_use]
    pub fn since(self, earlier: Self) -> Self {
        Self {
            calls: self.calls.saturating_sub(earlier.calls),
            shrinks: self.shrinks.saturating_sub(earlier.shrinks),
            splits: self.splits.saturating_sub(earlier.splits),
            unsealed_rows: self.unsealed_rows.saturating_sub(earlier.unsealed_rows),
        }
    }
}

/// What a finished read did to the walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    /// The walk moved on, or the span ended.
    Moved,
    /// The page held no row to continue from. The walk asks for more rows from
    /// the same start. The caller reads again.
    Widened,
    /// More than the most rows of one read lie in a row without an at-rest
    /// envelope. The walk cannot move on.
    Stuck,
}

/// One bounded piece of a walk.
#[derive(Debug, Default)]
pub(crate) struct Step {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Reads of this step that dropped a row with no at-rest envelope.
    pub unsealed: u64,
}

/// The error of a walk that met only rows without an at-rest envelope.
pub(crate) fn unsealed_run_error() -> SchemaError {
    SchemaError::InvalidData(format!(
        "more than {MAX_ROWS} rows in a row have no at-rest envelope and read as absent; \
         the range cannot be walked. Run `lastdb db reap-unsealed --collection <name>` and try again"
    ))
}

/// The state of a bounded walk over `start..end`.
#[derive(Debug, Clone)]
pub(crate) struct RangeWalk {
    /// Key spans not read yet. The last one is the lowest.
    todo: Vec<(Vec<u8>, Vec<u8>)>,
    limit: usize,
    asked: usize,
    pub stats: WalkStats,
}

impl RangeWalk {
    /// A walk over `start..end` (inclusive start, exclusive end) that asks for
    /// `limit` rows in each read.
    pub(crate) fn new(start: &[u8], end: &[u8], limit: usize) -> Self {
        let limit = limit.max(1);
        let todo = if start < end {
            vec![(start.to_vec(), end.to_vec())]
        } else {
            Vec::new()
        };
        Self {
            todo,
            limit,
            asked: limit,
            stats: WalkStats::default(),
        }
    }

    /// Ask for up to `rows` rows in each read from now on. A read that hit the
    /// limit keeps its smaller page until reads succeed again.
    pub(crate) fn set_limit(&mut self, rows: usize) {
        self.limit = rows.max(1);
        self.asked = self.asked.min(self.limit);
    }

    /// The counts since the last call, reset to zero.
    pub(crate) fn take_stats(&mut self) -> WalkStats {
        std::mem::take(&mut self.stats)
    }

    /// True when every key of the range was read.
    pub(crate) fn done(&self) -> bool {
        self.todo.is_empty()
    }

    /// The next read to make: start, end and the number of rows to ask for.
    pub(crate) fn request(&self) -> Option<(Vec<u8>, Vec<u8>, usize)> {
        self.todo
            .last()
            .map(|(start, end)| (start.clone(), end.clone(), self.asked))
    }

    /// A read returned `returned` rows and dropped `unsealed` rows that had no
    /// at-rest envelope. `last` is the key of the last returned row.
    ///
    /// The page was full when the rows read, returned and dropped, reach the
    /// rows asked for. A short page is the end of the span only if it is short
    /// after the dropped rows are added.
    pub(crate) fn finish_read(
        &mut self,
        returned: usize,
        unsealed: u64,
        last: Option<&[u8]>,
    ) -> Progress {
        self.stats.calls += 1;
        self.stats.unsealed_rows += unsealed;
        let read = returned as u64 + unsealed;
        if read < self.asked as u64 {
            self.todo.pop();
            return Progress::Moved;
        }
        // A full page: more rows may lie above the last one.
        let Some(last) = last else {
            // Every row of the page was dropped, so the page names no key to
            // continue from. Ask for more rows from the same start.
            if self.asked >= MAX_ROWS {
                return Progress::Stuck;
            }
            self.asked = (self.asked * 4).min(MAX_ROWS);
            return Progress::Widened;
        };
        if let Some((_, end)) = self.todo.pop() {
            let next = successor(last);
            if next < end {
                self.todo.push((next, end));
            }
        }
        self.asked = (self.asked * 2).min(self.limit);
        Progress::Moved
    }

    /// The read hit the read limit. Ask for fewer rows, or cut the span in two.
    /// Returns false when the span cannot be cut: the read cannot be bounded.
    pub(crate) fn read_too_big(&mut self) -> bool {
        self.stats.calls += 1;
        if self.asked > MIN_ROWS {
            self.stats.shrinks += 1;
            self.asked = MIN_ROWS;
            return true;
        }
        let Some((start, end)) = self.todo.pop() else {
            return false;
        };
        let Some(middle) = midpoint(&start, &end) else {
            self.todo.push((start, end));
            return false;
        };
        self.stats.splits += 1;
        self.todo.push((middle.clone(), end));
        self.todo.push((start, middle));
        true
    }
}

impl AtomStore {
    /// Read the next bounded piece of a walk, outside any captured read scope.
    ///
    /// The piece can hold no rows while the walk is not done: a span that held
    /// only deleted keys. A caller loops until [`RangeWalk::done`]. `unsealed`
    /// counts the reads that dropped a row with no at-rest envelope: such a row
    /// reads as absent everywhere, and the walk does not take the short page
    /// for the end of the range. A caller that must not act on a range with
    /// such a row (the recount) checks the count.
    pub(crate) async fn walk_step(&self, walk: &mut RangeWalk) -> Result<Step, SchemaError> {
        let mut unsealed = 0;
        loop {
            let Some((start, end, asked)) = walk.request() else {
                return Ok(Step::default());
            };
            let (result, dropped) = crate::crypto::count_unsealed_discards(
                self.raw().inner().scan_range_paged(&start, &end, asked),
            )
            .await;
            unsealed += dropped;
            match result.map_err(SchemaError::from) {
                Ok(rows) => {
                    let last = rows.last().map(|(key, _)| key.clone());
                    let progress = walk.finish_read(rows.len(), dropped, last.as_deref());
                    self.range_walks.add(walk.take_stats());
                    match progress {
                        Progress::Moved => return Ok(Step { rows, unsealed }),
                        Progress::Widened => {}
                        Progress::Stuck => return Err(unsealed_run_error()),
                    }
                }
                Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                    let cut = walk.read_too_big();
                    self.range_walks.add(walk.take_stats());
                    if !cut {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// The least key above `key`.
pub(crate) fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

/// A key strictly between `low` and `high`, near the middle of the key space
/// between them. The keys are read as numbers in base 128 (one ASCII byte per
/// digit). `None` when no ASCII key lies between them.
pub(crate) fn midpoint(low: &[u8], high: &[u8]) -> Option<Vec<u8>> {
    if low >= high || low.iter().chain(high).any(|byte| *byte >= 0x80) {
        return None;
    }
    let width = low.len().max(high.len()) + 1;
    let digits = |key: &[u8]| -> Vec<u32> {
        (0..width)
            .map(|at| key.get(at).map_or(0, |byte| u32::from(*byte)))
            .collect()
    };
    let (a, b) = (digits(low), digits(high));
    let mut sum = vec![0u32; width + 1];
    let mut carry = 0;
    for at in (0..width).rev() {
        let total = a[at] + b[at] + carry;
        sum[at + 1] = total % 128;
        carry = total / 128;
    }
    sum[0] = carry;
    let mut middle = Vec::with_capacity(width);
    let mut remainder = 0;
    for (at, digit) in sum.into_iter().enumerate() {
        let value = remainder * 128 + digit;
        remainder = value % 2;
        if at > 0 {
            middle.push(u8::try_from(value / 2).ok()?);
        }
    }
    while middle.len() > 1 && middle.last() == Some(&0) && middle[..middle.len() - 1] > *low {
        middle.pop();
    }
    (middle.as_slice() > low && middle.as_slice() < high).then_some(middle)
}
