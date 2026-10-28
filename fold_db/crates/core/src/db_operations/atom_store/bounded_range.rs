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
//! A read names where the next read goes on (`KvStore::scan_range_yielding`).
//! A full page names its last row. A read that ran short of key budget names the
//! last key it examined, with the live rows it found (it can find none) and no
//! error, so a range of any number of deleted keys, with live rows between them,
//! is walked one read per budget, with no failed read and no cut. The read keeps
//! the keys that the caller needs to read the counters and bodies of the rows it
//! returns.
//!
//! A read can still fail with the read limit: the byte or page budget, or a
//! live row that needs more than a call. The walk then asks for fewer rows, and
//! then cuts the key range in two and reads the lower half first.
//!
//! `RangeWalk` does no I/O. [`AtomStore::walk_step`] drives it for a caller
//! that reads outside a captured read scope. A caller that reads inside a scope
//! (the verifier) drives the same state machine itself, because a read-limit
//! error ends the scope and the retry needs a new one.

mod keys;
mod yielding;

use super::AtomStore;
use crate::schema::SchemaError;
use keys::midpoint;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// The least page the walk asks for after a read hit the limit.
const MIN_ROWS: usize = 16;
/// Counts of the calls one walk made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkStats {
    /// Range read calls, including calls that hit the read limit.
    pub calls: u64,
    /// Calls that hit the read limit and were retried with fewer rows.
    pub shrinks: u64,
    /// Times a key range was cut in two because fewer rows did not help.
    pub splits: u64,
    /// Reads that found no row and went on after the last key they examined: a
    /// run of deleted keys that used the key budget of the call.
    #[serde(default)]
    pub skips: u64,
}

impl WalkStats {
    /// Add the counts of another walk.
    pub fn absorb(&mut self, other: Self) {
        self.calls += other.calls;
        self.shrinks += other.shrinks;
        self.splits += other.splits;
        self.skips += other.skips;
    }
}

/// Totals of the bounded range reads since the process started.
#[derive(Debug, Default)]
pub(crate) struct WalkTotals {
    calls: AtomicU64,
    shrinks: AtomicU64,
    splits: AtomicU64,
    skips: AtomicU64,
}

impl WalkTotals {
    pub(crate) fn add(&self, stats: WalkStats) {
        self.calls.fetch_add(stats.calls, Ordering::Relaxed);
        self.shrinks.fetch_add(stats.shrinks, Ordering::Relaxed);
        self.splits.fetch_add(stats.splits, Ordering::Relaxed);
        self.skips.fetch_add(stats.skips, Ordering::Relaxed);
    }

    /// The totals now.
    pub(crate) fn snapshot(&self) -> WalkStats {
        WalkStats {
            calls: self.calls.load(Ordering::Relaxed),
            shrinks: self.shrinks.load(Ordering::Relaxed),
            splits: self.splits.load(Ordering::Relaxed),
            skips: self.skips.load(Ordering::Relaxed),
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
            skips: self.skips.saturating_sub(earlier.skips),
        }
    }
}

/// One bounded piece of a walk.
#[derive(Debug, Default)]
pub(crate) struct Step {
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
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

    /// Cut the lowest span in two and read the lower half first. Returns false
    /// when no key lies between the ends of the span.
    fn cut_top(&mut self) -> bool {
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

    /// The read hit the read limit. Ask for fewer rows, or cut the span in two.
    /// Returns false when the span cannot be cut: the read cannot be bounded.
    pub(crate) fn read_too_big(&mut self) -> bool {
        self.stats.calls += 1;
        if self.asked > MIN_ROWS {
            self.stats.shrinks += 1;
            self.asked = MIN_ROWS;
            return true;
        }
        self.cut_top()
    }
}

impl AtomStore {
    /// Read the next bounded piece of a walk, outside any captured read scope.
    ///
    /// The piece can hold no rows while the walk is not done: a span that held
    /// only deleted keys. A caller loops until [`RangeWalk::done`].
    pub(crate) async fn walk_step(&self, walk: &mut RangeWalk) -> Result<Step, SchemaError> {
        loop {
            let Some((start, end, asked)) = walk.request() else {
                return Ok(Step::default());
            };
            let result = self
                .raw()
                .inner()
                .scan_range_yielding(&start, &end, asked, 0)
                .await;
            match result.map_err(SchemaError::from) {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    self.range_walks.add(walk.take_stats());
                    return Ok(Step { rows: scan.rows });
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
