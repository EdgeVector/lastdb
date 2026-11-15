//! Ordered ranges and merged-page iteration over one held CURRENT.

use super::read::RunRead;
use super::{run_key, storage_key};
use crate::disk_run::{Found, MergeIter, RunRange};
use crate::{Error, Result};
use std::ops::Bound;
use std::path::PathBuf;

/// One page of live records. Tombstones count as examined keys but do not appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRangePage {
    /// Complete storage keys and their values, in key order.
    pub records: Vec<(String, Vec<u8>)>,
    /// True when the range has no more records.
    pub exhausted: bool,
    /// Set only by [`RunHashRange::next_page_yielding`]: resume above this storage key.
    pub resume: Option<String>,
}

/// An in-process cursor over one range of the held CURRENT.
pub struct RunHashRange<'a> {
    read: &'a mut RunRead,
    collection: String,
    exhausted: bool,
    records: MergeIter,
    held: Option<(Vec<u8>, Found)>,
}

impl RunRead {
    /// Internal metadata range over one collection, `[start, end)` in storage key bytes.
    pub fn administrative_range(
        &mut self,
        col: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.charge((col.len() + start.len() + end.len()) as u64)?;
        let (lower, upper) = (run_key(col, start)?, run_key(col, end)?);
        let lo = Bound::Included(&lower[..]);
        let base_from = self.runs.len() - self.manifest.base.len();
        let mut sources = Vec::new();
        for slot in 0..base_from {
            if self.runs[slot].0.meta.overlaps(&lower, Some(&upper)) {
                sources.push(self.reader(slot)?.range(lo, Bound::Excluded(&upper)));
            }
        }
        let base: Vec<PathBuf> = self.runs[base_from..]
            .iter()
            .filter(|(f, _)| f.meta.overlaps(&lower, Some(&upper)))
            .map(|(f, _)| f.path.clone())
            .collect();
        if !base.is_empty() {
            sources.push(RunRange::sequence(
                base,
                &self.key,
                Bound::Included(lower),
                Bound::Excluded(upper),
            ));
        }
        Ok(RunHashRange {
            read: self,
            collection: col.into(),
            exhausted: false,
            records: MergeIter::new(sources),
            held: None,
        })
    }
}

impl RunHashRange<'_> {
    /// Charge selected pending records to the same range budget.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        self.read.account_memory_record(key_bytes, body_bytes)
    }

    /// Up to `limit` live records under this exact root.
    pub fn next_page(&mut self, limit: usize) -> Result<RunRangePage> {
        self.page(limit, None)
    }

    /// Yield at the same administrative key budget as the ordinary cursor.
    pub fn next_page_yielding(
        &mut self,
        limit: usize,
        held: usize,
        keys_per_row: usize,
    ) -> Result<RunRangePage> {
        self.page(limit, Some((held, keys_per_row)))
    }

    fn should_yield(&self, yielding: Option<(usize, usize)>, n: usize, has_last: bool) -> bool {
        yielding.is_some_and(|(held, keys)| {
            let (rows, cur, lim) = (
                held + n,
                self.read.costs.examined_keys,
                self.read.budget.keys,
            );
            rows > 0
                && keys > 0
                && has_last
                && cur.saturating_add((keys.saturating_mul(rows) + 16) as u64) > lim
        })
    }

    fn page(&mut self, limit: usize, yielding: Option<(usize, usize)>) -> Result<RunRangePage> {
        let (mut resume, mut out, mut last) = (None, Vec::new(), None);
        while !self.exhausted && out.len() < limit {
            if self.should_yield(yielding, out.len(), last.is_some()) {
                resume = last.take();
                break;
            }
            let (key, found) = if let Some(item) = self.held.take() {
                item
            } else if let Some(next) = self.records.next() {
                next?
            } else {
                self.exhausted = true;
                break;
            };
            if self.read.examine(1).is_err() {
                self.held = Some((key, found));
                if yielding.is_some() && last.is_some() {
                    resume = last.take();
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            if let Found::Value(v) = found {
                self.read.charge((key.len() + v.len()) as u64)?;
                out.push((storage_key(&self.collection, &key)?, v));
            }
            last = yielding.map(|_| key);
        }
        let resume = resume
            .map(|k| storage_key(&self.collection, &k))
            .transpose()?;
        Ok(RunRangePage {
            records: out,
            exhausted: self.exhausted,
            resume,
        })
    }
}
