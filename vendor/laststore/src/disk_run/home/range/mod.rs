//! Ordered ranges and ordinary merged-page iteration over one held CURRENT.

mod bounded;

use super::read::RunRead;
use super::{invalid, run_key, storage_key};
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

enum RangeMode {
    Ordinary {
        records: MergeIter,
        held: Option<(Vec<u8>, Found)>,
    },
    Bounded {
        lower: Vec<u8>,
        upper: Vec<u8>,
        exclusive: bool,
    },
}

/// An in-process cursor over one range of the held CURRENT.
pub struct RunHashRange<'a> {
    read: &'a mut RunRead,
    collection: String,
    exhausted: bool,
    mode: RangeMode,
}

impl RunRead {
    /// Internal metadata range over one collection, `[start, end)` in storage key bytes.
    pub fn administrative_range(
        &mut self,
        col: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.range_between(col, start, end, false)
    }

    /// Continue an administrative range strictly above one complete storage key.
    pub fn administrative_range_after(
        &mut self,
        col: &str,
        after: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.range_between(col, after, end, true)
    }

    fn range_between(
        &mut self,
        col: &str,
        start: &[u8],
        end: &[u8],
        excl: bool,
    ) -> Result<RunHashRange<'_>> {
        self.charge((col.len() + start.len() + end.len()) as u64)?;
        let (lower, upper) = (run_key(col, start)?, run_key(col, end)?);
        let mode = if self.streamed {
            RangeMode::Bounded {
                lower,
                upper,
                exclusive: excl,
            }
        } else {
            let lo = if excl {
                Bound::Excluded(&lower[..])
            } else {
                Bound::Included(&lower[..])
            };
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
                    lo.map(<[u8]>::to_vec),
                    Bound::Excluded(upper),
                ));
            }
            RangeMode::Ordinary {
                records: MergeIter::new(sources),
                held: None,
            }
        };
        Ok(RunHashRange {
            read: self,
            collection: col.into(),
            exhausted: false,
            mode,
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

    /// Bound each body before allocation on a captured or stopped-home read.
    pub fn next_page_yielding_bounded(
        &mut self,
        limit: usize,
        held: usize,
        keys_per_row: usize,
        max: u64,
    ) -> Result<RunRangePage> {
        if matches!(self.mode, RangeMode::Ordinary { .. }) {
            return Err(invalid(
                "bounded run pages require a captured or stopped-home read",
            ));
        }
        self.bounded_page(limit, Some((held, keys_per_row)), max)
    }

    fn page(&mut self, limit: usize, yielding: Option<(usize, usize)>) -> Result<RunRangePage> {
        if matches!(self.mode, RangeMode::Ordinary { .. }) {
            self.ordinary_page(limit, yielding)
        } else {
            self.bounded_page(limit, yielding, u64::MAX)
        }
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

    fn ordinary_page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
    ) -> Result<RunRangePage> {
        let (mut resume, mut out, mut last) = (None, Vec::new(), None);
        while !self.exhausted && out.len() < limit {
            if self.should_yield(yielding, out.len(), last.is_some()) {
                resume = last.take();
                break;
            }
            let RangeMode::Ordinary { records, held } = &mut self.mode else {
                unreachable!()
            };
            let (key, found) = if let Some(item) = held.take() {
                item
            } else if let Some(next) = records.next() {
                next?
            } else {
                self.exhausted = true;
                break;
            };
            if self.read.examine(1).is_err() {
                if let RangeMode::Ordinary { held, .. } = &mut self.mode {
                    *held = Some((key, found));
                }
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
