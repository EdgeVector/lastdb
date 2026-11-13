//! Ordered ranges and bounded stream lookups over one held CURRENT.

use super::read::RunRead;
use super::{invalid, run_key, storage_key};
use crate::disk_run::{Found, MergeIter, RunRange, RunReader};
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
    owner: &'a mut RunRead,
    collection: String,
    exhausted: bool,
    mode: RangeMode,
}

enum RangeMode {
    Ordinary(MergeIter, Option<(Vec<u8>, Found)>),
    Bounded(Vec<u8>, Vec<u8>, bool),
}

impl RunRead {
    fn admit_stream(&self, extra: u64) -> Result<(u64, u64)> {
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        let logical = self
            .budget
            .bytes
            .checked_sub(self.costs.budget_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        let reserved = self
            .shared_metadata_bytes
            .max(self.retained_bytes())
            .checked_add(self.costs.budget_bytes)
            .and_then(|n| n.checked_add(extra))
            .ok_or(Error::ReadBudgetExceeded)?;
        let global = res
            .working_memory_bytes
            .checked_sub(reserved)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((logical.min(global).min(res.decoded_values_bytes), reserved))
    }

    fn open_stream(&self, slot: usize, extra: u64) -> Result<(RunReader, u64, u64)> {
        let (adm, res) = self.admit_stream(extra)?;
        Ok((
            RunReader::open_bounded(&self.runs[slot].0.path, &self.key, adm, res)?,
            adm,
            res,
        ))
    }

    pub(super) fn streamed_value(
        &mut self,
        col: &str,
        key: &str,
        max: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.account_memory_record(col.len(), key.len())?;
        let bufs = 2 * lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        let add_bufs = |n: usize| {
            (n as u64)
                .checked_add(bufs)
                .ok_or(Error::ReadBudgetExceeded)
        };
        self.admit_stream(add_bufs(col.len() + key.len() + 2)?)?;
        let wanted = run_key(col, key.as_bytes())?;
        self.admit_stream(add_bufs(wanted.capacity())?)?;
        for slot in 0..self.runs.len() {
            if self.runs[slot].0.meta.covers(&wanted) {
                let (reader, adm, res) = self.open_stream(slot, wanted.len() as u64)?;
                if let Some(hit) = reader.stream_body(&wanted, adm, res, max)? {
                    if let Some(v) = &hit {
                        self.charge(v.len() as u64)?;
                    }
                    return Ok(hit);
                }
            }
        }
        Ok(None)
    }

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
        exclusive: bool,
    ) -> Result<RunHashRange<'_>> {
        self.charge((col.len() + start.len() + end.len()) as u64)?;
        let (lower, upper) = (run_key(col, start)?, run_key(col, end)?);
        let mode = if self.bounded_streams {
            RangeMode::Bounded(lower, upper, exclusive)
        } else {
            let lo = if exclusive {
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
                let lo_vec = lo.map(<[u8]>::to_vec);
                sources.push(RunRange::sequence(
                    base,
                    &self.key,
                    lo_vec,
                    Bound::Excluded(upper),
                ));
            }
            RangeMode::Ordinary(MergeIter::new(sources), None)
        };
        Ok(RunHashRange {
            owner: self,
            collection: col.into(),
            exhausted: false,
            mode,
        })
    }
}

impl RunHashRange<'_> {
    /// Charge selected pending records to the same range budget.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        self.owner.account_memory_record(key_bytes, body_bytes)
    }

    /// Up to `limit` live records under this exact root.
    pub fn next_page(&mut self, limit: usize) -> Result<RunRangePage> {
        self.page(limit, None)
    }

    /// Yield at the same administrative key budget as the ordinary cursor.
    pub fn next_page_yielding(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
    ) -> Result<RunRangePage> {
        self.page(limit, Some((held_rows, keys_per_row)))
    }

    /// Bound each body before allocation on a captured or stopped-home read.
    pub fn next_page_yielding_bounded(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
        maximum: u64,
    ) -> Result<RunRangePage> {
        if matches!(self.mode, RangeMode::Ordinary(..)) {
            return Err(invalid(
                "bounded run pages require a captured or stopped-home read",
            ));
        }
        self.bounded_page(limit, Some((held_rows, keys_per_row)), maximum)
    }

    fn page(&mut self, limit: usize, yielding: Option<(usize, usize)>) -> Result<RunRangePage> {
        if matches!(self.mode, RangeMode::Ordinary(..)) {
            self.ordinary_page(limit, yielding)
        } else {
            self.bounded_page(limit, yielding, u64::MAX)
        }
    }

    fn should_yield(&self, yielding: Option<(usize, usize)>, n: usize, has_last: bool) -> bool {
        yielding.is_some_and(|(held, keys)| {
            let (rows, cur, lim) = (
                held + n,
                self.owner.costs.examined_keys,
                self.owner.budget.keys,
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
            let RangeMode::Ordinary(records, held) = &mut self.mode else {
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
            if self.owner.examine(1).is_err() {
                if let RangeMode::Ordinary(_, held) = &mut self.mode {
                    *held = Some((key, found));
                }
                if yielding.is_some() && last.is_some() {
                    resume = last.take();
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            if let Found::Value(v) = found {
                self.owner.charge((key.len() + v.len()) as u64)?;
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

    fn bounded_page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
        maximum: u64,
    ) -> Result<RunRangePage> {
        let (mut out, mut last, mut resume) = (Vec::new(), false, None);
        while !self.exhausted && out.len() < limit {
            if self.should_yield(yielding, out.len(), last) {
                resume = Some(self.resume_key()?);
                break;
            }
            let Some(key) = self.next_bounded_key()? else {
                self.exhausted = true;
                break;
            };
            if self.owner.costs.examined_keys >= self.owner.budget.keys {
                if yielding.is_some() && last {
                    resume = Some(self.resume_key()?);
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            let before = self.owner.shared_metadata_bytes;
            self.owner.shared_metadata_bytes = before
                .max(self.owner.retained_bytes())
                .checked_add(self.coord_bytes())
                .ok_or(Error::ReadBudgetExceeded)?;
            let val = self.owner.streamed_value(&self.collection, &key, maximum);
            self.owner.shared_metadata_bytes = before;
            let val = val?;
            if let RangeMode::Bounded(lower, _, exclusive) = &mut self.mode {
                (*lower, *exclusive) = (run_key(&self.collection, key.as_bytes())?, true);
            }
            last = true;
            if let Some(v) = val {
                out.push((key, v));
            }
        }
        Ok(RunRangePage {
            records: out,
            exhausted: self.exhausted,
            resume,
        })
    }

    fn next_bounded_key(&self) -> Result<Option<String>> {
        let RangeMode::Bounded(lower, upper, exclusive) = &self.mode else {
            unreachable!()
        };
        let mut sel: Option<Vec<u8>> = None;
        for slot in 0..self.owner.runs.len() {
            let end = sel.as_deref().unwrap_or(upper);
            if !self.owner.runs[slot].0.meta.overlaps(lower, Some(end)) {
                continue;
            }
            let extra = lower
                .len()
                .checked_add(end.len())
                .and_then(|n| n.checked_add(sel.as_ref().map_or(0, Vec::capacity)))
                .ok_or(Error::ReadBudgetExceeded)? as u64;
            let (r, adm, res) = self.owner.open_stream(slot, extra)?;
            if let Some(next) = r.stream_key(lower, Some(end), *exclusive, adm, res)? {
                sel = Some(next);
            }
        }
        sel.map(|k| storage_key(&self.collection, &k)).transpose()
    }

    fn coord_bytes(&self) -> u64 {
        if let RangeMode::Bounded(l, u, _) = &self.mode {
            (self.collection.capacity() + l.capacity() + u.capacity()) as u64
        } else {
            0
        }
    }

    fn resume_key(&self) -> Result<String> {
        let RangeMode::Bounded(lower, ..) = &self.mode else {
            unreachable!()
        };
        let retained = self
            .owner
            .shared_metadata_bytes
            .max(self.owner.retained_bytes())
            .saturating_add(
                self.owner.costs.budget_bytes + self.coord_bytes() + lower.len() as u64,
            );
        (retained <= lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes)
            .then(|| storage_key(&self.collection, lower))
            .ok_or(Error::ReadBudgetExceeded)?
    }
}
