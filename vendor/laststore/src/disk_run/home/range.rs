//! Ordered ranges over one collection, merged across the held runs (ordinary and memory-bounded).

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

/// An in-process cursor over one range of the held CURRENT.
pub struct RunHashRange<'a> {
    owner: &'a mut RunRead,
    collection: String,
    exhausted: bool,
    mode: RangeMode,
}

enum RangeMode {
    Ordinary {
        records: MergeIter,
        held: Option<(Vec<u8>, Found)>,
    },
    Bounded {
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
        exclusive: bool,
    },
}

impl RunRead {
    /// Internal metadata range over one collection, `[start, end)` in storage key bytes.
    pub fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.range_between(collection, start, end, false)
    }

    /// Continue an administrative range strictly above one complete storage key.
    pub fn administrative_range_after(
        &mut self,
        collection: &str,
        after: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.range_between(collection, after, end, true)
    }

    fn range_between(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
        exclusive: bool,
    ) -> Result<RunHashRange<'_>> {
        self.charge((collection.len() + start.len() + end.len()) as u64)?;
        let (lower, upper) = (run_key(collection, start)?, Some(run_key(collection, end)?));
        let mode = if self.bounded_streams {
            RangeMode::Bounded {
                lower,
                upper,
                exclusive,
            }
        } else {
            let lo = if exclusive {
                Bound::Excluded(lower.as_slice())
            } else {
                Bound::Included(lower.as_slice())
            };
            let (base_from, up_ref) = (
                self.runs.len() - self.manifest.base.len(),
                upper.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
            );
            let mut sources = Vec::new();
            for slot in 0..base_from {
                if self.runs[slot].0.meta.overlaps(&lower, upper.as_deref()) {
                    sources.push(self.reader(slot)?.range(lo, up_ref));
                }
            }
            let base: Vec<PathBuf> = self.runs[base_from..]
                .iter()
                .filter(|(f, _)| f.meta.overlaps(&lower, upper.as_deref()))
                .map(|(f, _)| f.path.clone())
                .collect();
            if !base.is_empty() {
                let (lo_vec, up_vec) = (
                    lo.map(<[u8]>::to_vec),
                    upper.map_or(Bound::Unbounded, Bound::Excluded),
                );
                sources.push(RunRange::sequence(base, &self.key, lo_vec, up_vec));
            }
            RangeMode::Ordinary {
                records: MergeIter::new(sources),
                held: None,
            }
        };
        Ok(RunHashRange {
            owner: self,
            collection: collection.into(),
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
        if matches!(self.mode, RangeMode::Ordinary { .. }) {
            return Err(invalid(
                "bounded run pages require a captured or stopped-home read",
            ));
        }
        self.bounded_page(limit, Some((held_rows, keys_per_row)), maximum)
    }

    fn page(&mut self, limit: usize, yielding: Option<(usize, usize)>) -> Result<RunRangePage> {
        match self.mode {
            RangeMode::Ordinary { .. } => self.ordinary_page(limit, yielding),
            RangeMode::Bounded { .. } => self.bounded_page(limit, yielding, u64::MAX),
        }
    }

    fn should_yield(
        &self,
        yielding: Option<(usize, usize)>,
        records: usize,
        has_last: bool,
    ) -> bool {
        yielding.is_some_and(|(held, keys)| {
            let (rows, cost) = (held + records, self.owner.costs.examined_keys);
            rows > 0
                && keys > 0
                && has_last
                && cost.saturating_add(keys.saturating_mul(rows).saturating_add(16) as u64)
                    > self.owner.budget.keys
        })
    }

    fn ordinary_page(
        &mut self,
        limit: usize,
        yielding: Option<(usize, usize)>,
    ) -> Result<RunRangePage> {
        let mut resume = None;
        let mut out = Vec::new();
        let mut last = None;
        while !self.exhausted && out.len() < limit {
            if self.should_yield(yielding, out.len(), last.is_some()) {
                resume = last.take();
                break;
            }
            let RangeMode::Ordinary { records, held } = &mut self.mode else {
                unreachable!()
            };
            let (key, found) = match held.take() {
                Some(item) => item,
                None => match records.next() {
                    Some(next) => next?,
                    None => {
                        self.exhausted = true;
                        break;
                    }
                },
            };
            if self.owner.examine(1).is_err() {
                if let RangeMode::Ordinary { held, .. } = &mut self.mode {
                    *held = Some((key, found));
                }
                if yielding.is_some() && last.is_some() {
                    resume = last.take();
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            if let Found::Value(value) = found {
                self.owner.charge((key.len() + value.len()) as u64)?;
                out.push((storage_key(&self.collection, &key)?, value));
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
                .checked_add(self.coordinate_bytes())
                .ok_or(Error::ReadBudgetExceeded)?;
            let value = self.owner.streamed_value(&self.collection, &key, maximum);
            self.owner.shared_metadata_bytes = before;
            let value = value?;
            if let RangeMode::Bounded {
                lower, exclusive, ..
            } = &mut self.mode
            {
                *lower = run_key(&self.collection, key.as_bytes())?;
                *exclusive = true;
            }
            last = true;
            if let Some(value) = value {
                out.push((key, value));
            }
        }
        Ok(RunRangePage {
            records: out,
            exhausted: self.exhausted,
            resume,
        })
    }

    fn next_bounded_key(&self) -> Result<Option<String>> {
        let RangeMode::Bounded {
            lower,
            upper,
            exclusive,
        } = &self.mode
        else {
            unreachable!()
        };
        let mut selected: Option<Vec<u8>> = None;
        for slot in 0..self.owner.runs.len() {
            let end = selected.as_deref().or(upper.as_deref());
            if !self.owner.runs[slot].0.meta.overlaps(lower, end) {
                continue;
            }
            let extra = lower
                .len()
                .checked_add(end.map_or(0, <[u8]>::len))
                .and_then(|n| n.checked_add(selected.as_ref().map_or(0, Vec::capacity)))
                .ok_or(Error::ReadBudgetExceeded)? as u64;
            let (reader, admission, reserved) = self.owner.open_stream(slot, extra)?;
            if let Some(next) = reader.stream_key(lower, end, *exclusive, admission, reserved)? {
                selected = Some(next);
            }
        }
        selected
            .map(|k| storage_key(&self.collection, &k))
            .transpose()
    }

    fn coordinate_bytes(&self) -> u64 {
        let RangeMode::Bounded { lower, upper, .. } = &self.mode else {
            return 0;
        };
        (self.collection.capacity() + lower.capacity() + upper.as_ref().map_or(0, Vec::capacity))
            as u64
    }

    fn resume_key(&self) -> Result<String> {
        let RangeMode::Bounded { lower, .. } = &self.mode else {
            unreachable!()
        };
        let retained = self
            .owner
            .shared_metadata_bytes
            .max(self.owner.retained_bytes())
            .saturating_add(self.owner.costs.budget_bytes)
            .saturating_add(self.coordinate_bytes())
            .saturating_add(lower.len() as u64);
        if retained > lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        storage_key(&self.collection, lower)
    }
}
