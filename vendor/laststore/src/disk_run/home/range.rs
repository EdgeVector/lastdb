//! Ordered ranges under one hash, merged across the held runs.

use super::read::{ReadBudget, RunRead, RunReadCosts};
use super::stream::{should_yield, BoundedRange};
use super::{prefix_end, run_key, storage_key};
use crate::disk_run::{Found, MergeIter, MergeSource, RunSequence};
use crate::{Error, Result};
use std::ops::Bound;
use std::path::PathBuf;

/// Half-open encoded range suffix bounds within the held hash.
pub struct RangeBounds {
    /// Inclusive encoded range suffix; empty means the first range under this hash.
    pub start: String,
    /// Exclusive encoded range suffix; None means the exact end of this hash.
    pub end: Option<String>,
}

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
    mode: RangeMode<'a>,
}

enum RangeMode<'a> {
    Ordinary(OrdinaryRange<'a>),
    Bounded(BoundedRange<'a>),
}

struct OrdinaryRange<'a> {
    records: MergeIter<'a>,
    collection: String,
    costs: &'a mut RunReadCosts,
    budget: ReadBudget,
    exhausted: bool,
    held: Option<(Vec<u8>, Found)>,
}

impl RunRead {
    /// Records under one complete encoded hash, inside `bounds`.
    pub fn hash_range(
        &mut self,
        collection: &str,
        prefix: &str,
        bounds: RangeBounds,
    ) -> Result<RunHashRange<'_>> {
        if prefix.len() < 2 || prefix.find('\0') != Some(prefix.len() - 1) {
            return Err(Error::Config(
                "disk run range requires one complete first-NUL-terminated encoded hash".into(),
            ));
        }
        self.charge((collection.len() + prefix.len() + bounds.start.len()) as u64)?;
        let base = run_key(collection, prefix.as_bytes())?;
        let lower = run_key(collection, format!("{prefix}{}", bounds.start).as_bytes())?;
        let upper = match bounds.end {
            Some(end) => Some(run_key(collection, format!("{prefix}{end}").as_bytes())?),
            None => prefix_end(&base),
        };
        self.range_between(collection, lower, upper, false)
    }

    /// Internal metadata range over one collection, `[start, end)` in storage key bytes.
    pub fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.admin_range(collection, start, end, false)
    }

    /// Continue an administrative range strictly above one complete storage key.
    pub fn administrative_range_after(
        &mut self,
        collection: &str,
        after: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.admin_range(collection, after, end, true)
    }

    fn admin_range(
        &mut self,
        collection: &str,
        lower: &[u8],
        end: &[u8],
        exclusive: bool,
    ) -> Result<RunHashRange<'_>> {
        self.charge((collection.len() + lower.len() + end.len()) as u64)?;
        self.range_between(
            collection,
            run_key(collection, lower)?,
            Some(run_key(collection, end)?),
            exclusive,
        )
    }

    fn range_between(
        &mut self,
        collection: &str,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
        exclusive: bool,
    ) -> Result<RunHashRange<'_>> {
        if self.bounded_streams {
            return Ok(RunHashRange {
                mode: RangeMode::Bounded(BoundedRange {
                    owner: self,
                    collection: collection.into(),
                    lower,
                    upper,
                    exclusive,
                    exhausted: false,
                }),
            });
        }
        let base_from = self.runs.len() - self.manifest.base.len();
        for slot in 0..base_from {
            if self.runs[slot].0.meta.overlaps(&lower, upper.as_deref()) {
                self.reader(slot)?;
            }
        }
        let base: Vec<PathBuf> = self.runs[base_from..]
            .iter()
            .filter(|(file, _)| file.meta.overlaps(&lower, upper.as_deref()))
            .map(|(file, _)| file.path.clone())
            .collect();
        let Self {
            runs,
            key,
            costs,
            budget,
            ..
        } = self;
        let upper_ref = upper.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let lower_ref = if exclusive {
            Bound::Excluded(lower.as_slice())
        } else {
            Bound::Included(lower.as_slice())
        };
        let mut sources: Vec<MergeSource<'_>> = runs[..base_from]
            .iter()
            .filter(|(file, _)| file.meta.overlaps(&lower, upper.as_deref()))
            .filter_map(|(_, reader)| reader.as_ref())
            .map(|reader| MergeSource::Run(reader.range(lower_ref, upper_ref)))
            .collect();
        if !base.is_empty() {
            let upper = upper.map_or(Bound::Unbounded, Bound::Excluded);
            let lower = if exclusive {
                Bound::Excluded(lower)
            } else {
                Bound::Included(lower)
            };
            sources.push(MergeSource::Sequence(RunSequence::new(
                base, key, lower, upper,
            )));
        }
        Ok(RunHashRange {
            mode: RangeMode::Ordinary(OrdinaryRange {
                records: MergeIter::from_sources(sources),
                collection: collection.to_string(),
                costs,
                budget: *budget,
                exhausted: false,
                held: None,
            }),
        })
    }
}

impl OrdinaryRange<'_> {
    fn account(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        let bytes = self
            .costs
            .budget_bytes
            .saturating_add(key_bytes.saturating_add(body_bytes) as u64);
        if self.costs.examined_keys >= self.budget.keys || bytes > self.budget.bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        self.costs.examined_keys += 1;
        self.costs.budget_bytes = bytes;
        Ok(())
    }

    fn page(&mut self, limit: usize, yielding: Option<(usize, usize)>) -> Result<RunRangePage> {
        let (mut records, mut last, mut resume) = (Vec::new(), None, None);
        while !self.exhausted && records.len() < limit {
            if should_yield(
                yielding,
                records.len(),
                self.costs.examined_keys,
                self.budget.keys,
                last.is_some(),
            ) {
                resume = last.take();
                break;
            }
            let (key, found) = match self.held.take() {
                Some(held) => held,
                None => match self.records.next() {
                    Some(next) => next?,
                    None => {
                        self.exhausted = true;
                        break;
                    }
                },
            };
            if self.costs.examined_keys >= self.budget.keys {
                self.held = Some((key, found));
                if yielding.is_some() && last.is_some() {
                    resume = last.take();
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            self.costs.examined_keys += 1;
            let Found::Value(value) = found else {
                last = yielding.map(|_| key);
                continue;
            };
            let bytes = self
                .costs
                .budget_bytes
                .saturating_add((key.len() + value.len()) as u64);
            if bytes > self.budget.bytes {
                return Err(Error::ReadBudgetExceeded);
            }
            self.costs.budget_bytes = bytes;
            records.push((storage_key(&self.collection, &key)?, value));
            last = yielding.map(|_| key);
        }
        Ok(RunRangePage {
            records,
            exhausted: self.exhausted,
            resume: resume
                .map(|key| storage_key(&self.collection, &key))
                .transpose()?,
        })
    }
}

impl RunHashRange<'_> {
    /// Charge selected pending records to the same range budget.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        match &mut self.mode {
            RangeMode::Ordinary(r) => r.account(key_bytes, body_bytes),
            RangeMode::Bounded(r) => r.owner.account_memory_record(key_bytes, body_bytes),
        }
    }

    /// Up to `limit` live records under this exact root.
    pub fn next_page(&mut self, limit: usize) -> Result<RunRangePage> {
        match &mut self.mode {
            RangeMode::Ordinary(r) => r.page(limit, None),
            RangeMode::Bounded(r) => r.page(limit, None, u64::MAX),
        }
    }

    /// Yield at the same administrative key budget as the ordinary cursor.
    pub fn next_page_yielding(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
    ) -> Result<RunRangePage> {
        match &mut self.mode {
            RangeMode::Ordinary(r) => r.page(limit, Some((held_rows, keys_per_row))),
            RangeMode::Bounded(r) => r.page(limit, Some((held_rows, keys_per_row)), u64::MAX),
        }
    }

    /// Bound each body before allocation on a captured or stopped-home read.
    pub fn next_page_yielding_bounded(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
        maximum: u64,
    ) -> Result<RunRangePage> {
        match &mut self.mode {
            RangeMode::Bounded(r) => r.page(limit, Some((held_rows, keys_per_row)), maximum),
            RangeMode::Ordinary(_) => Err(Error::Config(
                "bounded run pages require a captured or stopped-home read".into(),
            )),
        }
    }
}
