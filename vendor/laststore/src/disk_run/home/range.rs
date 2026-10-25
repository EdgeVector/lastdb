//! Ordered ranges under one hash, merged across the held runs.

use super::keys::{prefix_end, run_key, storage_key};
use super::read::{RunRead, RunReadCosts};
use crate::disk_pack::{RangeBounds, ReadBudget};
use crate::disk_run::{Found, MergeIter};
use crate::{Error, Result};
use std::ops::Bound;

/// One page of live records. Tombstones count as examined keys but do not
/// appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRangePage {
    /// Complete storage keys and their values, in key order.
    pub records: Vec<(String, Vec<u8>)>,
    /// True when the range has no more records.
    pub exhausted: bool,
}

/// An in-process cursor over one range of the held CURRENT.
pub struct RunHashRange<'a> {
    records: MergeIter<'a>,
    collection: String,
    costs: &'a mut RunReadCosts,
    budget: ReadBudget,
    exhausted: bool,
}

impl RunRead {
    /// Records under one complete encoded hash, inside `bounds`.
    /// `prefix` ends at its first NUL, which closes the encoded hash.
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
        self.range_between(collection, lower, upper)
    }

    /// Internal metadata range over one collection, `[start, end)` in
    /// storage key bytes. Node bootstrap and maintenance use it; user
    /// molecule reads use [`RunRead::hash_range`].
    pub fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.charge((collection.len() + start.len() + end.len()) as u64)?;
        let lower = run_key(collection, start)?;
        let upper = run_key(collection, end)?;
        self.range_between(collection, lower, Some(upper))
    }

    fn range_between(
        &mut self,
        collection: &str,
        lower: Vec<u8>,
        upper: Option<Vec<u8>>,
    ) -> Result<RunHashRange<'_>> {
        for slot in 0..self.runs.len() {
            if self.runs[slot].0.meta.overlaps(&lower, upper.as_deref()) {
                self.reader(slot)?;
            }
        }
        let RunRead {
            runs,
            costs,
            budget,
            ..
        } = self;
        let upper_bound = match &upper {
            Some(upper) => Bound::Excluded(upper.as_slice()),
            None => Bound::Unbounded,
        };
        let sources = runs
            .iter()
            .filter(|(file, _)| file.meta.overlaps(&lower, upper.as_deref()))
            .filter_map(|(_, reader)| reader.as_ref())
            .map(|reader| reader.range(Bound::Included(lower.as_slice()), upper_bound))
            .collect();
        Ok(RunHashRange {
            records: MergeIter::new(sources),
            collection: collection.to_string(),
            costs,
            budget: *budget,
            exhausted: false,
        })
    }
}

impl RunHashRange<'_> {
    /// Up to `limit` live records. Each examined record, tombstones
    /// included, counts against the call's key budget.
    pub fn next_page(&mut self, limit: usize) -> Result<RunRangePage> {
        let mut records = Vec::new();
        while !self.exhausted && records.len() < limit {
            let Some(next) = self.records.next() else {
                self.exhausted = true;
                break;
            };
            let (key, found) = next?;
            if self.costs.examined_keys >= self.budget.keys {
                return Err(Error::ReadBudgetExceeded);
            }
            self.costs.examined_keys += 1;
            let Found::Value(value) = found else {
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
        }
        Ok(RunRangePage {
            records,
            exhausted: self.exhausted,
        })
    }
}
