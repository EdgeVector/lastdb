//! Ordered ranges under one hash, merged across the held runs.

use super::keys::{prefix_end, run_key, storage_key};
use super::read::{ReadBudget, RunRead, RunReadCosts};
use crate::disk_run::{value, Found, MergeIter, MergeSource, RunSequence};
use crate::{Error, Result};
use std::ops::Bound;
use std::path::PathBuf;
#[path = "range/stream.rs"]
mod stream;

/// Half-open encoded range suffix bounds within the held hash.
pub struct RangeBounds {
    /// Inclusive encoded range suffix; empty means the first range under this hash.
    pub start: String,
    /// Exclusive encoded range suffix; None means the exact end of this hash.
    pub end: Option<String>,
}

/// One page of live records. Tombstones count as examined keys but do not
/// appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRangePage {
    /// Complete storage keys and their values, in key order.
    pub records: Vec<(String, Vec<u8>)>,
    /// True when the range has no more records.
    pub exhausted: bool,
    /// Set only by [`RunHashRange::next_page_yielding`]: the call stopped
    /// before the page was full because its key budget ran out or was about
    /// to. Every key up to and including this storage key was examined; the
    /// next call continues above it.
    pub resume: Option<String>,
}

/// An in-process cursor over one range of the held CURRENT.
pub struct RunHashRange<'a> {
    mode: RangeMode<'a>,
}
enum RangeMode<'a> {
    Ordinary(OrdinaryRange<'a>),
    Bounded(stream::BoundedRange<'a>),
}
struct OrdinaryRange<'a> {
    records: MergeIter<'a>,
    collection: String,
    costs: &'a mut RunReadCosts,
    budget: ReadBudget,
    exhausted: bool,
    /// A record taken from `records` but not examined: the key budget was
    /// out. The next page starts with it.
    held: Option<(Vec<u8>, Found)>,
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
        self.range_between(collection, lower, upper, false)
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
        self.range_between(collection, lower, Some(upper), false)
    }

    /// Continue an administrative range strictly above one complete storage
    /// key. No sentinel is appended, so a maximum-length key remains valid.
    pub fn administrative_range_after(
        &mut self,
        collection: &str,
        after: &[u8],
        end: &[u8],
    ) -> Result<RunHashRange<'_>> {
        self.charge((collection.len() + after.len() + end.len()) as u64)?;
        let lower = run_key(collection, after)?;
        let upper = run_key(collection, end)?;
        self.range_between(collection, lower, Some(upper), true)
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
                mode: RangeMode::Bounded(stream::BoundedRange::new(
                    self, collection, lower, upper, exclusive,
                )),
            });
        }
        // The base runs do not overlap, so they are one source that opens one
        // run at a time; each other run is a source of its own.
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
        self.costs.runs_opened += base.len() as u64;
        let RunRead {
            runs,
            key,
            costs,
            budget,
            ..
        } = self;
        let upper_bound = match &upper {
            Some(upper) => Bound::Excluded(upper.as_slice()),
            None => Bound::Unbounded,
        };
        let lower_bound = if exclusive {
            Bound::Excluded(lower.as_slice())
        } else {
            Bound::Included(lower.as_slice())
        };
        let mut sources: Vec<MergeSource<'_>> = runs[..base_from]
            .iter()
            .filter(|(file, _)| file.meta.overlaps(&lower, upper.as_deref()))
            .filter_map(|(_, reader)| reader.as_ref())
            .map(|reader| MergeSource::Run(reader.range(lower_bound, upper_bound)))
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
    /// Charge a selected pending-write value to this range's call budget.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        if self.costs.examined_keys >= self.budget.keys {
            return Err(Error::ReadBudgetExceeded);
        }
        let bytes = self
            .costs
            .budget_bytes
            .saturating_add(key_bytes.saturating_add(body_bytes) as u64);
        if bytes > self.budget.bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        self.costs.examined_keys += 1;
        self.costs.budget_bytes = bytes;
        Ok(())
    }

    /// Up to `limit` live records. Each examined record, tombstones
    /// included, counts against the call's key budget.
    pub fn next_page(&mut self, limit: usize) -> Result<RunRangePage> {
        self.page(limit, None)
    }

    /// Like [`Self::next_page`], but a key budget that runs out once a key
    /// was examined ends the page with `resume` instead of an error. The
    /// page stops early while the call still has `keys_per_row` keys for
    /// each of the `held_rows` rows the caller holds and each row of this
    /// page. A budget failure before any key was examined is still an error.
    pub fn next_page_yielding(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
    ) -> Result<RunRangePage> {
        self.page(limit, Some((held_rows, keys_per_row)))
    }

    fn page(&mut self, limit: usize, yielding: Option<(usize, usize)>) -> Result<RunRangePage> {
        let mut records = Vec::new();
        let mut last: Option<Vec<u8>> = None;
        let mut resume = None;
        while !self.exhausted && records.len() < limit {
            if let Some((held, keys_per_row)) = yielding {
                let rows = held + records.len();
                let needed = keys_per_row.saturating_mul(rows).saturating_add(16) as u64;
                let low = self.costs.examined_keys.saturating_add(needed) > self.budget.keys;
                if rows > 0 && keys_per_row > 0 && low && last.is_some() {
                    resume = last.take();
                    break;
                }
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
            // As in v1, a range that ends exactly at the key budget is
            // exhausted, not over budget. A yielding page that has examined
            // a key resumes after it.
            if self.costs.examined_keys >= self.budget.keys {
                self.held = Some((key, found));
                if yielding.is_some() && last.is_some() {
                    resume = last.take();
                    break;
                }
                return Err(Error::ReadBudgetExceeded);
            }
            self.costs.examined_keys += 1;
            let Found::Value(stored) = found else {
                last = yielding.map(|_| key);
                continue;
            };
            let value = value::decode(stored, super::MAX_VALUE_BYTES)?;
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
        let resume = match resume {
            Some(key) => Some(storage_key(&self.collection, &key)?),
            None => None,
        };
        Ok(RunRangePage {
            records,
            exhausted: self.exhausted,
            resume,
        })
    }
}

impl RunHashRange<'_> {
    /// Charge selected pending records to the same range budget.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        match &mut self.mode {
            RangeMode::Ordinary(r) => r.account_memory_record(key_bytes, body_bytes),
            RangeMode::Bounded(r) => r.account(key_bytes, body_bytes),
        }
    }
    /// Up to `limit` live records under this exact root.
    pub fn next_page(&mut self, limit: usize) -> Result<RunRangePage> {
        match &mut self.mode {
            RangeMode::Ordinary(r) => r.next_page(limit),
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
            RangeMode::Ordinary(r) => r.next_page_yielding(limit, held_rows, keys_per_row),
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
    /// One bounded backup page with fresh per-page work admission. The cursor
    /// keeps its authenticated next-key heads; opened-run costs stay cumulative.
    /// The caller must release the preceding page before it requests another.
    pub fn next_backup_page(&mut self, limit: usize, maximum: u64) -> Result<RunRangePage> {
        match &mut self.mode {
            RangeMode::Bounded(range) => {
                range.reset_page_budget();
                range.page(limit, Some((0, 0)), maximum)
            }
            RangeMode::Ordinary(_) => Err(Error::Config(
                "backup run pages require a captured or stopped-home read".into(),
            )),
        }
    }
}
