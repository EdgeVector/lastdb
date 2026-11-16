//! Ordered run ranges, k-way merge, and merged-page iteration over one held CURRENT.

use super::read::RunRead;
use super::{run_key, storage_key};
use crate::disk_run::{Found, IndexBlock, RunReader, RunWriter};
use crate::{Error, Result};
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct RunRange {
    reader: Option<Arc<RunReader>>,
    paths: std::vec::IntoIter<PathBuf>,
    data_key: [u8; 32],
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    next_index: usize,
    current_index: Option<IndexBlock>,
    next_block: usize,
    positioned: bool,
    records: std::vec::IntoIter<(Vec<u8>, Found)>,
    finished: bool,
}

impl RunRange {
    pub(crate) fn sequence(
        paths: Vec<PathBuf>,
        data_key: &[u8; 32],
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            reader: None,
            paths: paths.into_iter(),
            data_key: *data_key,
            lower,
            upper,
            next_index: 0,
            current_index: None,
            next_block: 0,
            positioned: false,
            records: Vec::new().into_iter(),
            finished: false,
        }
    }

    pub(crate) fn set_reader(&mut self, reader: Arc<RunReader>) {
        self.next_index = match &self.lower {
            Bound::Included(k) | Bound::Excluded(k) => reader.index_for(k).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        self.current_index = None;
        self.next_block = 0;
        self.positioned = false;
        self.reader = Some(reader);
    }

    fn above_upper(upper: &Bound<Vec<u8>>, key: &[u8]) -> bool {
        match upper {
            Bound::Included(u) => key > u.as_slice(),
            Bound::Excluded(u) => key >= u.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        loop {
            let Some(reader) = &self.reader else {
                let Some(path) = self.paths.next() else {
                    return Ok(false);
                };
                self.set_reader(Arc::new(RunReader::open(&path, &self.data_key)?));
                continue;
            };
            if self
                .current_index
                .as_ref()
                .is_none_or(|idx| self.next_block >= idx.blocks.len())
            {
                if self.next_index >= reader.top.indexes.len() {
                    self.reader = None;
                    continue;
                }
                let idx = reader.read_index(self.next_index, None)?;
                self.next_index += 1;
                let first = !std::mem::replace(&mut self.positioned, true);
                self.next_block = match (&self.lower, first) {
                    (Bound::Included(k) | Bound::Excluded(k), true) => {
                        idx.block_pos(k).unwrap_or(0)
                    }
                    _ => 0,
                };
                self.current_index = Some(idx);
                continue;
            }
            let block = &self.current_index.as_ref().unwrap().blocks[self.next_block];
            if Self::above_upper(&self.upper, &block.first_key) {
                return Ok(false);
            }
            self.records = reader.read_data(block, None)?.into_iter();
            self.next_block += 1;
            return Ok(true);
        }
    }
}

impl Iterator for RunRange {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.finished {
            for (key, found) in self.records.by_ref() {
                let below = match &self.lower {
                    Bound::Included(l) => key < *l,
                    Bound::Excluded(l) => key <= *l,
                    Bound::Unbounded => false,
                };
                if !below {
                    if Self::above_upper(&self.upper, &key) {
                        self.finished = true;
                        return None;
                    }
                    return Some(Ok((key, found)));
                }
            }
            match self.load_next_block() {
                Ok(ok) => self.finished = !ok,
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }
        None
    }
}

type MergeHead = Option<(Vec<u8>, Found)>;

pub(crate) struct MergeIter {
    sources: Vec<RunRange>,
    heads: Option<Vec<MergeHead>>,
    finished: bool,
}

impl MergeIter {
    pub(crate) fn new(sources: Vec<RunRange>) -> Self {
        Self {
            sources,
            heads: None,
            finished: false,
        }
    }
}

impl Iterator for MergeIter {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let step = (|| -> Result<Option<(Vec<u8>, Found)>> {
            let heads = match &mut self.heads {
                Some(h) => h,
                slot => slot.insert(
                    self.sources
                        .iter_mut()
                        .map(|r| r.next().transpose())
                        .collect::<Result<_>>()?,
                ),
            };
            let winner = heads
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().map(|(k, _)| (k, i)))
                .min_by(|(a, i), (b, j)| a.cmp(b).then(i.cmp(j)))
                .map(|(_, i)| i);
            let Some(win) = winner else { return Ok(None) };
            let record = heads[win].take().unwrap();
            for (slot, src) in heads[win + 1..]
                .iter_mut()
                .zip(&mut self.sources[win + 1..])
            {
                if slot.as_ref().is_some_and(|(k, _)| *k == record.0) {
                    *slot = src.next().transpose()?;
                }
            }
            heads[win] = self.sources[win].next().transpose()?;
            Ok(Some(record))
        })();
        self.finished |= !matches!(step, Ok(Some(_)));
        step.transpose()
    }
}

pub(crate) fn merge_runs(sources: Vec<RunRange>, writer: &mut RunWriter) -> Result<u64> {
    let mut written = 0;
    for record in MergeIter::new(sources) {
        let (key, found) = record?;
        let value = match &found {
            Found::Value(v) => Some(v.as_slice()),
            Found::Tombstone => None,
        };
        writer.push(&key, value)?;
        written += 1;
    }
    Ok(written)
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
