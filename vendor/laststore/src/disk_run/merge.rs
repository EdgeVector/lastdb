//! Metadata memo, streaming blocks, ordered run ranges, and k-way merge over run files.

use super::codec::corrupt;
use super::reader::{read_vec, RunReader};
use super::writer::RunWriter;
use super::{Found, IndexBlock, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::frame::{DecodeScratch, VerifiedFrameReader};
use crate::{Error, Result};
use std::io::{self, Read};
use std::mem::size_of;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

pub(crate) struct Block(
    pub(super) VerifiedFrameReader,
    pub(crate) Vec<u8>,
    pub(super) bool,
    pub(crate) (u64, u64, u64),
);

impl Block {
    fn varint(&mut self) -> Result<Option<u64>> {
        let mut val = 0u64;
        for shift in (0..64).step_by(7) {
            let mut b = [0u8; 1];
            if self.0.read(&mut b)? == 0 {
                return (shift == 0)
                    .then_some(None)
                    .ok_or_else(|| corrupt("stream varint eof"));
            }
            if shift == 63 && b[0] > 1 {
                break;
            }
            val |= u64::from(b[0] & 0x7f) << shift;
            if b[0] & 0x80 == 0 {
                return Ok(Some(val));
            }
        }
        Err(corrupt("stream varint overflow"))
    }

    fn length(&mut self, max: usize) -> Result<usize> {
        self.varint()?
            .and_then(|n| usize::try_from(n).ok())
            .filter(|&n| n <= max)
            .ok_or_else(|| corrupt("stream len"))
    }

    pub(crate) fn next(&mut self, first_key: Option<&[u8]>) -> Result<Option<bool>> {
        let skip = std::mem::take(&mut self.3 .0);
        if io::copy(&mut (&mut self.0).take(skip), &mut io::sink())? != skip {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        let (first, bad) = (!self.2, || {
            corrupt("data block does not start at its index entry")
        });
        let Some(sh) = self.varint()? else {
            return (!(first && first_key.is_some()))
                .then_some(None)
                .ok_or_else(bad);
        };
        let shared = usize::try_from(sh)
            .ok()
            .filter(|&n| n <= self.1.len())
            .ok_or_else(|| corrupt("key prefix"))?;
        let suffix = self.length(MAX_RUN_KEY_BYTES - shared)?;
        let len = shared + suffix;
        if (len.max(self.1.capacity()) as u64).saturating_mul(2) > self.3 .1 {
            return Err(Error::ReadBudgetExceeded);
        }
        let tail = read_vec(&mut self.0, suffix)?;
        if len == 0 || (!first && tail.as_slice() <= &self.1[shared..]) {
            return Err(corrupt("stream block key order"));
        }
        self.1.truncate(shared);
        self.1
            .try_reserve_exact(len.saturating_sub(self.1.len()))
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.1.extend_from_slice(&tail);
        drop(tail);
        self.2 = true;
        if first && first_key.is_some_and(|exp| self.1 != exp) {
            return Err(bad());
        }
        let tag = self.length(MAX_RUN_VALUE_BYTES + 1)?;
        self.3 .0 = tag.saturating_sub(1) as u64;
        Ok(Some(tag != 0))
    }

    pub(super) fn finish(mut self) -> Result<()> {
        while self.next(None)?.is_some() {}
        Ok(())
    }
}

const MAX_MEMO_ENTRIES: usize = 64;
const MAX_MEMO_BYTES: u64 = 256 * 1024;
type MemoItem = (Uuid, usize, Arc<IndexBlock>, u64);
type Bounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);
type Cursor = (usize, Option<IndexBlock>, usize, bool);
type RecIter = std::vec::IntoIter<(Vec<u8>, Found)>;

pub(crate) struct MetadataMemo(Vec<MemoItem>, pub(crate) u64, u64);

impl MetadataMemo {
    pub(crate) fn new(budget: u64) -> Self {
        Self(Vec::new(), 0, MAX_MEMO_BYTES.min(budget / 16))
    }

    pub(crate) fn read(
        &mut self,
        r: &RunReader,
        n: usize,
        s: &mut DecodeScratch,
    ) -> Result<Arc<IndexBlock>> {
        let run = r.4 .0;
        if let Some((_, _, b, _)) = self.0.iter().find(|(id, i, _, _)| (*id, *i) == (run, n)) {
            return Ok(Arc::clone(b));
        }
        let block = Arc::new(r.read_index(n, Some(s), None)?);
        let item_sz = size_of::<MemoItem>();
        let bytes = (size_of::<IndexBlock>() + 2 * size_of::<usize>()) as u64 + block.footprint();
        let cap_ok = self.0.capacity() > 0 || self.0.try_reserve_exact(MAX_MEMO_ENTRIES).is_ok();
        if bytes.saturating_add((MAX_MEMO_ENTRIES * item_sz) as u64) <= self.2 && cap_ok {
            if self.1 == 0 {
                self.1 = (self.0.capacity() * item_sz) as u64;
            }
            while self.0.len() >= MAX_MEMO_ENTRIES || self.1.saturating_add(bytes) > self.2 {
                self.1 -= self.0.remove(0).3;
            }
            self.0.push((run, n, Arc::clone(&block), bytes));
            self.1 += bytes;
        }
        Ok(block)
    }
}

type PathIter = std::vec::IntoIter<PathBuf>;
pub(crate) struct RunRange(
    Option<Arc<RunReader>>,
    PathIter,
    [u8; 32],
    Bounds,
    Cursor,
    RecIter,
    bool,
);

impl RunRange {
    pub(crate) fn sequence(
        paths: Vec<PathBuf>,
        key: &[u8; 32],
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
    ) -> Self {
        Self(
            None,
            paths.into_iter(),
            *key,
            (lo, hi),
            (0, None, 0, false),
            Vec::new().into_iter(),
            false,
        )
    }

    pub(crate) fn set_reader(&mut self, r: Arc<RunReader>) {
        let idx = match &self.3 .0 {
            Bound::Included(k) | Bound::Excluded(k) => r.index_for(k).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        (self.4, self.0) = ((idx, None, 0, false), Some(r));
    }

    fn above_upper(hi: &Bound<Vec<u8>>, key: &[u8]) -> bool {
        match hi {
            Bound::Included(u) => key > u.as_slice(),
            Bound::Excluded(u) => key >= u.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        loop {
            let Some(reader) = &self.0 else {
                let Some(path) = self.1.next() else {
                    return Ok(false);
                };
                self.set_reader(Arc::new(RunReader::open(&path, &self.2)?));
                continue;
            };
            let cur = &mut self.4;
            if cur.1.as_ref().is_none_or(|idx| cur.2 >= idx.0.len()) {
                if cur.0 >= reader.5 .1.len() {
                    self.0 = None;
                    continue;
                }
                let idx = reader.read_index(cur.0, None, None)?;
                cur.0 += 1;
                let first = !std::mem::replace(&mut cur.3, true);
                cur.2 = match (&self.3 .0, first) {
                    (Bound::Included(k) | Bound::Excluded(k), true) => {
                        idx.block_pos(k).unwrap_or(0)
                    }
                    _ => 0,
                };
                cur.1 = Some(idx);
                continue;
            }
            let block = &cur.1.as_ref().unwrap().0[cur.2];
            if Self::above_upper(&self.3 .1, &block.0) {
                return Ok(false);
            }
            self.5 = reader.read_data(block, None)?.into_iter();
            cur.2 += 1;
            return Ok(true);
        }
    }
}

impl Iterator for RunRange {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.6 {
            for (key, found) in self.5.by_ref() {
                let below = match &self.3 .0 {
                    Bound::Included(l) => key < *l,
                    Bound::Excluded(l) => key <= *l,
                    Bound::Unbounded => false,
                };
                if !below {
                    if Self::above_upper(&self.3 .1, &key) {
                        self.6 = true;
                        return None;
                    }
                    return Some(Ok((key, found)));
                }
            }
            match self.load_next_block() {
                Ok(ok) => self.6 = !ok,
                Err(e) => {
                    self.6 = true;
                    return Some(Err(e));
                }
            }
        }
        None
    }
}

type MergeHeads = Vec<Option<(Vec<u8>, Found)>>;
pub(crate) struct MergeIter(Vec<RunRange>, Option<MergeHeads>, bool);

impl MergeIter {
    pub(crate) fn new(sources: Vec<RunRange>) -> Self {
        Self(sources, None, false)
    }
}

impl Iterator for MergeIter {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.2 {
            return None;
        }
        let step = (|| -> Result<Option<(Vec<u8>, Found)>> {
            let heads = match &mut self.1 {
                Some(h) => h,
                s => s.insert(
                    self.0
                        .iter_mut()
                        .map(|r| r.next().transpose())
                        .collect::<Result<_>>()?,
                ),
            };
            let win = heads
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().map(|(k, _)| (k, i)))
                .min_by(|(a, i), (b, j)| a.cmp(b).then(i.cmp(j)))
                .map(|(_, i)| i);
            let Some(win) = win else { return Ok(None) };
            let rec = heads[win].take().unwrap();
            for (slot, src) in heads[win + 1..].iter_mut().zip(&mut self.0[win + 1..]) {
                if slot.as_ref().is_some_and(|(k, _)| *k == rec.0) {
                    *slot = src.next().transpose()?;
                }
            }
            heads[win] = self.0[win].next().transpose()?;
            Ok(Some(rec))
        })();
        self.2 |= !matches!(step, Ok(Some(_)));
        step.transpose()
    }
}

pub(crate) fn merge_runs(sources: Vec<RunRange>, writer: &mut RunWriter) -> Result<u64> {
    let mut written = 0;
    for record in MergeIter::new(sources) {
        let (key, found) = record?;
        writer.push(
            &key,
            if let Found::Value(v) = &found {
                Some(v)
            } else {
                None
            },
        )?;
        written += 1;
    }
    Ok(written)
}

pub(crate) fn count_merged_keys(
    key: &[u8; 32],
    retained: u64,
    runs: &[PathBuf],
    base: Vec<PathBuf>,
    deadline: Instant,
) -> (bool, u64) {
    let mut merged = 0u64;
    let complete = (|| -> Result<()> {
        let count = runs.len() + usize::from(!base.is_empty());
        if count == 0 {
            return Ok(());
        }
        if count > 512 || retained > 56 << 20 {
            return Err(Error::ReadBudgetExceeded);
        }
        let allow = ((56 << 20) - retained) / count as u64;
        let mut sources: Vec<_> = runs
            .iter()
            .map(|p| Ok(Arc::new(RunReader::open_bounded(p, key, allow, 0)?).iter()))
            .collect::<Result<_>>()?;
        if !base.is_empty() {
            sources.push(RunRange::sequence(
                base,
                key,
                Bound::Unbounded,
                Bound::Unbounded,
            ));
        }
        for item in MergeIter::new(sources) {
            if merged >= 50_000_000 || Instant::now() >= deadline {
                return Err(Error::ReadBudgetExceeded);
            }
            let (rk, _) = item?;
            if rk
                .first_chunk::<2>()
                .is_none_or(|p| rk.len() < 2 + u16::from_be_bytes(*p) as usize)
            {
                return Err(corrupt("diagnostic prefix"));
            }
            merged += 1;
        }
        Ok(())
    })()
    .is_ok();
    (complete, merged)
}
