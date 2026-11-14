//! Streaming blocks, ordered run ranges, and k-way merge over run files.

use super::codec::corrupt;
use super::reader::{read_vec, RunReader};
use super::writer::RunWriter;
use super::{Found, IndexBlock, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
use crate::frame::VerifiedFrameReader;
use crate::{Error, Result};
use std::io::{self, Read};
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct Block {
    pub(super) reader: VerifiedFrameReader,
    pub(crate) key: Vec<u8>,
    started: bool,
    pub(crate) pending_value_bytes: u64,
    key_budget_bytes: u64,
    pub(crate) avail_bytes: u64,
}

impl Block {
    pub(super) fn new(
        reader: VerifiedFrameReader,
        key_budget_bytes: u64,
        avail_bytes: u64,
    ) -> Self {
        Self {
            reader,
            key: Vec::new(),
            started: false,
            pending_value_bytes: 0,
            key_budget_bytes,
            avail_bytes,
        }
    }

    fn varint(&mut self) -> Result<Option<u64>> {
        let mut val = 0u64;
        for shift in (0..64).step_by(7) {
            let mut byte = [0u8; 1];
            if self.reader.read(&mut byte)? == 0 {
                return (shift == 0)
                    .then_some(None)
                    .ok_or_else(|| corrupt("stream varint eof"));
            }
            if shift == 63 && byte[0] > 1 {
                break;
            }
            val |= u64::from(byte[0] & 0x7f) << shift;
            if byte[0] & 0x80 == 0 {
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
        let skip = std::mem::take(&mut self.pending_value_bytes);
        if io::copy(&mut (&mut self.reader).take(skip), &mut io::sink())? != skip {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
        let (first, bad) = (!self.started, || {
            corrupt("data block does not start at its index entry")
        });
        let Some(sh) = self.varint()? else {
            return (!(first && first_key.is_some()))
                .then_some(None)
                .ok_or_else(bad);
        };
        let shared = usize::try_from(sh)
            .ok()
            .filter(|&n| n <= self.key.len())
            .ok_or_else(|| corrupt("key prefix"))?;
        let suffix = self.length(MAX_RUN_KEY_BYTES - shared)?;
        let len = shared + suffix;
        if (len.max(self.key.capacity()) as u64).saturating_mul(2) > self.key_budget_bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        let tail = read_vec(&mut self.reader, suffix)?;
        if len == 0 || (!first && tail.as_slice() <= &self.key[shared..]) {
            return Err(corrupt("stream block key order"));
        }
        self.key.truncate(shared);
        self.key
            .try_reserve_exact(len.saturating_sub(self.key.len()))
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.key.extend_from_slice(&tail);
        drop(tail);
        self.started = true;
        if first && first_key.is_some_and(|exp| self.key != exp) {
            return Err(bad());
        }
        let tag = self.length(MAX_RUN_VALUE_BYTES + 1)?;
        self.pending_value_bytes = tag.saturating_sub(1) as u64;
        Ok(Some(tag != 0))
    }

    pub(super) fn finish(mut self) -> Result<()> {
        while self.next(None)?.is_some() {}
        Ok(())
    }
}

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
                let idx = reader.read_index(self.next_index, None, None)?;
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
