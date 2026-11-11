//! Bounded run ranges and ordered k-way merge of runs, newest first.

use super::reader::{IndexBlock, RunReader};
use super::writer::RunWriter;
use super::Found;
use crate::Result;
use std::ops::{Bound, Deref};
use std::path::PathBuf;
use std::sync::Arc;

pub(super) enum ReaderRef<'a> {
    Borrowed(&'a RunReader),
    Shared(Arc<RunReader>),
}

impl Deref for ReaderRef<'_> {
    type Target = RunReader;

    fn deref(&self) -> &RunReader {
        match self {
            Self::Borrowed(r) => r,
            Self::Shared(r) => r,
        }
    }
}

pub(crate) struct RunRange<'a> {
    reader: ReaderRef<'a>,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    next_index: usize,
    index: Option<IndexBlock>,
    next_block: usize,
    positioned: bool,
    current: std::vec::IntoIter<(Vec<u8>, Found)>,
    done: bool,
}

impl<'a> RunRange<'a> {
    pub(super) fn new(reader: ReaderRef<'a>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Self {
        let next_index = match lower {
            Bound::Included(key) | Bound::Excluded(key) => reader.index_for(key).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        Self {
            reader,
            lower: owned(lower),
            upper: owned(upper),
            next_index,
            index: None,
            next_block: 0,
            positioned: false,
            current: Vec::new().into_iter(),
            done: false,
        }
    }

    fn below_lower(&self, key: &[u8]) -> bool {
        match &self.lower {
            Bound::Included(lower) => key < lower.as_slice(),
            Bound::Excluded(lower) => key <= lower.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn above_upper(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(upper) => key > upper.as_slice(),
            Bound::Excluded(upper) => key >= upper.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        loop {
            if self
                .index
                .as_ref()
                .is_none_or(|idx| self.next_block >= idx.blocks.len())
            {
                if self.next_index >= self.reader.index_count() {
                    return Ok(false);
                }
                let index = self.reader.read_index(self.next_index, None)?;
                self.next_index += 1;
                self.next_block = self.first_block_in(&index);
                self.index = Some(index);
                continue;
            }
            let block = &self.index.as_ref().expect("index loaded").blocks[self.next_block];
            if self.above_upper(&block.first_key) {
                return Ok(false);
            }
            self.current = self.reader.read_data(block, None)?.into_iter();
            self.next_block += 1;
            return Ok(true);
        }
    }

    fn first_block_in(&mut self, index: &IndexBlock) -> usize {
        if std::mem::replace(&mut self.positioned, true) {
            return 0;
        }
        match &self.lower {
            Bound::Included(key) | Bound::Excluded(key) => index
                .blocks
                .partition_point(|b| b.first_key.as_slice() <= key.as_slice())
                .saturating_sub(1),
            Bound::Unbounded => 0,
        }
    }
}

fn owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(k) => Bound::Included(k.to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn borrowed(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(k) => Bound::Included(k),
        Bound::Excluded(k) => Bound::Excluded(k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl Iterator for RunRange<'_> {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            if let Some((key, found)) = self.current.next() {
                if self.below_lower(&key) {
                    continue;
                }
                if self.above_upper(&key) {
                    self.done = true;
                    return None;
                }
                return Some(Ok((key, found)));
            }
            match self.load_next_block() {
                Ok(true) => {}
                Ok(false) => self.done = true,
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
        None
    }
}

pub(crate) enum MergeSource<'a> {
    Run(RunRange<'a>),
    Sequence(RunSequence),
}

impl Iterator for MergeSource<'_> {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Run(r) => r.next(),
            Self::Sequence(s) => s.next(),
        }
    }
}

pub(crate) struct RunSequence {
    paths: std::vec::IntoIter<PathBuf>,
    key: [u8; 32],
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    current: Option<RunRange<'static>>,
    failed: bool,
}

impl RunSequence {
    pub(crate) fn new(
        paths: Vec<PathBuf>,
        key: &[u8; 32],
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Self {
        Self {
            paths: paths.into_iter(),
            key: *key,
            lower,
            upper,
            current: None,
            failed: false,
        }
    }
}

impl Iterator for RunSequence {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.failed {
            if let Some(range) = self.current.as_mut() {
                if let Some(record) = range.next() {
                    return Some(record);
                }
                self.current = None;
            }
            let path = self.paths.next()?;
            match RunReader::open(&path, &self.key) {
                Ok(reader) => {
                    self.current = Some(
                        Arc::new(reader).shared_range(borrowed(&self.lower), borrowed(&self.upper)),
                    );
                }
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
        None
    }
}

pub(crate) struct MergeIter<'a> {
    sources: Vec<MergeSource<'a>>,
    heads: Vec<Option<(Vec<u8>, Found)>>,
    started: bool,
    done: bool,
}

impl<'a> MergeIter<'a> {
    pub(crate) fn new(sources: Vec<RunRange<'a>>) -> Self {
        Self::from_sources(sources.into_iter().map(MergeSource::Run).collect())
    }

    pub(crate) fn from_sources(sources: Vec<MergeSource<'a>>) -> Self {
        let heads = vec![None; sources.len()];
        Self {
            sources,
            heads,
            started: false,
            done: false,
        }
    }

    fn advance(&mut self, source: usize) -> Result<()> {
        self.heads[source] = self.sources[source].next().transpose()?;
        Ok(())
    }

    fn step(&mut self) -> Result<Option<(Vec<u8>, Found)>> {
        if !std::mem::replace(&mut self.started, true) {
            for source in 0..self.sources.len() {
                self.advance(source)?;
            }
        }
        let mut winner: Option<usize> = None;
        for (source, head) in self.heads.iter().enumerate() {
            let Some((key, _)) = head else { continue };
            if winner
                .and_then(|w| self.heads[w].as_ref())
                .is_none_or(|(best, _)| key < best)
            {
                winner = Some(source);
            }
        }
        let Some(winner) = winner else {
            return Ok(None);
        };
        let record = self.heads[winner].take().expect("winner has a head");
        for source in winner + 1..self.heads.len() {
            if self.heads[source]
                .as_ref()
                .is_some_and(|(k, _)| *k == record.0)
            {
                self.advance(source)?;
            }
        }
        self.advance(winner)?;
        Ok(Some(record))
    }
}

impl Iterator for MergeIter<'_> {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.step() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

pub(crate) fn merge_runs(
    sources: Vec<RunRange<'_>>,
    writer: &mut RunWriter,
    keep_tombstones: bool,
) -> Result<u64> {
    let mut written = 0;
    for record in MergeIter::new(sources) {
        let (key, found) = record?;
        match found {
            Found::Value(value) => writer.push(&key, Some(&value))?,
            Found::Tombstone if keep_tombstones => writer.push(&key, None)?,
            Found::Tombstone => continue,
        }
        written += 1;
    }
    Ok(written)
}
