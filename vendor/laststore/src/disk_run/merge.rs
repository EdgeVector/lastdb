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
            Bound::Included(k) | Bound::Excluded(k) => reader.index_for(k).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        Self {
            reader,
            lower: lower.map(<[u8]>::to_vec),
            upper: upper.map(<[u8]>::to_vec),
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
            Bound::Included(l) => key < l.as_slice(),
            Bound::Excluded(l) => key <= l.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn above_upper(&self, key: &[u8]) -> bool {
        match &self.upper {
            Bound::Included(u) => key > u.as_slice(),
            Bound::Excluded(u) => key >= u.as_slice(),
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
                self.next_block = if std::mem::replace(&mut self.positioned, true) {
                    0
                } else {
                    match &self.lower {
                        Bound::Included(k) | Bound::Excluded(k) => index.block_pos(k).unwrap_or(0),
                        Bound::Unbounded => 0,
                    }
                };
                self.index = Some(index);
                continue;
            }
            let block = &self.index.as_ref().unwrap().blocks[self.next_block];
            if self.above_upper(&block.first_key) {
                return Ok(false);
            }
            self.current = self.reader.read_data(block, None)?.into_iter();
            self.next_block += 1;
            return Ok(true);
        }
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
            if let Some(record) = self.current.as_mut().and_then(Iterator::next) {
                return Some(record);
            }
            self.current = None;
            let path = self.paths.next()?;
            match RunReader::open(&path, &self.key) {
                Ok(r) => {
                    self.current = Some(Arc::new(r).shared_range(
                        self.lower.as_ref().map(Vec::as_slice),
                        self.upper.as_ref().map(Vec::as_slice),
                    ));
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
    pub(crate) fn from_sources(sources: Vec<MergeSource<'a>>) -> Self {
        Self {
            heads: vec![None; sources.len()],
            sources,
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
        let Some(winner) = self
            .heads
            .iter()
            .enumerate()
            .filter_map(|(i, h)| h.as_ref().map(|(k, _)| (k, i)))
            .min_by(|(a, i), (b, j)| a.cmp(b).then(i.cmp(j)))
            .map(|(_, i)| i)
        else {
            return Ok(None);
        };
        let record = self.heads[winner].take().unwrap();
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
    for record in MergeIter::from_sources(sources.into_iter().map(MergeSource::Run).collect()) {
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
