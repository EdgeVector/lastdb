//! Ordered merge of several sources, newest first.

use super::range::RunRange;
use super::reader::RunReader;
use super::writer::RunWriter;
use super::Found;
use crate::Result;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;

/// One input of a merge: one run, or a sequence of runs in key order whose
/// key ranges do not overlap.
pub enum MergeSource<'a> {
    /// One run inside its bounds.
    Run(RunRange<'a>),
    /// Runs in key order; one run is open at a time.
    Sequence(RunSequence),
}

impl Iterator for MergeSource<'_> {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Run(range) => range.next(),
            Self::Sequence(sequence) => sequence.next(),
        }
    }
}

/// Records of runs in key order whose key ranges do not overlap, inside
/// key bounds. It opens the next run only when the one before it ends, so a
/// long sequence holds one open file.
pub struct RunSequence {
    paths: std::vec::IntoIter<PathBuf>,
    key: [u8; 32],
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    current: Option<RunRange<'static>>,
    failed: bool,
}

impl RunSequence {
    /// The records of the runs at `paths`, in key order, inside the bounds.
    pub fn new(
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

fn borrowed(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(key) => Bound::Included(key),
        Bound::Excluded(key) => Bound::Excluded(key),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl Iterator for RunSequence {
    type Item = Result<(Vec<u8>, Found)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.failed {
            if let Some(range) = self.current.as_mut() {
                match range.next() {
                    Some(record) => return Some(record),
                    None => self.current = None,
                }
            }
            let path = self.paths.next()?;
            match RunReader::open(&path, &self.key) {
                Ok(reader) => {
                    let range =
                        Arc::new(reader).shared_range(borrowed(&self.lower), borrowed(&self.upper));
                    self.current = Some(range);
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

/// Records of several sources in key order. When sources hold the same
/// key, the record from the earliest source wins, so pass the newest first.
pub struct MergeIter<'a> {
    sources: Vec<MergeSource<'a>>,
    heads: Vec<Option<(Vec<u8>, Found)>>,
    started: bool,
    done: bool,
}

impl<'a> MergeIter<'a> {
    /// Merges `sources`, newest first.
    pub fn new(sources: Vec<RunRange<'a>>) -> Self {
        Self::from_sources(sources.into_iter().map(MergeSource::Run).collect())
    }

    /// Merges runs and run sequences, newest first.
    pub fn from_sources(sources: Vec<MergeSource<'a>>) -> Self {
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
        if !self.started {
            self.started = true;
            for source in 0..self.sources.len() {
                self.advance(source)?;
            }
        }
        let mut winner: Option<usize> = None;
        for (source, head) in self.heads.iter().enumerate() {
            let Some((key, _)) = head else { continue };
            let smaller = match winner.and_then(|w| self.heads[w].as_ref()) {
                Some((best, _)) => key < best,
                None => true,
            };
            if smaller {
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

/// Writes the merge of `sources` (newest first) into `writer`. A merge into
/// the base passes `keep_tombstones = false`: no older run is left for a
/// tombstone to hide. Returns the records written.
pub fn merge_runs(
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
