//! Ordered merge of several runs, newest first.

use super::range::RunRange;
use super::writer::RunWriter;
use super::Found;
use crate::Result;

/// Records of several runs in key order. When runs hold the same key, the
/// record from the earliest source wins, so pass the newest run first.
pub struct MergeIter<'a> {
    sources: Vec<RunRange<'a>>,
    heads: Vec<Option<(Vec<u8>, Found)>>,
    started: bool,
    done: bool,
}

impl<'a> MergeIter<'a> {
    /// Merges `sources`, newest first.
    pub fn new(sources: Vec<RunRange<'a>>) -> Self {
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
/// the oldest level passes `keep_tombstones = false`: no older run is left
/// for a tombstone to hide. Returns the records written.
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
