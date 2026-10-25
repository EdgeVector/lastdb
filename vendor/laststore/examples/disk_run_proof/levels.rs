//! Levels of runs for the incremental part of the proof.

use crate::{dir_bytes, Expected, Result};
use laststore::disk_run::{merge_runs, RunOptions, RunReader, RunWriter};
use std::fs;
use std::path::{Path, PathBuf};

const MERGE_FANIN: usize = 8;

#[derive(Default)]
pub(crate) struct Levels {
    /// `levels[n]` lists the runs of level `n`, oldest first.
    levels: Vec<Vec<PathBuf>>,
    pub(crate) written: u64,
    pub(crate) peak: u64,
}

impl Levels {
    pub(crate) fn add(&mut self, dir: &Path, key: &[u8; 32], batch: &Expected) -> Result<()> {
        let mut writer = RunWriter::create(dir, key, RunOptions::default())?;
        for (k, v) in batch {
            writer.push(k, v.as_deref())?;
        }
        let summary = writer.finish()?;
        self.written += summary.file_bytes;
        self.push(dir, key, 0, summary.path)
    }

    fn push(&mut self, dir: &Path, key: &[u8; 32], level: usize, run: PathBuf) -> Result<()> {
        if self.levels.len() == level {
            self.levels.push(Vec::new());
        }
        self.levels[level].push(run);
        self.peak = self.peak.max(dir_bytes(dir)?);
        if self.levels[level].len() < MERGE_FANIN {
            return Ok(());
        }
        let inputs = std::mem::take(&mut self.levels[level]);
        let merged = self.merge(dir, key, &inputs, true)?;
        self.push(dir, key, level + 1, merged)
    }

    /// Writes the merge of `inputs` (oldest first), measures the peak while
    /// both the inputs and the output are on disk, then deletes the inputs.
    fn merge(
        &mut self,
        dir: &Path,
        key: &[u8; 32],
        inputs: &[PathBuf],
        keep_tombstones: bool,
    ) -> Result<PathBuf> {
        let readers: Vec<RunReader> = inputs
            .iter()
            .rev()
            .map(|p| RunReader::open(p, key))
            .collect::<laststore::Result<_>>()?;
        let mut writer = RunWriter::create(dir, key, RunOptions::default())?;
        merge_runs(
            readers.iter().map(RunReader::iter).collect(),
            &mut writer,
            keep_tombstones,
        )?;
        let summary = writer.finish()?;
        self.written += summary.file_bytes;
        self.peak = self.peak.max(dir_bytes(dir)?);
        drop(readers);
        for input in inputs {
            fs::remove_file(input)?;
        }
        Ok(summary.path)
    }

    pub(crate) fn newest_first(&self) -> Vec<PathBuf> {
        self.levels
            .iter()
            .flat_map(|level| level.iter().rev().cloned())
            .collect()
    }

    pub(crate) fn merge_all(&mut self, dir: &Path, key: &[u8; 32]) -> Result<()> {
        let mut oldest_first = self.newest_first();
        oldest_first.reverse();
        let merged = self.merge(dir, key, &oldest_first, false)?;
        self.levels = vec![vec![merged]];
        Ok(())
    }
}
