//! Merge step dispatch and base-merge output piece writer.

use super::commit::meta;
use super::manifest::RunFile;
use super::plan::{plan, Plan};
use super::RunHome;
use crate::disk_run::space::MergeSpace;
use crate::disk_run::{Found, MergeIter, MergeSource, RunReader, RunSequence, RunWriter};
use crate::space::SpacePolicy;
use crate::Result;
use std::ops::Bound;
use std::sync::Arc;

impl RunHome {
    /// Run a tiered or base merge step, first counting new hidden records.
    pub fn merge_step(&self) -> Result<bool> {
        self.merge_step_with_space(&SpacePolicy::default())
    }

    /// Merge with this home's disk cap and the shared filesystem write quota.
    pub fn merge_step_with_space(&self, policy: &SpacePolicy) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
        self.count_hidden(&held)?;
        let Some(plan) = plan(&held) else {
            return Ok(false);
        };
        match plan {
            Plan::Tiered { level, inputs } => self.merge_tiered(level, inputs, policy),
            Plan::Base(start) => self.base_step(start, policy),
        }
        .map(|()| true)
    }

    pub(in crate::disk_run::home) fn write_base_pieces(
        &self,
        merging: &[Arc<RunFile>],
        taken: &[Arc<RunFile>],
        cut: Option<&[u8]>,
        upper: Option<&[u8]>,
        target: u64,
        space: &MergeSpace,
    ) -> Result<Option<Vec<Arc<RunFile>>>> {
        if !merging
            .iter()
            .any(|run| run.meta.meets_merge_interval(cut, upper))
        {
            return Ok(None);
        }
        let lower = cut.map_or(Bound::Unbounded, Bound::Excluded);
        let upper = upper.map_or(Bound::Unbounded, Bound::Included);
        let mut sources = Vec::with_capacity(merging.len() + 1);
        for run in merging {
            let reader = Arc::new(RunReader::open(&run.path, &self.key)?);
            sources.push(MergeSource::Run(reader.shared_range(lower, upper)));
        }
        let paths = taken.iter().map(|run| run.path.clone()).collect();
        let sequence = RunSequence::new(paths, &self.key, Bound::Unbounded, Bound::Unbounded);
        sources.push(MergeSource::Sequence(sequence));
        let mut pieces = Vec::new();
        if let Err(error) =
            self.write_pieces(MergeIter::from_sources(sources), target, &mut pieces, space)
        {
            for piece in &pieces {
                piece.retire();
            }
            return Err(error);
        }
        Ok(Some(pieces))
    }

    fn write_pieces(
        &self,
        records: MergeIter<'_>,
        target: u64,
        pieces: &mut Vec<Arc<RunFile>>,
        space: &MergeSpace,
    ) -> Result<()> {
        let mut writer: Option<RunWriter> = None;
        for record in records {
            let (key, found) = record?;
            let Found::Value(value) = found else {
                continue;
            };
            if writer.is_none() {
                writer = Some(RunWriter::create_reserved(
                    &self.runs,
                    &self.key,
                    self.options,
                    space,
                )?);
            }
            let open = writer.as_mut().expect("writer is open");
            open.push(&key, Some(&value))?;
            if open.bytes_written() >= target {
                let finished = writer.take().expect("writer is open").finish()?;
                pieces.push(RunFile::new(&self.runs, meta(finished)?));
            }
        }
        if let Some(open) = writer {
            pieces.push(RunFile::new(&self.runs, meta(open.finish()?)?));
        }
        Ok(())
    }
}
