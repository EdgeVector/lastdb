//! One merge step, independent of foreground native commits.

use super::commit::RunMerge;
use super::plan::{plan, Plan};
use super::RunHome;
use crate::space::SpacePolicy;
use crate::Result;

impl RunHome {
    /// Run a tiered or base merge step, first counting new hidden records.
    /// Commits continue until publication. Older readers retain input runs.
    pub fn merge_step(&self) -> Result<Option<RunMerge>> {
        self.merge_step_with_space(&SpacePolicy::default())
    }

    /// Merge with this home's disk cap and the shared filesystem write quota.
    /// A capacity refusal leaves the previous CURRENT authoritative.
    pub fn merge_step_with_space(&self, policy: &SpacePolicy) -> Result<Option<RunMerge>> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        // One state for the count and plan: a run added after this capture
        // waits for the next step, so the plan sees counted runs.
        let held = self.held();
        self.count_hidden(&held)?;
        let Some(plan) = plan(&held) else {
            return Ok(None);
        };
        match plan {
            Plan::Tiered { level, inputs } => self.merge_tiered(level, inputs, policy),
            Plan::Base(start) => self.base_step(start, policy),
        }
        .map(Some)
    }
}
