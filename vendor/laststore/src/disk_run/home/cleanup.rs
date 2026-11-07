//! Explicit base compaction below the automatic merge thresholds.
//!
//! This operation drains already deleted or superseded records. The node
//! still decides record deletion. Normal background merge policy is unchanged.

use super::{RunHome, RunMerge, MAX_MERGE_INPUTS};
use crate::space::SpacePolicy;
use crate::Result;

impl RunHome {
    /// Run one existing base-merge step without the automatic size threshold.
    ///
    /// Repeated calls drain tiered runs when writes stop. Concurrent commits
    /// retain their normal ordering and can add later work. CURRENT already
    /// records an unfinished base merge, so reopening can continue that merge.
    /// The caller must preserve any request to drain further tiered runs.
    /// Each call uses the existing base-piece and merge-input limits; those
    /// limits bound the work shape, not a wall-clock deadline.
    ///
    /// `None` means no tiered run or base merge remains at this instant. It
    /// does not certify file removal: held reads, backup hardlinks and failed
    /// unlinks can retain bytes independently. It also makes no cross-home
    /// completion claim for the node's main and CAS homes.
    pub fn cleanup_step(&self) -> Result<Option<RunMerge>> {
        self.cleanup_step_with_space(&SpacePolicy::default())
    }

    /// Explicit cleanup uses the same replacement admission as automatic merges.
    pub fn cleanup_step_with_space(&self, policy: &SpacePolicy) -> Result<Option<RunMerge>> {
        self.cleanup_step_inner(policy, false)
    }

    /// Whether the current files contain no known or uncounted hidden versions.
    /// A caller freezing several homes must exclude concurrent logical writes.
    pub fn snapshot_has_clean_runs(&self) -> bool {
        clean_runs(&self.held())
    }

    /// Remove hidden versions below the ordinary merge thresholds before a
    /// new cloud snapshot. Append-only runs need no base merge.
    pub fn snapshot_cleanup_step_with_space(
        &self,
        policy: &SpacePolicy,
    ) -> Result<Option<RunMerge>> {
        self.cleanup_step_inner(policy, true)
    }

    fn cleanup_step_inner(&self, policy: &SpacePolicy, snapshot: bool) -> Result<Option<RunMerge>> {
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
        if snapshot {
            self.count_hidden(&held)?;
            if clean_runs(&held) {
                return Ok(None);
            }
        }
        let start = if held.merging.is_empty() {
            let tiered: Vec<_> = held.tiered().cloned().collect();
            if tiered.is_empty() {
                return Ok(None);
            }
            // The existing base merge requires the oldest inputs. Every
            // remaining tiered run is newer and stays above the new base.
            let first = tiered.len().saturating_sub(MAX_MERGE_INPUTS);
            Some(tiered[first..].to_vec())
        } else {
            None
        };
        self.base_step(start, policy).map(Some)
    }
}

fn clean_runs(held: &super::manifest::Manifest) -> bool {
    held.merging.is_empty() && held.tiered().all(|run| run.hidden() == Some(0))
}
