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
        self.refuse_if_poisoned()?;
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        let held = self.held();
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
