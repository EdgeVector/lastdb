//! Local mutation admission and automatic compaction after legacy cloud capture retirement.
//! No log staging, cloud worker, provider transfer or scheduler is present.

pub(crate) mod plane_compactor;
pub(crate) mod worker;
mod write_path;

pub(crate) use plane_compactor::PlaneCompactor;
pub(crate) use worker::{
    atoms_compact_max_bytes, atoms_compact_min_overhang_bps, atoms_compact_min_overhang_bytes,
    capture_reexport_compact_max_plane_bytes, capture_reexport_probe_interval_s,
    fill_overhang_status, large_captured_plane_probe_interval_s,
    photograph_aligned_compact_budget_secs, photograph_aligned_compact_interval_s,
    tips_compact_max_bytes, tips_compact_min_overhang_bps, tips_compact_min_overhang_bytes,
    tips_compact_probe_interval_s, ResidualPlaneTrigger, CAPTURE_REEXPORT_NAMESPACE,
    RESIDUAL_SELF_COMPACT_PLANES,
};
pub(crate) use write_path::{
    capture_logical_commit_with_policy, capture_logical_commit_with_policy_and_author_clock,
    current_mutation_admission, mark_logical_commit_published, with_capture_suppressed,
    with_existing_mutation_admission, with_mutation_admission, MutationAdmission,
    MutationLogCaptureNamespacedStore, MutationLogCaptureRouter,
};
