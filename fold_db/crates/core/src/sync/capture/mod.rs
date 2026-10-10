//! Store-level change capture surface (legacy cold watermark path removed).
//!
//! Continuous product export is [`crate::sync::engine::CaptureMode::MutationLog`].
//! Sealed snapshot / backup / outbox heal still live on the capture worker
//! module as helpers that no longer touch a durable export-baseline watermark.
//!
//! **Tests:** do not reintroduce the retired store-diff cold capture variant.
//! Capture/sync coverage uses [`crate::sync::engine::CaptureMode::Off`]
//! (local-first + backup/outbox valves) or `MutationLog` (continuous pin-log).
//! Pure store-diff cold suites were deleted with the product path (fold PR #1261).
//!
//! Historical design notes: `docs/designs/store-level-log-based-cloud-sync.md`
//! (watermark cold path retired 2026-08).

pub(crate) mod pending;
pub(crate) mod plane_compactor;
pub(crate) mod worker;
mod write_path;

pub(crate) use plane_compactor::PlaneCompactor;
pub(crate) use worker::{
    atoms_compact_max_bytes, atoms_compact_min_overhang_bps, atoms_compact_min_overhang_bytes,
    capture_reexport_compact_max_plane_bytes, capture_reexport_probe_interval_s,
    fill_overhang_status, large_captured_plane_probe_interval_s, locator_compact_min_overhang_bps,
    locator_probe_interval_s, order_log_compact_max_bytes, order_log_compact_min_overhang_bps,
    order_log_compact_min_overhang_bytes, photograph_aligned_compact_budget_secs,
    photograph_aligned_compact_interval_s, tips_compact_max_bytes, tips_compact_min_overhang_bps,
    tips_compact_min_overhang_bytes, tips_compact_probe_interval_s, ResidualPlaneTrigger,
    CAPTURE_REEXPORT_NAMESPACE, RESIDUAL_SELF_COMPACT_PLANES,
};
pub use worker::{CaptureTickStats, HealStagingReport};
pub(crate) use write_path::{
    capture_logical_commit_with_policy, capture_logical_commit_with_policy_and_author_clock,
    current_mutation_admission, mark_logical_commit_published, with_capture_suppressed,
    with_existing_mutation_admission, with_mutation_admission, MutationAdmission,
    MutationLogCaptureNamespacedStore, MutationLogCaptureRouter,
};
