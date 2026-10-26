//! Local mutation admission after legacy cloud capture retirement.
//! No log staging, cloud worker, provider transfer or scheduler is present.

mod write_path;

pub(crate) use write_path::{
    capture_logical_commit_with_policy, capture_logical_commit_with_policy_and_author_clock,
    current_mutation_admission, mark_logical_commit_published, with_capture_suppressed,
    with_existing_mutation_admission, with_mutation_admission, MutationAdmission,
    MutationLogCaptureNamespacedStore, MutationLogCaptureRouter,
};
