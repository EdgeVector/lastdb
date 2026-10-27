//! Local mutation admission after legacy cloud capture retirement.
//! No log staging, cloud worker, provider transfer or scheduler is present.

mod write_path;

pub(crate) use write_path::{
    current_mutation_admission, with_existing_mutation_admission, with_mutation_admission,
    MutationAdmission, MutationLogCaptureRouter,
};
