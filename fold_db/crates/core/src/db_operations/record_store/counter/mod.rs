//! The guarded row commit and the restore check.
//!
//! - [`RecordStore::commit_guarded_deletes`](super::RecordStore::commit_guarded_deletes):
//!   one native batch of guarded deletes. The retirement of a time queue
//!   bucket writes through it.
//! - `restore_validation`: the read-only check of a restored home.

mod commit;
mod guarded;
pub(crate) mod restore_validation;

pub(crate) use commit::GuardedDelete;
