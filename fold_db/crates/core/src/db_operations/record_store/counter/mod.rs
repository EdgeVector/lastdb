//! The guarded row commit.
//!
//! - [`RecordStore::commit_guarded_deletes`](super::RecordStore::commit_guarded_deletes):
//!   one native batch of guarded deletes. The retirement of a time queue
//!   bucket writes through it.

mod commit;
mod guarded;

pub(crate) use commit::GuardedDelete;
