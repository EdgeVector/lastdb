//! A time-bucketed queue: entries that become due at a time, and a reader that
//! never has to pass the deleted keys of a long history.
//!
//! The contract, in short (the full text is `docs/lastdb-time-queue.md`):
//!
//! - **Layout.** An entry lives in the bucket of its expiry: the hash is one
//!   slice of time, and the range is the expiry and the id. A small directory
//!   lists every bucket that holds, or held, an entry. See `keys.rs`.
//! - **Writers.** A writer adds an entry and the directory row of its bucket in
//!   one batch, through the planner (`TimeQueue::rows`). The directory row gets
//!   a new token with each entry. No writer writes a queue key on its own.
//! - **Release.** An entry ends with a delete in the batch of the work that it
//!   names. A deleted entry leaves a deleted key in its bucket.
//! - **Readers.** A reader lists the directory, then walks the buckets that
//!   have started, oldest first. Each walk is a bounded range walk with its own
//!   cursor, kept in memory (`reader.rs`). The cursor moves over deleted keys,
//!   and a read that meets the read budget cuts its key range in two. A reader
//!   never restarts at the head of a hash that holds many deleted keys.
//! - **Retirement.** A bucket whose walk has read every key, with no entry
//!   left that waits for other work, leaves the directory. The directory row is
//!   deleted in the write gate, and only if its token is the one that the reader
//!   read before the walk (`retire.rs`). The deleted keys of the bucket die with
//!   it: nothing reads that hash again.
//! - **Cost of a count.** The size of a queue is counted by the directory and
//!   the buckets with bounded reads, and it does not fail on deleted keys
//!   (`stats.rs`).
//!
//! The conflict record retention queue (`retention`) is the first user. Another
//! queue with the same needs (the blob queue) takes the same layout, the same
//! reader and the same retirement, and adds its own entry value and release.

mod keys;
mod reader;
mod retire;
mod stats;

pub(crate) use keys::TimeQueue;
pub(crate) use reader::QueueReader;
