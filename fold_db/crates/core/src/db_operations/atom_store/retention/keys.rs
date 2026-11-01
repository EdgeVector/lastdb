//! Key layouts of the retention queue.
//!
//! - Queue: a time-bucketed queue (`time_queue`). An entry is
//!   `crq:v1:{bucket:020}\0{expires_at_nanos:020}:{record_id}`: the hash is one
//!   time slice of the expiry, and the range is the expiry and the id. The
//!   bucket directory is `crb:v1\0{bucket:020}`, one row for each bucket.
//! - Slot directory: `{scope:}crs:v1:...`, written by format 6 erases only. The
//!   drain of a marker reads it.
//!
//! `record_id` is the SHA-256 of the full storage key of the history row. The
//! queue is not scoped: its entries name records of every storage scope, and the
//! entry value names the history row, which carries the scope.

use sha2::{Digest, Sha256};

/// The family of the queue entries.
pub(super) const QUEUE_ENTRIES: &str = "crq:v1";
/// The family of the bucket directory of the queue.
pub(super) const QUEUE_DIRECTORY: &str = "crb:v1";

/// The layout of the queue for entries that live `window_nanos`.
pub(crate) fn queue_for_window(
    window_nanos: u64,
) -> crate::db_operations::atom_store::time_queue::TimeQueue {
    crate::db_operations::atom_store::time_queue::TimeQueue::for_window(
        QUEUE_ENTRIES,
        QUEUE_DIRECTORY,
        window_nanos,
    )
}

/// The identity of one record: the SHA-256 of its history key, in hex.
pub(super) fn record_id(history_key: &str) -> String {
    use std::fmt::Write;
    Sha256::digest(history_key.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        },
    )
}

/// The first 128 bits of the id of a record, as the queue keys name it. The
/// verifier and the recount compare it with `TimeQueue::entry_digest`.
pub(crate) fn record_digest(history_key: &str) -> Option<u128> {
    crate::db_operations::atom_store::time_queue::digest_of_id(&record_id(history_key))
}

/// The half-open range of every key that begins with `prefix`.
pub(super) fn prefix_range(prefix: &str) -> (String, String) {
    lastdb_model::kind_partition::exact_prefix_bounds(prefix)
}
