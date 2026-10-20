//! Key layouts of the retention queue and the slot directory.
//!
//! - Queue: a time-bucketed queue (`time_queue`). An entry is
//!   `crq:v1:{bucket:020}\0{expires_at_nanos:020}:{record_id}`: the hash is one
//!   time slice of the expiry, and the range is the expiry and the id. The
//!   bucket directory is `crb:v1\0{bucket:020}`, one row for each bucket.
//! - Slot directory: `{scope:}crs:v1:{M}\0{esc(hash)}\0{range}\0{record_id}`. One
//!   hash for each scope and molecule. The part after the first NUL is the
//!   slot suffix of the `mk:` key of the slot, then the record.
//!
//! `record_id` is the SHA-256 of the full storage key of the history row. The
//! queue is not scoped: its entries name records of every storage scope, and the
//! entry value names the history row, which carries the scope.

use crate::atom::molecule_key_codec::{hash_range_record_key, molecule_record_prefix};
use crate::schema::types::field::build_storage_key;
use sha2::{Digest, Sha256};

/// The family of the queue entries.
pub(super) const QUEUE_ENTRIES: &str = "crq:v1";
/// The family of the bucket directory of the queue.
pub(super) const QUEUE_DIRECTORY: &str = "crb:v1";
const DIR_FAMILY: &str = "crs:v1:";

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

/// The identity of the drain marker of one slot.
pub(super) fn drain_id(dir_prefix: &str) -> String {
    record_id(&format!("drain:{dir_prefix}"))
}

/// The slot suffix of the `mk:` key: `{esc(hash)}\0{range}`.
fn slot_suffix(molecule: &str, hash: &str, range: &str) -> String {
    let record = hash_range_record_key(molecule, hash, range);
    record[molecule_record_prefix(molecule).len()..].to_string()
}

/// The prefix of the directory entries of one slot.
pub(super) fn slot_dir_prefix(
    scope: Option<&str>,
    molecule: &str,
    hash: &str,
    range: &str,
) -> String {
    let suffix = slot_suffix(molecule, hash, range);
    build_storage_key(scope, &format!("{DIR_FAMILY}{molecule}\0{suffix}\0"))
}

pub(super) fn dir_key(
    scope: Option<&str>,
    molecule: &str,
    hash: &str,
    range: &str,
    record_id: &str,
) -> String {
    format!(
        "{}{record_id}",
        slot_dir_prefix(scope, molecule, hash, range)
    )
}

/// The full storage key of the `mk:` record of one slot.
pub(super) fn slot_key(scope: Option<&str>, molecule: &str, hash: &str, range: &str) -> String {
    build_storage_key(scope, &hash_range_record_key(molecule, hash, range))
}

/// The half-open range of every key that begins with `prefix`.
pub(super) fn prefix_range(prefix: &str) -> (String, String) {
    crate::kind_partition::exact_prefix_bounds(prefix)
}
