//! Key layouts of the retention queue and the slot directory.
//!
//! - Queue: `crq:v1\0{expires_at_nanos:020}:{record_id}`. The first NUL ends
//!   the hash, so the whole queue is one hash, and the range is the expiry.
//!   A range read of the queue returns the oldest expiry first.
//! - Directory: `{scope:}crs:v1:{M}\0{esc(hash)}\0{range}\0{record_id}`. One
//!   hash for each scope and molecule. The part after the first NUL is the
//!   slot suffix of the `mk:` key of the slot, then the record.
//!
//! `record_id` is the SHA-256 of the full storage key of the history row. The
//! queue is not scoped: its entries name records of every storage scope, and the
//! entry value names the history row, which carries the scope.

use crate::atom::molecule_key_codec::{hash_range_record_key, molecule_record_prefix};
use crate::schema::types::field::build_storage_key;
use sha2::{Digest, Sha256};

const QUEUE_HASH: &str = "crq:v1\0";
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

pub(super) fn queue_key(expires_at_nanos: u64, record_id: &str) -> String {
    format!("{QUEUE_HASH}{expires_at_nanos:020}:{record_id}")
}

/// The expiry in the key of a queue entry.
pub(super) fn queue_expiry(queue_key: &str) -> Option<u64> {
    let (nanos, id) = queue_key.strip_prefix(QUEUE_HASH)?.split_once(':')?;
    if nanos.len() != 20 || id.is_empty() {
        return None;
    }
    nanos.parse().ok()
}

/// The half-open range of every queue entry that sorts after `after`, or of
/// the whole queue.
pub(super) fn queue_range(after: Option<&str>) -> (String, String) {
    let end = "crq:v1\u{1}".to_string();
    let start = match after {
        // The smallest key above `after`.
        Some(key) => format!("{key}\0"),
        None => QUEUE_HASH.to_string(),
    };
    (start, end)
}

/// The slot suffix of the `mk:` key: `{esc(hash)}\0{range}`.
fn slot_suffix(molecule: &str, hash: &str, range: &str) -> String {
    let record = hash_range_record_key(molecule, hash, range);
    record[molecule_record_prefix(molecule).len()..].to_string()
}

/// `{scope:}crs:v1:{M}\0`: every directory entry of one molecule.
pub(super) fn molecule_dir_prefix(scope: Option<&str>, molecule: &str) -> String {
    build_storage_key(scope, &format!("{DIR_FAMILY}{molecule}\0"))
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
