//! Key layout of a time-bucketed queue.
//!
//! - Entry: `{entries}:{bucket:020}\0{expires:020}:{id}`. The first NUL ends the
//!   hash, so the hash is one time slice of the expiry, and the range is the
//!   expiry and then the id. A range read of one bucket returns the oldest
//!   expiry first.
//! - Directory row: `{directory}\0{bucket:020}`. One hash for the whole
//!   directory, one row for each bucket that holds, or held, an entry. The
//!   value is a token that changes with every entry written to the bucket.
//!
//! `bucket` is the start of the slice in nanoseconds, not an index, so a change
//! of the slice length cannot make an old bucket name a different slice.

use crate::kind_partition::exact_prefix_bounds;
use serde_json::{json, Value};

/// Digits of a nanosecond number in a key. `u64::MAX` has 20.
const WIDTH: usize = 20;
/// The shortest slice: one second.
pub(crate) const MIN_SLICE_NANOS: u64 = 1_000_000_000;
/// The longest slice: one hour.
pub(crate) const MAX_SLICE_NANOS: u64 = 3_600_000_000_000;
/// A queue holds about this many slices over a retention window, at most.
const SLICES_PER_WINDOW: u64 = 64;
/// The most slices a list of slices holds. A window holds about 64, so this
/// bounds only a list for a very short slice and a very long span.
const MAX_SLICES: usize = 4_096;

/// The layout of one time-bucketed queue. It holds names and a number, and no
/// state, so it is cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimeQueue {
    entries: &'static str,
    directory: &'static str,
    slice_nanos: u64,
}

impl TimeQueue {
    /// A queue with slices of `slice_nanos` (at least 1).
    pub(crate) const fn new(
        entries: &'static str,
        directory: &'static str,
        slice_nanos: u64,
    ) -> Self {
        Self {
            entries,
            directory,
            slice_nanos: if slice_nanos == 0 { 1 } else { slice_nanos },
        }
    }

    /// A queue for entries that live about `window_nanos`: a slice of a 64th of
    /// the window, between one second and one hour. A short window gets short
    /// slices, so a bucket ends soon after its entries do. A long window gets
    /// one-hour slices, so the directory stays small.
    pub(crate) fn for_window(
        entries: &'static str,
        directory: &'static str,
        window_nanos: u64,
    ) -> Self {
        let slice = (window_nanos / SLICES_PER_WINDOW).clamp(MIN_SLICE_NANOS, MAX_SLICE_NANOS);
        Self::new(entries, directory, slice)
    }

    /// The start of the slice that holds `expires`.
    pub(crate) fn bucket_of(&self, expires: u64) -> u64 {
        expires - expires % self.slice_nanos
    }

    /// The key of the entry that expires at `expires`.
    pub(crate) fn entry_key(&self, expires: u64, id: &str) -> String {
        format!(
            "{}:{:020}\0{expires:020}:{id}",
            self.entries,
            self.bucket_of(expires)
        )
    }

    /// The key of the directory row of `bucket`.
    pub(crate) fn directory_key(&self, bucket: u64) -> String {
        format!("{}\0{bucket:020}", self.directory)
    }

    /// The length of a slice in nanoseconds.
    pub(crate) fn slice_nanos(&self) -> u64 {
        self.slice_nanos
    }

    /// The starts of the slices from the one that holds `from` to the one that
    /// holds `to`, oldest first. At most [`MAX_SLICES`] of them.
    pub(crate) fn slices(&self, from: u64, to: u64) -> Vec<u64> {
        let mut starts = Vec::new();
        let mut bucket = self.bucket_of(from);
        let last = self.bucket_of(to);
        while bucket <= last && starts.len() < MAX_SLICES {
            starts.push(bucket);
            match bucket.checked_add(self.slice_nanos) {
                Some(next) => bucket = next,
                None => break,
            }
        }
        starts
    }

    /// The directory row of `bucket` with the token `token`.
    pub(crate) fn directory_row(&self, bucket: u64, token: &str) -> (String, Value) {
        (self.directory_key(bucket), json!({ "v": 1, "t": token }))
    }

    /// The two rows that one new entry writes in its batch: the entry, and the
    /// directory row of its bucket with a new token. The token names the entry,
    /// so two entries never leave the same row. A reader that retires a bucket
    /// deletes the row only if the token is the one it read before it walked
    /// the bucket (see `AtomStore::retire_bucket`).
    pub(crate) fn rows(&self, expires: u64, id: &str, value: &Value) -> Vec<(String, Value)> {
        let bucket = self.bucket_of(expires);
        vec![
            (self.entry_key(expires, id), value.clone()),
            self.directory_row(bucket, &format!("{expires:020}:{id}")),
        ]
    }

    /// The expiry and the bucket in the key of an entry.
    fn parse_entry(&self, key: &str) -> Option<(u64, u64)> {
        let rest = key.strip_prefix(self.entries)?.strip_prefix(':')?;
        let (bucket, tail) = rest.split_once('\0')?;
        let (expires, id) = tail.split_once(':')?;
        if bucket.len() != WIDTH || expires.len() != WIDTH || id.is_empty() {
            return None;
        }
        Some((bucket.parse().ok()?, expires.parse().ok()?))
    }

    /// The id in the key of an entry, as a 128-bit digest of its first 32 hex
    /// digits. The ids of a queue are hashes, so the digest names an entry.
    pub(crate) fn entry_digest(&self, key: &str) -> Option<u128> {
        self.parse_entry(key)?;
        digest_of_id(key.rsplit(':').next()?)
    }

    /// The expiry in the key of an entry.
    pub(crate) fn entry_expiry(&self, key: &str) -> Option<u64> {
        self.parse_entry(key).map(|(_, expires)| expires)
    }

    /// The bucket in the key of an entry.
    pub(crate) fn entry_bucket(&self, key: &str) -> Option<u64> {
        self.parse_entry(key).map(|(bucket, _)| bucket)
    }

    /// The half-open key range of every entry of one bucket: one hash.
    pub(crate) fn bucket_range(&self, bucket: u64) -> (String, String) {
        exact_prefix_bounds(&format!("{}:{bucket:020}\0", self.entries))
    }

    /// The half-open key range of the directory rows of the buckets from `from`
    /// to `upto`, both included.
    pub(crate) fn directory_range(&self, from: u64, upto: u64) -> (String, String) {
        (
            self.directory_key(from),
            format!("{}\0", self.directory_key(upto)),
        )
    }

    /// The bucket in the key of a directory row.
    pub(crate) fn directory_bucket(&self, key: &str) -> Option<u64> {
        let digits = key.strip_prefix(self.directory)?.strip_prefix('\0')?;
        if digits.len() != WIDTH {
            return None;
        }
        digits.parse().ok()
    }
}

/// The first 128 bits of an id of hex digits (at least 32 digits), or `None`.
pub(crate) fn digest_of_id(id: &str) -> Option<u128> {
    u128::from_str_radix(id.get(..32)?, 16).ok()
}
