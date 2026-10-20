//! The blob counter: the only authority for whether a local file blob may go.
//!
//! A blob is a row of the `cas_blobs` plane, keyed by its `blob_ref`. Each blob
//! has one counter record in the main store. The counter holds the number of
//! references to the blob: one for each atom counter that lists the blob in its
//! `blobs`, and one for each live tip record whose metadata names the blob. The
//! planner of the atom counter changes the blob counters in the same batch as
//! the atom counters (`counter::build`), so a count and the references it counts
//! change together.
//!
//! An upload comes before the atom that names it. So a blob whose count is zero
//! is not deleted at once. It has a dwell (`LASTDB_BLOB_DWELL_SECS`, one hour by
//! default, never below 120 s unless a dev knob says so) that starts when its
//! count reaches zero or when it is uploaded. A new reference during the dwell
//! cancels it. The dwell must be longer than the delay between the ack of a
//! `queued` write and the batch of that write (the batch raises the counters):
//! see [`BLOB_DWELL_FLOOR_SECS`]. A dwell pass reads the time ordered queue of
//! blobs at zero, and for each blob whose dwell has passed it reads the counter
//! again inside the write gate and deletes the blob row, the counter and the
//! queue entry in one batch.
//!
//! Records, all in the `atoms` collection:
//!
//! | Record | Key |
//! |---|---|
//! | Counter | `bref:v3:c:{sha256(blob_ref)}\0c` |
//! | Queue entry | `bref:v3:q\0{zero_since_ms:020}\0{sha256(blob_ref)}` |
//!
//! A key hashes on its first NUL. The counter of a blob is one hash, as the
//! counter of an atom is. The queue key hashes with the tag only, so the whole
//! queue is one hash and a page of the oldest entries is one range read. Only
//! the modules here write these records. `scripts/lints/lint-atom-counter-writers.py` guards that.

mod clock;
mod commit;
mod deltas;
mod dwell;
mod plan;
mod recount;
mod report;

pub(crate) use deltas::{blobs_of_tip_row, BlobDeltas};
pub use dwell::{DwellChunk, DwellEntry, DwellScan, DWELL_CHUNK};
pub use recount::BlobRecountReport;
pub(crate) use report::BlobEvents;
pub use report::{BlobCounterReport, BlobCounterView, DwellGuard};

use crate::hex::hex_lower;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Format version of a blob counter record.
pub const BLOB_COUNTER_VERSION: u8 = 1;

/// Environment knob: the dwell of a blob at zero, in seconds.
pub const BLOB_DWELL_SECS_ENV: &str = "LASTDB_BLOB_DWELL_SECS";

/// One hour. An upload and the atom that names it come close together.
pub const BLOB_DWELL_SECS_DEFAULT: u64 = 3600;

/// The shortest dwell that the node accepts, in seconds.
///
/// A `queued` write is acked from memory and its batch (which raises the blob
/// counters) runs later. A blob row that the dwell pass deletes before that batch
/// runs leaves an atom that names a blob that is gone. A request has at most 90 s
/// (the handler budget) and the persist lane that runs the batch has a bulk queue
/// budget of about 60 s, so the floor is the handler budget plus 30 s. It is a
/// floor, not a proof: a lane that stalls for longer than the floor can still lose
/// the race, and the one hour default is the real margin. A shorter dwell breaks
/// the contract. With a dwell of 3 s and a backlog of 400 queued writes, a queued
/// write was acked after 3.7 s and the pass had deleted the blob first.
pub const BLOB_DWELL_FLOOR_SECS: u64 = 120;

/// Dev knob: allow a dwell below [`BLOB_DWELL_FLOOR_SECS`]. For short proofs on a
/// synthetic home only. Never set it on a home that holds user data.
pub const BLOB_DWELL_UNSAFE_SHORT_ENV: &str = "LASTDB_BLOB_DWELL_UNSAFE_SHORT_FOR_DEV";

const COUNTER_PREFIX: &str = "bref:v3:c:";
const QUEUE_HASH: &str = "bref:v3:q";

/// One blob counter record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobCounter {
    #[serde(rename = "v")]
    pub version: u8,
    /// The blob reference. The key holds only its hash.
    pub blob: String,
    /// References: atom counters that list the blob plus live tips that name it.
    pub refs: u64,
    /// Unix milliseconds when the count reached zero, or when the blob was last
    /// uploaded at a count of zero. `None` while `refs > 0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero_since: Option<u64>,
    /// A removal found a count that disagrees with it. The blob stays, whatever
    /// the count says, until an operator repairs the counter.
    #[serde(default, skip_serializing_if = "is_false")]
    pub suspect: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl BlobCounter {
    pub(super) fn new(blob: &str, refs: u64, zero_since: Option<u64>) -> Self {
        Self {
            version: BLOB_COUNTER_VERSION,
            blob: blob.to_string(),
            refs,
            zero_since,
            suspect: false,
        }
    }
}

/// The dwell of a blob at zero, in milliseconds. A value below the floor is
/// refused: the floor applies, and the node logs it once.
#[must_use]
pub fn blob_dwell_millis() -> u64 {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let asked = env_flag::var_parsed::<u64>(BLOB_DWELL_SECS_ENV).unwrap_or(BLOB_DWELL_SECS_DEFAULT);
    let secs =
        if asked < BLOB_DWELL_FLOOR_SECS && !env_flag::var_truthy(BLOB_DWELL_UNSAFE_SHORT_ENV) {
            if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(
                    asked_secs = asked,
                    floor_secs = BLOB_DWELL_FLOOR_SECS,
                    "{BLOB_DWELL_SECS_ENV} is below the floor and is refused; the floor applies. \
                 {BLOB_DWELL_UNSAFE_SHORT_ENV}=1 allows a short dwell on a synthetic home"
                );
            }
            BLOB_DWELL_FLOOR_SECS
        } else {
            asked
        };
    secs.saturating_mul(1000)
}

/// What a read of one counter record found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobRead {
    Absent,
    /// The record exists and does not decode.
    Unreadable,
    Present(BlobCounter),
}

impl BlobRead {
    pub(crate) fn parse(bytes: Option<&[u8]>) -> Self {
        match bytes {
            None => Self::Absent,
            Some(bytes) => serde_json::from_slice(bytes).map_or(Self::Unreadable, Self::Present),
        }
    }

    /// The counter, when the record exists and decodes.
    pub(crate) fn counter(&self) -> Option<&BlobCounter> {
        match self {
            Self::Present(counter) => Some(counter),
            _ => None,
        }
    }
}

/// The identity of a blob in a counter key and a queue key.
#[must_use]
pub(crate) fn blob_id(blob_ref: &str) -> String {
    hex_lower(Sha256::digest(blob_ref.as_bytes()))
}

/// Counter key `bref:v3:c:{id}\0c`.
#[must_use]
pub(crate) fn blob_counter_key(blob_ref: &str) -> String {
    blob_counter_key_of_id(&blob_id(blob_ref))
}

/// Counter key of a blob identity.
#[must_use]
pub(crate) fn blob_counter_key_of_id(id: &str) -> String {
    format!("{COUNTER_PREFIX}{id}\0c")
}

/// Queue key `bref:v3:q\0{zero_since:020}\0{id}`.
#[must_use]
pub(crate) fn blob_queue_key(zero_since: u64, blob_ref: &str) -> String {
    format!("{QUEUE_HASH}\0{zero_since:020}\0{}", blob_id(blob_ref))
}

/// The prefix of the whole queue: one hash.
#[must_use]
pub(crate) fn blob_queue_prefix() -> String {
    format!("{QUEUE_HASH}\0")
}

/// The key range of every blob counter: one hash for each blob.
#[must_use]
pub(crate) fn counter_range() -> (Vec<u8>, Vec<u8>) {
    prefix_range(COUNTER_PREFIX)
}

/// The key range of the whole queue: one hash.
#[must_use]
pub(crate) fn queue_range() -> (Vec<u8>, Vec<u8>) {
    prefix_range(&blob_queue_prefix())
}

fn prefix_range(prefix: &str) -> (Vec<u8>, Vec<u8>) {
    let end = crate::schema::types::field::FilterUtils::create_prefix_end(prefix)
        .expect("a family prefix has a finite upper bound");
    (prefix.as_bytes().to_vec(), end.into_bytes())
}

/// `(zero_since, id)` of a queue key. `None` for a key of another shape.
#[must_use]
pub(crate) fn parse_blob_queue_key(key: &[u8]) -> Option<(u64, String)> {
    let rest = std::str::from_utf8(key)
        .ok()?
        .strip_prefix(blob_queue_prefix().as_str())?;
    let (time, id) = rest.split_once('\0')?;
    if time.len() != 20 || !is_id(id) {
        return None;
    }
    Some((time.parse().ok()?, id.to_string()))
}

/// True for a counter key or a queue key of the current layout. The key check of
/// every native write and every import calls this for the `bref` family.
#[must_use]
pub(crate) fn is_current_key(key: &str) -> bool {
    match key.strip_prefix(COUNTER_PREFIX) {
        Some(rest) => rest.strip_suffix("\0c").is_some_and(is_id),
        None => parse_blob_queue_key(key.as_bytes()).is_some(),
    }
}

fn is_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}
