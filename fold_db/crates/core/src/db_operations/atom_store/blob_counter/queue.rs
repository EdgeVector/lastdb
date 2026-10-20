//! The dwell queue of the blob counters: a time-bucketed queue.
//!
//! A blob at zero has one queue entry. The entry is in the bucket of its
//! `zero_since`, one slice of time, and the dwell pass releases the entries whose
//! dwell is over, oldest first. The layout, the reader and the retirement are
//! the ones of `time_queue` (`docs/lastdb-time-queue.md`). This module gives the
//! queue its names, its slice and its entry value, and it turns a `zero_since`
//! in milliseconds into the time of the queue in nanoseconds.
//!
//! | Row | Key | Value |
//! |---|---|---|
//! | Entry | `bref:v3:q:{bucket:020}\0{zero_since_ns:020}:{sha256(blob_ref)}` | `{"blob":"sha256:.."}` |
//! | Directory row | `bref:v3:d\0{bucket:020}` | a token |
//!
//! The time of an entry is the `zero_since` of its counter, not its due time. The
//! dwell is a property of the reader: it asks for the entries older than
//! `now - dwell`. A change of `LASTDB_BLOB_DWELL_SECS` between two starts then
//! needs no rewrite of the queue, and the key of an entry never depends on a
//! knob (the cancel of a dwell can always name the key from the counter).
//!
//! The key families are `bref:v3:q` and `bref:v3:d`. Neither holds a NUL, and
//! neither is a prefix of the other or of the counter family `bref:v3:c:`.

use super::blob_id;
use crate::db_operations::atom_store::time_queue::TimeQueue;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde_json::json;

/// Key family of the entries.
const ENTRIES: &str = "bref:v3:q";
/// Key family of the bucket directory.
const DIRECTORY: &str = "bref:v3:d";
/// Nanoseconds in a millisecond.
const NANOS_PER_MILLI: u64 = 1_000_000;

/// The layout of the blob queue. A value holds names and a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlobQueue {
    queue: TimeQueue,
}

impl BlobQueue {
    /// The queue for a dwell of `dwell_ms`: slices of a 64th of the dwell, between
    /// one second and one hour (`TimeQueue::for_window`).
    pub(crate) fn for_dwell(dwell_ms: u64) -> Self {
        Self {
            queue: TimeQueue::for_window(
                ENTRIES,
                DIRECTORY,
                dwell_ms.saturating_mul(NANOS_PER_MILLI),
            ),
        }
    }

    /// The layout, for the reader and the size count.
    pub(crate) fn layout(&self) -> TimeQueue {
        self.queue
    }

    /// The time of the queue, in nanoseconds, for a `zero_since` in milliseconds.
    pub(crate) fn nanos(zero_since_ms: u64) -> u64 {
        zero_since_ms.saturating_mul(NANOS_PER_MILLI)
    }

    /// The key of the entry of `blob_ref` at `zero_since_ms`.
    pub(crate) fn entry_key(&self, zero_since_ms: u64, blob_ref: &str) -> String {
        self.entry_key_of_id(zero_since_ms, &blob_id(blob_ref))
    }

    /// The key of the entry of the blob with identity `id`.
    pub(crate) fn entry_key_of_id(&self, zero_since_ms: u64, id: &str) -> String {
        self.queue.entry_key(Self::nanos(zero_since_ms), id)
    }

    /// The two rows that the batch of a new entry writes: the entry, and the
    /// directory row of its bucket with a new token.
    pub(crate) fn put_mutations(
        &self,
        zero_since_ms: u64,
        blob_ref: &str,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let id = blob_id(blob_ref);
        let rows = self.queue.rows(
            Self::nanos(zero_since_ms),
            &id,
            &json!({ "blob": blob_ref }),
        );
        rows.into_iter()
            .map(|(key, value)| {
                serde_json::to_vec(&value)
                    .map(|bytes| KvMutation::put(key.into_bytes(), bytes))
                    .map_err(|error| {
                        SchemaError::InvalidData(format!("serialize blob queue row: {error}"))
                    })
            })
            .collect()
    }

    /// `(zero_since_ms, id)` of an entry key. `None` for a key of another shape.
    pub(crate) fn parse_entry(&self, key: &[u8]) -> Option<(u64, String)> {
        let key = std::str::from_utf8(key).ok()?;
        let nanos = self.queue.entry_expiry(key)?;
        let id = key.rsplit_once(':')?.1;
        super::is_id(id).then(|| (nanos / NANOS_PER_MILLI, id.to_string()))
    }

    /// The key range of every entry of every bucket. An admin walk uses it with
    /// a bounded range walk. A reader of the queue never does: it lists the
    /// directory and walks one bucket at a time.
    pub(crate) fn entries_range() -> (Vec<u8>, Vec<u8>) {
        super::prefix_range(&format!("{ENTRIES}:"))
    }

    /// One batch writes the directory row of a bucket once. Several blobs of one
    /// planner run can reach zero in one slice of time, and each of them brings
    /// the row with its own token. The last row of a key stays, as in the planner
    /// (`counter/records.rs`).
    pub(crate) fn keep_last_directory_rows(mutations: Vec<KvMutation>) -> Vec<KvMutation> {
        let prefix = format!("{DIRECTORY}\0").into_bytes();
        let is_row = |mutation: &KvMutation| matches!(mutation, KvMutation::Put { key, .. } if key.starts_with(&prefix));
        let mut last: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
        for (index, mutation) in mutations.iter().enumerate() {
            if is_row(mutation) {
                last.insert(mutation.key().to_vec(), index);
            }
        }
        mutations
            .into_iter()
            .enumerate()
            .filter(|(index, mutation)| !is_row(mutation) || last[mutation.key()] == *index)
            .map(|(_, mutation)| mutation)
            .collect()
    }

    /// True for the key of an entry or of a directory row.
    pub(crate) fn is_queue_key(key: &str) -> bool {
        if let Some(rest) = key.strip_prefix(&format!("{DIRECTORY}\0")) {
            return digits(rest);
        }
        let Some(rest) = key.strip_prefix(&format!("{ENTRIES}:")) else {
            return false;
        };
        let Some((bucket, tail)) = rest.split_once('\0') else {
            return false;
        };
        let Some((at, id)) = tail.split_once(':') else {
            return false;
        };
        digits(bucket) && digits(at) && super::is_id(id)
    }
}

fn digits(text: &str) -> bool {
    text.len() == 20 && text.bytes().all(|byte| byte.is_ascii_digit())
}

impl Default for BlobQueue {
    fn default() -> Self {
        Self::for_dwell(super::blob_dwell_millis())
    }
}
