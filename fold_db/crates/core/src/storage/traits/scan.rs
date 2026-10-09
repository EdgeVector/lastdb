//! Capability hints, partitioned-scan results and the batch mutation enum.

use serde::{Deserialize, Serialize};

/// Describes how the backend executes operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionModel {
    /// Backend is truly async (network I/O, e.g., Exemem API).
    Async,
    /// Backend is sync but wrapped in async (local I/O, e.g., Sled).
    SyncWrapped,
}

/// Describes flush behavior for the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushBehavior {
    /// Flush is a no-op (eventually consistent backend, e.g., Exemem API).
    NoOp,
    /// Flush performs actual persistence (strongly consistent, e.g., Sled).
    Persists,
}

/// The result of [`KvStore::scan_prefix_partition_undecryptable`]: rows that
/// read (decrypt) cleanly, separated from the keys of rows whose at-rest value
/// could not be decrypted with any registered key ("poison" / undecryptable
/// rows).
///
/// Unlike [`KvStore::scan_prefix`] (which fails the whole scan on the first
/// undecryptable row) and [`KvStore::scan_prefix_lossy`] (which silently drops
/// them), this keeps the undecryptable keys so the caller can *report* them
/// loudly — a snapshot backup that must not abort on a handful of unrecoverable
/// poison rows — or *scrub* them (an explicit maintenance delete).
#[derive(Debug, Default, Clone)]
pub struct PartitionedScan {
    /// Rows that decrypted cleanly: `(key, plaintext value)`.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Keys of rows that could not be decrypted. Their values are withheld
    /// (they are unreadable). Non-encrypting backends never populate this.
    pub undecryptable: Vec<Vec<u8>>,
}

/// Typed-layer counterpart to [`PartitionedScan`]: rows that deserialized
/// cleanly, plus the keys that did not and why.
///
/// Returned by
/// [`TypedKvStore::scan_items_with_prefix_partition_undecodable`]. `undecodable`
/// carries `(key, serde error)` so a caller can log exactly which record is
/// poisoned — an operator grepping for one bad key should not have to guess.
#[derive(Debug)]
pub struct PartitionedItems<T> {
    /// Items that deserialized cleanly: `(key, value)`.
    pub items: Vec<(String, T)>,
    /// `(key, error)` for rows present in storage but not decodable as `T`.
    pub undecodable: Vec<(String, String)>,
}

/// One ordered mutation in a mixed durable batch.
///
/// Backends that implement [`KvStore::batch_mutate`] must preserve this order.
/// LastStore applies the operations in order, then runs one durability barrier.
/// If an operation fails, LastStore restores the applied prefix in reverse
/// order and flushes that restore before it releases the point-key locks.
/// This is in-process rollback, not a cross-group write-ahead log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvMutation {
    /// Insert or replace one raw key/value row.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Remove one raw key if it exists.
    Delete { key: Vec<u8> },
}

impl KvMutation {
    /// Build a put mutation.
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Build a delete mutation.
    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// Return the key affected by this mutation.
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Durable position for a scan that advances by physical storage handle.
///
/// `collection` is set by logical stores that span several physical
/// collections. `after_key` is exclusive inside the named shard/group.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PhysicalScanCursor {
    /// Physical collection, when a logical namespace spans more than one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Physical shard that the next page starts in.
    pub shard: u16,
    /// Hash group that the next page starts in. `None` is a segment-log shard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<u32>,
    /// Last raw key consumed in this handle. The next page starts after it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_key: Option<Vec<u8>>,
}

/// One physically bounded scan page.
#[derive(Debug, Clone, Default)]
pub struct PhysicalScanPage {
    /// Rows returned from the visited physical handles.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Resume position, or `None` when the current physical walk is complete.
    pub next_cursor: Option<PhysicalScanCursor>,
    /// Physical handle that supplied `rows`, when the page returned rows.
    pub row_handle: Option<PhysicalScanCursor>,
    /// Physical handles resolved by this call.
    pub handles_visited: u64,
    /// Cold shard loads observed during this call.
    pub cold_shard_loads: u64,
}
