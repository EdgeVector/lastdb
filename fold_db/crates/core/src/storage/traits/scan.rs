//! Partitioned-scan results and the batch mutation enum.

use super::KvStore;
use crate::storage::error::StorageResult;

/// The result of [`KvStore::scan_prefix_partition_undecryptable`]: rows that
/// read (decrypt) cleanly, separated from the keys of rows whose at-rest value
/// could not be decrypted with any registered key ("poison" / undecryptable
/// rows).
///
/// Unlike [`KvStore::scan_prefix`] (which fails the whole scan on the first
/// undecryptable row), this keeps the undecryptable keys so the caller can *report* them
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
/// The disk format v2 backend validates the whole batch before it writes, then
/// commits it as one write or stages it in the open transaction batch.
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

/// Position of a resumable scan.
///
/// `after_key` is exclusive. Disk format v2 has no physical shards or hash
/// groups, and every page comes from the default
/// [`KvStore::scan_range_physical_paged`], so the cursor is the last key read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalScanCursor {
    /// Last raw key consumed. The next page starts after it.
    pub after_key: Option<Vec<u8>>,
}

/// One physically bounded scan page.
#[derive(Debug, Clone, Default)]
pub struct PhysicalScanPage {
    /// Rows returned from the visited physical handles.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Resume position, or `None` when the current physical walk is complete.
    pub next_cursor: Option<PhysicalScanCursor>,
}

/// One read of a range that names where the next read continues.
///
/// Returned by [`KvStore::scan_range_yielding`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct YieldedScan {
    /// The rows read, in key order. It can be short or empty while the range
    /// has not ended.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// The last key this read examined. The next read starts at the least key
    /// above it. `None` means the range ended.
    pub resume: Option<Vec<u8>>,
}

/// The default of [`KvStore::scan_range_physical_paged`]: one logical keyset page.
pub(super) async fn keyset_physical_page<S: KvStore + ?Sized>(
    store: &S,
    start: &[u8],
    end: &[u8],
    cursor: Option<&PhysicalScanCursor>,
    limit: usize,
    max_handles: usize,
) -> StorageResult<PhysicalScanPage> {
    if limit == 0 || max_handles == 0 || start >= end {
        return Ok(PhysicalScanPage {
            next_cursor: cursor.cloned(),
            ..Default::default()
        });
    }
    let resume = cursor
        .and_then(|position| position.after_key.as_deref())
        .unwrap_or(start);
    // `scan_range_paged` includes its start key. A resume page therefore
    // needs one slot for that duplicate plus one slot for lookahead.
    let lookahead = limit
        .saturating_add(1)
        .saturating_add(usize::from(cursor.is_some()));
    let mut rows = store.scan_range_paged(resume, end, lookahead).await?;
    if let Some(after) = cursor.and_then(|position| position.after_key.as_deref()) {
        rows.retain(|(key, _)| key.as_slice() != after);
    }
    let more_remaining = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = more_remaining.then(|| PhysicalScanCursor {
        after_key: rows.last().map(|(key, _)| key.clone()),
    });
    Ok(PhysicalScanPage { rows, next_cursor })
}
