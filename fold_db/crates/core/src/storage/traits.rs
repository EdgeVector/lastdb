//! Scan pages and the batch mutation enum.

/// One ordered mutation in a mixed durable batch.
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

/// One read of a range that names where the next read continues.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct YieldedScan {
    /// The rows read, in key order. It can be short or empty while the range
    /// has not ended.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// The last key this read examined. The next read starts at the least key
    /// above it. `None` means the range ended.
    pub resume: Option<Vec<u8>>,
}
