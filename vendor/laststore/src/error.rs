//! Error types for Last Store.

use std::fmt;
use std::path::PathBuf;

/// Result alias for Last Store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by [`crate::LastStore`].
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem or IO failure.
    Io(std::io::Error),
    /// On-disk format is truncated, truncated mid-record, or inconsistent.
    Corrupt(String),
    /// AEAD authentication failed while decrypting a frame.
    AeadAuthFail,
    /// A sealed encrypted chunk failed verification and was moved aside.
    ChunkQuarantined {
        /// Chunk UUID that failed verification.
        chunk_uuid: uuid::Uuid,
        /// Quarantine path holding the suspect bytes.
        path: PathBuf,
    },
    /// Invalid configuration (e.g. `shard_bits` out of range).
    Config(String),
    /// A cold hash group is larger than the store will load whole.
    ///
    /// `shard_handle_at` reads, decrypts and parses every segment of a group
    /// before the first row is served, and a group has no upper bound: on
    /// 2026-09-21 the primary's `metadata/0/g/025` held 39 GB of superseded
    /// copies of one key, and the first write after boot loaded it, blew the
    /// 16 GiB memory guard and restarted the daemon every 7-17 minutes. This
    /// refusal turns that into a request error naming the group, so an
    /// operator can drop it (`drop_hash_group_dir`) instead of the process.
    /// See [`crate::LastStoreOptions::max_cold_group_load_bytes`].
    ColdGroupTooLarge {
        /// Collection that owns the group.
        collection: String,
        /// Shard number.
        shard: u16,
        /// Hash group index.
        group: u32,
        /// On-disk bytes the load would have read.
        bytes: u64,
        /// The cap in force.
        cap: u64,
    },
    /// A product read does not identify one partition. Keep caller key bytes
    /// out of both Display and Debug; request attribution uses the operation.
    UnanchoredRead {
        /// The rejected read operation; never caller-provided key text.
        operation: &'static str,
        /// Length of the rejected bound, without retaining its contents.
        prefix_bytes: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
            Self::AeadAuthFail => write!(f, "aead authentication failed"),
            Self::ChunkQuarantined { chunk_uuid, path } => {
                write!(f, "chunk {chunk_uuid} quarantined at {}", path.display())
            }
            Self::Config(s) => write!(f, "config: {s}"),
            Self::ColdGroupTooLarge {
                collection,
                shard,
                group,
                bytes,
                cap,
            } => write!(
                f,
                "cold group too large to load: {collection} shard {shard} group {group:#05x} is \
                 {bytes} bytes on disk, cap {cap} (LASTDB_MAX_COLD_GROUP_LOAD_BYTES); refusing \
                 the whole-group load rather than the process"
            ),
            Self::UnanchoredRead { operation, prefix_bytes } => write!(
                f, "unanchored read: {operation} requires one partition (bound length {prefix_bytes} bytes)"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
