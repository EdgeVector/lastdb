//! Where `fetch-blob` gets a record file that is not on this device (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! Core has no cloud client. The node registers a source on [`FoldDB`] when
//! this home can ask the cloud, and on a personal home that still needs the
//! owner to sign in. A dedicated shared home keeps no source. With no source,
//! a record file that is not local cannot be read. The source returns the
//! sealed row bytes exactly as the cloud stores them. Core checks them against
//! the record's marker before it uses or installs them, so the source is not
//! trusted for integrity.
//!
//! [`FoldDB`]: super::FoldDB

use async_trait::async_trait;
pub use cloud_backup_protocol::v2::blobs::BlobPath;

/// Owner-visible body when a personal home has no saved credentials.
/// The download returns this sentence as the whole message, and the
/// proof matches these bytes.
pub const SIGN_IN_TO_FETCH_FILE: &str = "sign in to fetch this file";

/// The result of one blob download.
pub enum BlobFetch {
    /// The sealed row bytes, exactly as stored.
    Bytes(Vec<u8>),
    /// The cloud has no object at this path: another writer released it, or
    /// it was never uploaded.
    Absent,
    /// The cloud could not be asked or did not answer; the reason is
    /// owner-visible.
    Unavailable(String),
}

/// Downloads the sealed row of one record file version.
#[async_trait]
pub trait RecordBlobSource: Send + Sync {
    /// Read the object at `path`. `sealed_bytes` is the exact length the
    /// record's marker allows; a source refuses an object of another length
    /// as [`BlobFetch::Unavailable`] instead of downloading it.
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> BlobFetch;
}
