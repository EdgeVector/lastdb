//! S3 upload/download, decrypt proofs, and cursor helpers.

mod cursor;
mod download;
mod fetch;
mod proof;
mod retry;
mod upload;

use crate::sync::error::SyncError;

/// True when S3 refused the object for exceeding `max_download_entry_bytes`.
///
/// Shared by retry (non-retryable classification) and fetch (skip + advance
/// cursor) so a wording change cannot reintroduce oversize-vs-transient
/// confusion between the two call sites.
fn is_oversize_s3_error(err: &SyncError) -> bool {
    matches!(err, SyncError::S3(msg) if msg.contains("max_download_entry_bytes"))
}
