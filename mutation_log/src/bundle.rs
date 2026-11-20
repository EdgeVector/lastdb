//! Signed bundle entries: caps, inner changes, and validation after blob verify.

use crate::accept::RejectReason;
use crate::entry::BundleBody;

/// Max encrypted bundle blob size (64 MiB).
pub const BUNDLE_BLOB_SIZE_MAX: u64 = 67_108_864;

/// Max inner data changes per bundle.
pub const BUNDLE_CHANGE_COUNT_MAX: u32 = 10_000;

/// One inner data change inside a decrypted bundle blob (direct put/tombstone).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleChange {
    /// Application namespace (1..=128).
    pub namespace: String,
    /// Record key within `namespace`.
    pub record_key: String,
    /// Value bytes; empty = tombstone.
    pub value: Vec<u8>,
}

/// Validate bundle body caps and `change_count` minimum (OpenAPI).
pub fn validate_bundle_body(body: &BundleBody) -> Option<RejectReason> {
    if body.blob_size == 0
        || body.blob_size > BUNDLE_BLOB_SIZE_MAX
        || body.change_count == 0
        || body.change_count > BUNDLE_CHANGE_COUNT_MAX
    {
        return Some(RejectReason::Malformed("bundle over cap or invalid counts"));
    }
    None
}

/// Validate inner changes: count, ascending `record_key` (byte-wise), unique keys per namespace.
pub fn validate_bundle_changes(
    body: &BundleBody,
    changes: &[BundleChange],
) -> Result<(), RejectReason> {
    if changes.len() != body.change_count as usize {
        return Err(RejectReason::Malformed("bundle change_count mismatch"));
    }
    let mut prev: Option<(&str, &str)> = None;
    for ch in changes {
        if ch.namespace.is_empty() || ch.namespace.len() > 128 {
            return Err(RejectReason::Malformed("bundle change namespace invalid"));
        }
        if ch.record_key.is_empty() {
            return Err(RejectReason::Malformed("bundle change record_key empty"));
        }
        let cur = (ch.namespace.as_str(), ch.record_key.as_str());
        if let Some(p) = prev {
            if cur <= p {
                return Err(RejectReason::Malformed(
                    "bundle changes must be sorted unique by namespace then record_key",
                ));
            }
        }
        prev = Some(cur);
    }
    Ok(())
}
