//! Small helpers re-exported from the history module.

use crate::entry::GrantBody;
use crate::grants::AcceptedMeta;

/// Compute expected next lamport for authoring.
#[must_use]
pub fn next_lamport(prev_lamport: u64, seen_lamports: impl Iterator<Item = u64>) -> u64 {
    let mut max = prev_lamport;
    for l in seen_lamports {
        max = max.max(l);
    }
    1 + max
}

/// Borrow grant body from accepted meta helper for external folders.
#[must_use]
pub fn grant_body_of(meta: &AcceptedMeta) -> Option<&GrantBody> {
    meta.grant_body.as_ref()
}
