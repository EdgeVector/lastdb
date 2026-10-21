//! The blob counter fault counts of a verify report. They are part of
//! [`super::report::Mismatches`] and serialize flat next to the atom counts.

use super::report::Kind;
use serde::{Deserialize, Serialize};

/// Faults of the blob phases, by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMismatches {
    /// An atom counter or a tip names a blob that has no counter.
    #[serde(default)]
    pub blob_counter_missing: u64,
    /// A blob counter holds fewer references than the atom counters or tips of
    /// one page that name the blob.
    #[serde(default)]
    pub blob_counter_below_refs: u64,
    /// A blob counter holds zero, is not `suspect`, and has no `zero_since`.
    #[serde(default)]
    pub blob_zero_without_dwell: u64,
    /// A blob counter holds references and has a `zero_since`.
    #[serde(default)]
    pub blob_dwell_with_refs: u64,
    /// A blob counter has a `zero_since` and the queue has no entry for it: the
    /// dwell pass never reads the blob, so it is never deleted.
    #[serde(default)]
    pub zero_blob_without_queue_entry: u64,
    /// A queue entry names a counter that is absent, has references, is
    /// `suspect`, or has another `zero_since`.
    #[serde(default)]
    pub blob_stale_queue_entry: u64,
    /// A blob counter has the `suspect` flag.
    #[serde(default)]
    pub blob_suspect: u64,
    #[serde(default)]
    pub unreadable_blob_counter: u64,
    /// A blob counter holds references and the blob has no row in the store: an
    /// atom names a blob that is not there. A late write after the dwell makes
    /// one. With on-demand cloud file sync, a blob that has not downloaded yet
    /// reads the same way.
    #[serde(default)]
    pub blob_counter_without_blob: u64,
}

impl BlobMismatches {
    pub(super) fn slot(&mut self, kind: Kind) -> &mut u64 {
        match kind {
            Kind::BlobCounterMissing => &mut self.blob_counter_missing,
            Kind::BlobCounterBelowRefs => &mut self.blob_counter_below_refs,
            Kind::BlobZeroWithoutDwell => &mut self.blob_zero_without_dwell,
            Kind::BlobDwellWithRefs => &mut self.blob_dwell_with_refs,
            Kind::ZeroBlobWithoutQueueEntry => &mut self.zero_blob_without_queue_entry,
            Kind::BlobStaleQueueEntry => &mut self.blob_stale_queue_entry,
            Kind::BlobSuspect => &mut self.blob_suspect,
            Kind::UnreadableBlobCounter => &mut self.unreadable_blob_counter,
            Kind::BlobCounterWithoutBlob => &mut self.blob_counter_without_blob,
            other => unreachable!("{} is not a blob fault kind", other.name()),
        }
    }

    /// The blob counter faults.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.blob_counter_missing
            + self.blob_counter_below_refs
            + self.blob_zero_without_dwell
            + self.blob_dwell_with_refs
            + self.zero_blob_without_queue_entry
            + self.blob_stale_queue_entry
            + self.blob_suspect
            + self.unreadable_blob_counter
            + self.blob_counter_without_blob
    }
}
