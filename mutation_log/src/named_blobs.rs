//! The blobs that a signed entry needs on a relay.

use crate::entry::SignedEntry;
use crate::ids::Hex32;

/// The digest of every blob that `entry` names and that a reader of the entry fetches: the file
/// of a data entry, the encrypted body of a bundle, the manifest of a snapshot.
///
/// The files of a snapshot are named by its manifest, not by the entry, so they are not here. A
/// `blob_delete` entry names a blob to drop, so it needs none. Every kind is listed, so a new
/// kind that carries a digest does not compile until it is placed.
#[must_use]
pub fn named_blobs(entry: &SignedEntry) -> Vec<Hex32> {
    match entry {
        SignedEntry::Data { body, .. } => body.file.iter().map(|file| file.sha256).collect(),
        SignedEntry::Bundle { body, .. } => vec![body.blob_sha256],
        SignedEntry::Snapshot { body, .. } => vec![body.manifest_sha256],
        SignedEntry::Birth { .. }
        | SignedEntry::Grant { .. }
        | SignedEntry::Schema { .. }
        | SignedEntry::BlobDelete { .. }
        | SignedEntry::Heads { .. }
        | SignedEntry::Relays { .. }
        | SignedEntry::Lease { .. } => Vec::new(),
    }
}
