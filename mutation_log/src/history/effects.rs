//! Side effects of accepting an entry into [`VerifiedHistory`].

use crate::entry::SignedEntry;
use crate::ids::EntryHash;
use crate::payload::{decode_record_payload, MaterializedRecord, RecordPosition};

use super::VerifiedHistory;

impl VerifiedHistory {
    pub(super) fn apply_effects(
        &mut self,
        entry: &SignedEntry,
        hash: EntryHash,
        position: RecordPosition,
        is_publisher: bool,
    ) {
        match entry {
            SignedEntry::Data { body, .. } => {
                // Proposals count as content only after publisher incorporation.
                let merge = body.stream == crate::kinds::StreamKind::Direct
                    || (is_publisher && body.incorporated_from.is_some());
                if merge {
                    let author = body
                        .incorporated_from
                        .as_ref()
                        .filter(|_| is_publisher)
                        .map_or(position.node_key, |from| from.node_key);
                    self.merge_data(&body.namespace, &body.payload, position, author, hash);
                }
            }
            SignedEntry::Schema { body, .. } => {
                self.schemas
                    .insert(body.namespace.clone(), body.definition.clone());
            }
            SignedEntry::Bundle { body, .. } => {
                self.live_bundle_blobs.insert(body.blob_sha256);
            }
            SignedEntry::BlobDelete { body, .. } => {
                if !self.live_bundle_blobs.contains(&body.sha256) {
                    self.deleted_blobs.insert(body.sha256);
                }
            }
            SignedEntry::Heads { body, .. } => {
                self.publisher_heads = body.publisher_heads.clone();
            }
            SignedEntry::Relays { body, .. } => {
                self.relays = body.relays.iter().map(|r| r.url.clone()).collect();
            }
            _ => {}
        }
    }

    pub(crate) fn merge_data(
        &mut self,
        namespace: &str,
        payload: &[u8],
        position: RecordPosition,
        author: crate::ids::NodeKey,
        entry: EntryHash,
    ) {
        let Ok(decoded) = decode_record_payload(payload) else {
            return;
        };
        let key = (namespace.to_owned(), decoded.record_key);
        let (creator, creator_position) = match self.records.get(&key) {
            Some(held)
                if !position.beats(held.creator_position) && position != held.creator_position =>
            {
                (author, position)
            }
            Some(held) => (held.creator, held.creator_position),
            None => (author, position),
        };
        match self.records.get_mut(&key) {
            Some(existing) if !position.beats(existing.position) => {
                existing.creator = creator;
                existing.creator_position = creator_position;
            }
            _ => {
                self.records.insert(
                    key,
                    MaterializedRecord {
                        position,
                        author,
                        entry,
                        creator,
                        creator_position,
                        value: decoded.value,
                    },
                );
            }
        }
    }
}
