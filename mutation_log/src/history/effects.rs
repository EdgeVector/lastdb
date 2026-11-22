//! Side effects of accepting an entry into [`VerifiedHistory`].

use std::collections::BTreeSet;

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

    /// Recompute the winners of keys written by entries that the current
    /// grants void.
    ///
    /// A publisher role reduction carries a cutoff per device stream. An entry
    /// accepted before the reduction can fall past it, and then it must not
    /// count for any node. The entry stays in the accepted map, so the stream
    /// chain is intact. Only the keys it wrote are recomputed, from the
    /// entries that still count, so a key falls back to its previous winner or
    /// has none.
    pub(super) fn refold_voided(&mut self) {
        let mut affected: BTreeSet<(String, String)> = BTreeSet::new();
        for meta in self.accepted.values() {
            let SignedEntry::Data { body, .. } = &meta.entry else {
                continue;
            };
            if meta.is_publisher
                || body.stream != crate::kinds::StreamKind::Direct
                || crate::auth::within_cutoff(&meta.entry, &self.grants)
            {
                continue;
            }
            if let Ok(decoded) = decode_record_payload(&body.payload) {
                affected.insert((body.namespace.clone(), decoded.record_key));
            }
        }
        if affected.is_empty() {
            return;
        }
        for key in &affected {
            self.records.remove(key);
        }
        self.voided.extend(affected.iter().cloned());
        let mut counted = Vec::new();
        for (hash, meta) in &self.accepted {
            let SignedEntry::Data { body, .. } = &meta.entry else {
                continue;
            };
            let merges = if meta.is_publisher {
                body.stream == crate::kinds::StreamKind::Direct || body.incorporated_from.is_some()
            } else {
                body.stream == crate::kinds::StreamKind::Direct
                    && crate::auth::within_cutoff(&meta.entry, &self.grants)
            };
            let Ok(decoded) = decode_record_payload(&body.payload) else {
                continue;
            };
            if !merges || !affected.contains(&(body.namespace.clone(), decoded.record_key)) {
                continue;
            }
            let author = body
                .incorporated_from
                .as_ref()
                .filter(|_| meta.is_publisher)
                .map_or(meta.position.node_key, |from| from.node_key);
            counted.push((
                body.namespace.clone(),
                body.payload.clone(),
                meta.position,
                author,
                *hash,
            ));
        }
        for (namespace, payload, position, author, hash) in counted {
            self.merge_data(&namespace, &payload, position, author, hash);
        }
    }

    /// Direct entries `writer` signed that the publisher's cutoffs now void,
    /// in stream order. They stay in the history; none of them counts.
    #[must_use]
    pub fn voided_direct_by(&self, writer: crate::ids::NodeKey) -> Vec<SignedEntry> {
        let mut found: Vec<SignedEntry> = self
            .accepted
            .values()
            .filter(|meta| {
                !meta.is_publisher
                    && meta.entry.header().node_key == writer
                    && matches!(
                        &meta.entry,
                        SignedEntry::Data { body, .. }
                            if body.stream == crate::kinds::StreamKind::Direct
                    )
                    && !crate::auth::within_cutoff(&meta.entry, &self.grants)
            })
            .map(|meta| meta.entry.clone())
            .collect();
        found.sort_by_key(|entry| (entry.header().device_id, entry.header().writer_seq));
        found
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
