//! Side effects of accepting an entry into [`VerifiedHistory`].

use crate::accept::{AcceptOutcome, RejectReason};
use crate::entry::{EntryHeader, SignedEntry};
use crate::ids::EntryHash;
use crate::payload::{decode_record_payload, MaterializedRecord, RecordPosition};
use crate::sealed::{decode_named, SEALED_NAMESPACE};

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
            SignedEntry::Data { header, body } => {
                // Proposals count as content only after publisher incorporation.
                let merge = body.stream == crate::kinds::StreamKind::Direct
                    || (is_publisher && body.incorporated_from.is_some());
                if merge {
                    let author = body
                        .incorporated_from
                        .as_ref()
                        .filter(|_| is_publisher)
                        .map_or(position.node_key, |from| from.node_key);
                    if self.is_sealed(&body.namespace) {
                        if let Some((namespace, payload)) = self.open_content(header, &body.payload)
                        {
                            self.merge_data(&namespace, &payload, position, author, hash);
                        }
                    } else {
                        self.merge_data(&body.namespace, &body.payload, position, author, hash);
                    }
                }
            }
            SignedEntry::Schema { header, body } => {
                if self.is_sealed(&body.namespace) {
                    if let Some((namespace, definition)) =
                        self.open_content(header, &body.definition)
                    {
                        self.schemas.insert(namespace, definition);
                    }
                } else {
                    self.schemas
                        .insert(body.namespace.clone(), body.definition.clone());
                }
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

    /// True when `namespace` marks a sealed payload that this history can open.
    fn is_sealed(&self, namespace: &str) -> bool {
        self.opener.is_some() && namespace == SEALED_NAMESPACE
    }

    /// The schema name and the bytes after it inside a sealed payload.
    fn open_content(&self, header: &EntryHeader, sealed: &[u8]) -> Option<(String, Vec<u8>)> {
        let plain = self.opener.as_ref()?.open(header, sealed)?;
        let (name, rest) = decode_named(&plain)?;
        Some((name.to_owned(), rest.to_vec()))
    }

    /// A history with an opener takes a data or schema entry only when its
    /// payload is sealed and opens. Other kinds, and an entry that is already
    /// accepted, pass.
    pub(super) fn require_sealed(
        &self,
        entry: &SignedEntry,
        outcome: AcceptOutcome,
    ) -> AcceptOutcome {
        let AcceptOutcome::Accept {
            duplicate: false, ..
        } = outcome
        else {
            return outcome;
        };
        if self.opener.is_none() {
            return outcome;
        }
        let (namespace, sealed) = match entry {
            SignedEntry::Data { body, .. } => (&body.namespace, &body.payload),
            SignedEntry::Schema { body, .. } => (&body.namespace, &body.definition),
            _ => return outcome,
        };
        let reason = if namespace != SEALED_NAMESPACE {
            "this database takes sealed entries only"
        } else if self.open_content(entry.header(), sealed).is_none() {
            "sealed payload does not open"
        } else {
            return outcome;
        };
        AcceptOutcome::Reject {
            reason: RejectReason::Malformed(reason),
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
