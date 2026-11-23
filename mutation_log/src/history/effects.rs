//! Side effects of accepting an entry into [`VerifiedHistory`].

use crate::accept::{AcceptOutcome, RejectReason};
use crate::entry::{DataBody, EntryHeader, SignedEntry};
use crate::ids::EntryHash;
use crate::payload::{decode_record_payload, MaterializedRecord, RecordPosition};
use crate::sealed::{decode_named, SEALED_NAMESPACE};

use super::recompute::WriteMeta;
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
                self.apply_data_effect(header, body, hash, position, is_publisher);
            }
            SignedEntry::Lease { header, body, .. } => {
                self.note_lease(hash, position, header, body);
            }
            SignedEntry::Schema { header, body } => {
                self.apply_schema_effect(header, body, position);
            }
            SignedEntry::Bundle { body, .. } => {
                self.live_bundle_blobs.insert(body.blob_sha256);
            }
            SignedEntry::BlobDelete { body, .. }
                if !self.live_bundle_blobs.contains(&body.sha256) =>
            {
                self.deleted_blobs.insert(body.sha256);
            }
            SignedEntry::Heads { body, .. }
                if self
                    .publisher_heads_position
                    .is_none_or(|held| position.beats(held)) =>
            {
                self.publisher_heads_position = Some(position);
                self.publisher_heads = body.publisher_heads.clone();
            }
            SignedEntry::Relays { body, .. }
                if self.relays_position.is_none_or(|held| position.beats(held)) =>
            {
                self.relays_position = Some(position);
                self.relays = body.relays.iter().map(|r| r.url.clone()).collect();
            }
            _ => {}
        }
    }

    fn apply_schema_effect(
        &mut self,
        header: &EntryHeader,
        body: &crate::entry::SchemaBody,
        position: RecordPosition,
    ) {
        let opened = if self.is_sealed(&body.namespace) {
            self.open_content(header, &body.definition)
        } else {
            Some((body.namespace.clone(), body.definition.clone()))
        };
        let Some((namespace, definition)) = opened else {
            return;
        };
        if self
            .schema_positions
            .get(&namespace)
            .is_none_or(|held| position.beats(*held))
        {
            self.schema_positions.insert(namespace.clone(), position);
            self.schemas.insert(namespace, definition);
        }
    }

    fn apply_data_effect(
        &mut self,
        header: &EntryHeader,
        body: &DataBody,
        hash: EntryHash,
        position: RecordPosition,
        is_publisher: bool,
    ) {
        // Proposals count as content only after publisher incorporation.
        // A member's direct entry counts under the lease it names.
        if body.stream == crate::kinds::StreamKind::Direct && !is_publisher {
            self.note_member_direct(header, hash);
        }
        let Some((author, namespace, decoded)) = self.decoded_direct(header, body, is_publisher)
        else {
            return;
        };
        let key = (namespace, decoded.record_key);
        self.record_write_meta(
            &key,
            WriteMeta {
                position,
                author,
                entry: hash,
                tombstone: decoded.value.is_empty(),
                from_bundle: false,
            },
        );
        if is_publisher || !self.excluded.contains(&hash) {
            let write = MaterializedRecord {
                position,
                author,
                entry: hash,
                creator: author,
                creator_position: position,
                value: decoded.value,
            };
            self.merge_write(key, write, false);
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

    /// The namespace and record payload that a data entry writes: the plain
    /// ones, or the opened ones of a sealed entry.
    pub(super) fn data_write(
        &self,
        header: &EntryHeader,
        body: &DataBody,
    ) -> Option<(String, Vec<u8>)> {
        if self.is_sealed(&body.namespace) {
            self.open_content(header, &body.payload)
        } else {
            Some((body.namespace.clone(), body.payload.clone()))
        }
    }

    /// Merge one whole-record write by payload. `from_bundle` marks a bundle's change.
    pub(crate) fn merge_data(
        &mut self,
        namespace: &str,
        payload: &[u8],
        position: RecordPosition,
        author: crate::ids::NodeKey,
        entry: EntryHash,
        from_bundle: bool,
    ) {
        let Ok(decoded) = decode_record_payload(payload) else {
            return;
        };
        let key = (namespace.to_owned(), decoded.record_key);
        self.record_write_meta(
            &key,
            WriteMeta {
                position,
                author,
                entry,
                tombstone: decoded.value.is_empty(),
                from_bundle,
            },
        );
        let write = MaterializedRecord {
            position,
            author,
            entry,
            creator: author,
            creator_position: position,
            value: decoded.value,
        };
        self.merge_write(key, write, from_bundle);
    }

    /// Merge one whole-record write: the higher position wins. When a bundle
    /// write and a non-bundle write meet and the bundle write loses, it is kept
    /// aside, because a lease closing can take the other write back.
    pub(crate) fn merge_write(
        &mut self,
        key: (String, String),
        write: MaterializedRecord,
        from_bundle: bool,
    ) {
        let position = write.position;
        let mut aside: Option<MaterializedRecord> = None;
        if let Some(held) = self.records.get(&key) {
            let held_bundle = self.is_bundle_write(&key, held.position);
            if from_bundle != held_bundle {
                let (bundle_at, other_at) = if from_bundle {
                    (position, held.position)
                } else {
                    (held.position, position)
                };
                if !bundle_at.beats(other_at) {
                    aside = Some(if from_bundle {
                        write.clone()
                    } else {
                        held.clone()
                    });
                }
            }
        }
        if let Some(candidate) = aside {
            let better = self
                .bundle_floor
                .get(&key)
                .is_none_or(|floor| candidate.position.beats(floor.position));
            if better {
                self.bundle_floor.insert(key.clone(), candidate);
            }
        }
        let (creator, creator_position) = self
            .active_creator(&key)
            .unwrap_or((write.author, position));
        match self.records.get_mut(&key) {
            Some(existing) if !position.beats(existing.position) => {
                existing.creator = creator;
                existing.creator_position = creator_position;
            }
            _ => {
                self.records.insert(
                    key,
                    MaterializedRecord {
                        creator,
                        creator_position,
                        ..write
                    },
                );
            }
        }
    }
}
