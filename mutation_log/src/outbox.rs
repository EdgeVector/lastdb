//! Local outbox of signed entries not yet acknowledged by a relay.

use std::collections::{BTreeMap, VecDeque};

use crate::entry::SignedEntry;
use crate::ids::EntryHash;

/// FIFO outbox keyed by entry hash for ack.
#[derive(Clone, Debug, Default)]
pub struct Outbox {
    order: VecDeque<EntryHash>,
    entries: BTreeMap<EntryHash, SignedEntry>,
}

impl Outbox {
    /// Empty outbox.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of pending uploads.
    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Enqueue a signed entry (idempotent on hash).
    pub fn enqueue(&mut self, hash: EntryHash, entry: SignedEntry) {
        if self.entries.contains_key(&hash) {
            return;
        }
        self.order.push_back(hash);
        self.entries.insert(hash, entry);
    }

    /// Peek the next entry to upload.
    #[must_use]
    pub fn peek(&self) -> Option<(&EntryHash, &SignedEntry)> {
        let hash = self.order.front()?;
        Some((hash, self.entries.get(hash)?))
    }

    /// Drain up to `limit` entries in order (does not remove).
    #[must_use]
    pub fn batch(&self, limit: usize) -> Vec<&SignedEntry> {
        self.order
            .iter()
            .take(limit)
            .filter_map(|h| self.entries.get(h))
            .collect()
    }

    /// Acknowledge that `hash` was accepted by a relay (remove from outbox).
    pub fn ack(&mut self, hash: &EntryHash) -> bool {
        if self.entries.remove(hash).is_none() {
            return false;
        }
        self.order.retain(|h| h != hash);
        true
    }

    /// Acknowledge many hashes.
    pub fn ack_many(&mut self, hashes: &[EntryHash]) {
        for h in hashes {
            self.ack(h);
        }
    }
}
