//! Open local content without replacing the signed evidence used by the chain.

use std::sync::Arc;

use crate::{EntryHash, EntryOpener, RejectReason, SignedEntry};

use super::VerifiedHistory;

impl VerifiedHistory {
    /// Attach the node's database content opener before replay or acceptance.
    #[must_use]
    pub fn with_opener(mut self, opener: Arc<dyn EntryOpener>) -> Self {
        self.opener = Some(opener);
        self
    }

    /// Open an already accepted entry by its original signed hash.
    #[must_use]
    pub fn opened_entry(&self, hash: &EntryHash) -> Option<SignedEntry> {
        self.open_content(&self.accepted.get(hash)?.entry)
    }

    fn open_content(&self, entry: &SignedEntry) -> Option<SignedEntry> {
        match &self.opener {
            Some(opener) => opener.open_entry(entry),
            None => Some(entry.clone()),
        }
    }

    pub(crate) fn apply_accepted(&mut self, hash: EntryHash, entry: SignedEntry) -> bool {
        let Some(opened) = self.open_content(&entry) else {
            self.note_reject(RejectReason::Malformed("entry content did not open"), entry);
            return false;
        };
        self.apply_accepted_opened(hash, entry, &opened);
        true
    }
}
