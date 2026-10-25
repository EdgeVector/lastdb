//! A schema's discovery cursor and its one temporary page of expired keys.

use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;

/// Keys that one pass erases from one schema, in the order it erases them.
pub(super) struct Backlog {
    pub(super) schema_name: String,
    pub(super) schema_type: DeclarativeSchemaType,
    /// Fields of the schema. A batch of keys must fit in one native batch, and
    /// its size follows from this count.
    pub(super) fields: usize,
    /// A cap on the batch size after a batch hit a size limit. `usize::MAX`
    /// means no cap.
    batch_cap: usize,
    /// The cutoff of the listing: a key older than this was expired then.
    pub(super) cutoff: u64,
    pub(super) source: super::discovery::Source,
    keys: Vec<KeyValue>,
    next: usize,
    failed: bool,
    abandoned: usize,
}

impl Backlog {
    pub(super) fn new(
        schema_name: String,
        schema_type: DeclarativeSchemaType,
        fields: usize,
        cutoff: u64,
        source: super::discovery::Source,
    ) -> Self {
        Self {
            schema_name,
            schema_type,
            fields,
            batch_cap: usize::MAX,
            cutoff,
            source,
            keys: Vec::new(),
            next: 0,
            failed: false,
            abandoned: 0,
        }
    }

    /// True when the pass has no batch left for this schema: it reached the
    /// last key, or a batch failed and the pass skips the schema.
    pub(super) fn is_done(&self) -> bool {
        self.failed || (self.source.is_done() && self.left() == 0)
    }

    /// Keys that the pass listed and has not reached.
    pub(super) fn left(&self) -> usize {
        self.abandoned + self.page_left()
    }

    /// Skip this schema for the rest of the pass. Its keys stay counted as left.
    pub(super) fn abandon(&mut self) {
        self.failed = true;
        self.abandoned += self.page_left();
        self.release_page();
    }

    pub(super) fn page_left(&self) -> usize {
        self.keys.len().saturating_sub(self.next)
    }

    /// Only the schema currently served by the worker owns a page. Other
    /// schemas keep cursors, never buffered expired-key pages.
    pub(super) fn set_page(&mut self, keys: Vec<KeyValue>) {
        debug_assert!(self.keys.is_empty());
        self.keys = keys;
        self.next = 0;
    }

    pub(super) fn release_page(&mut self) {
        self.keys = Vec::new();
        self.next = 0;
    }

    /// The next `n` keys. The backlog moves past them.
    pub(super) fn take(&mut self, n: usize) -> Vec<KeyValue> {
        let end = self.keys.len().min(self.next.saturating_add(n));
        let chunk = self.keys[self.next..end].to_vec();
        self.next = end;
        chunk
    }

    /// The batch size cap that earlier size limit refusals set.
    pub(super) fn batch_cap(&self) -> usize {
        self.batch_cap
    }

    /// A batch of `taken` keys hit a size limit. Put its keys back and cap the
    /// next batch at half of it, at least one key.
    pub(super) fn shrink_after_limit(&mut self, taken: usize) {
        self.next = self.next.saturating_sub(taken);
        self.batch_cap = (taken / 2).max(1);
    }

    /// Give up the keys that are left, for example when the policy is gone.
    pub(super) fn clear(&mut self) {
        self.next = self.keys.len();
        self.source = super::discovery::Source::Done;
    }
}
