//! The expired keys of one schema, listed once at the start of a pass.

use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;

/// Keys that one pass erases from one schema, in the order it erases them.
pub(super) struct Backlog {
    pub(super) schema_name: String,
    pub(super) schema_type: DeclarativeSchemaType,
    /// The cutoff of the listing: a key older than this was expired then.
    pub(super) cutoff: u64,
    keys: Vec<KeyValue>,
    next: usize,
    failed: bool,
}

impl Backlog {
    pub(super) fn new(
        schema_name: String,
        schema_type: DeclarativeSchemaType,
        cutoff: u64,
        keys: Vec<KeyValue>,
    ) -> Self {
        Self {
            schema_name,
            schema_type,
            cutoff,
            keys,
            next: 0,
            failed: false,
        }
    }

    /// True when the pass has no batch left for this schema: it reached the
    /// last key, or a batch failed and the pass skips the schema.
    pub(super) fn is_done(&self) -> bool {
        self.failed || self.next >= self.keys.len()
    }

    /// Keys that the pass listed and has not reached.
    pub(super) fn left(&self) -> usize {
        self.keys.len().saturating_sub(self.next)
    }

    /// Skip this schema for the rest of the pass. Its keys stay counted as left.
    pub(super) fn abandon(&mut self) {
        self.failed = true;
    }

    /// The next `n` keys. The backlog moves past them.
    pub(super) fn take(&mut self, n: usize) -> Vec<KeyValue> {
        let end = self.keys.len().min(self.next.saturating_add(n));
        let chunk = self.keys[self.next..end].to_vec();
        self.next = end;
        chunk
    }

    /// Give up the keys that are left, for example when the policy is gone.
    pub(super) fn clear(&mut self) {
        self.next = self.keys.len();
    }
}
