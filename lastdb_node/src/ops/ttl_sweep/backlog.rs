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
        }
    }

    pub(super) fn is_done(&self) -> bool {
        self.next >= self.keys.len()
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
