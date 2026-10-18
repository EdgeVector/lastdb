//! Exact source edges belong to the warm records they protect.
//!
//! One source can serve several logical paths. Each retained owner has one
//! source, so these edges cannot outgrow the existing logical-record budget.

use super::{LogicalResidentSet, RecordKey};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub(super) struct RecordLinks {
    sources: HashMap<RecordKey, HashSet<RecordKey>>,
    owners: HashMap<RecordKey, RecordKey>,
}

impl LogicalResidentSet {
    /// Attach the exact source that supplied this already-admitted body.
    /// Source and owner edges leave with the owner; they retain no values.
    pub fn link_record_source(
        &mut self,
        collection: &str,
        id: &str,
        source_collection: &str,
        source_id: &str,
    ) -> bool {
        let owner = record_key(collection, id);
        if !self.records.contains_key(&owner) {
            return false;
        }
        self.record_links
            .link(owner, record_key(source_collection, source_id));
        true
    }

    /// A source write removes every affected warm path before publication.
    /// Active calls retain their copied result and their complete read view.
    pub fn forget_source_records(&mut self, collection: &str, id: &str) {
        let source = record_key(collection, id);
        let Some(owners) = self.record_links.sources.remove(&source) else {
            return;
        };
        for owner in owners {
            self.forget_record(&owner.collection, &owner.id);
        }
    }
}

impl RecordLinks {
    fn link(&mut self, owner: RecordKey, source: RecordKey) {
        if self.owners.get(&owner) == Some(&source) {
            return;
        }
        self.forget_owner(&owner);
        self.sources
            .entry(source.clone())
            .or_default()
            .insert(owner.clone());
        self.owners.insert(owner, source);
    }

    pub(super) fn forget_owner(&mut self, owner: &RecordKey) {
        let Some(source) = self.owners.remove(owner) else {
            return;
        };
        let empty = self.sources.get_mut(&source).is_some_and(|owners| {
            owners.remove(owner);
            owners.is_empty()
        });
        if empty {
            self.sources.remove(&source);
        }
    }

    pub(super) fn clear(&mut self) {
        self.sources.clear();
        self.owners.clear();
    }
}

fn record_key(collection: &str, id: &str) -> RecordKey {
    RecordKey {
        collection: collection.to_string(),
        id: id.to_string(),
    }
}
