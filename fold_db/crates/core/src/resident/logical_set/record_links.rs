//! Exact request-path aliases to fetched logical records. Aliases contain no
//! body, map page, file handle, or independently admitted storage object.

use super::{LogicalResidentSet, RecordKey};
use std::collections::HashMap;

const MAX_LINKS: usize = 10_000;
const MAX_OWNER_LINKS: usize = 4;
const MAX_LINK_KEY_BYTES: usize = 256;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LinkKey {
    namespace: String,
    alias: String,
}

#[derive(Debug, Default)]
pub(super) struct RecordLinks {
    targets: HashMap<LinkKey, RecordKey>,
    owners: HashMap<RecordKey, Vec<LinkKey>>,
}

impl LogicalResidentSet {
    /// Link one complete request path to an existing fetched record. The
    /// namespace and alias must include every scope; the caller validates its
    /// captured view before it uses the body.
    /// The set permits at most 10,000 links and four aliases per owner.
    /// Capacity refusal leaves an existing alias unchanged.
    pub fn link_record(
        &mut self,
        namespace: &str,
        alias: &str,
        collection: &str,
        id: &str,
    ) -> bool {
        if namespace.len().saturating_add(alias.len()) > MAX_LINK_KEY_BYTES {
            return false;
        }
        let owner = RecordKey {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        if !self.records.contains_key(&owner) {
            return false;
        }
        self.record_links.link(link_key(namespace, alias), owner)
    }

    /// Inspect the linked owner before byte admission. This method copies no
    /// body and changes no recency; the subsequent fetched_record marks use.
    pub fn linked_record_target(&self, namespace: &str, alias: &str) -> Option<(String, String)> {
        let owner = self.record_links.targets.get(&link_key(namespace, alias))?;
        self.records
            .contains_key(owner)
            .then(|| (owner.collection.clone(), owner.id.clone()))
    }

    /// Fetch through an exact alias and mark its single owner most recent.
    /// None in the returned body means that the owner was fetched absent.
    pub fn linked_record(
        &mut self,
        namespace: &str,
        alias: &str,
    ) -> Option<(String, String, Option<Vec<u8>>)> {
        let (collection, id) = self.linked_record_target(namespace, alias)?;
        let body = self.fetched_record(&collection, &id)?;
        Some((collection, id, body))
    }
}

impl RecordLinks {
    fn link(&mut self, link: LinkKey, owner: RecordKey) -> bool {
        let previous = self.targets.get(&link);
        if previous == Some(&owner) {
            return true;
        }
        if (previous.is_none() && self.targets.len() >= MAX_LINKS)
            || self
                .owners
                .get(&owner)
                .is_some_and(|links| links.len() >= MAX_OWNER_LINKS)
        {
            return false;
        }
        if let Some(previous) = self.targets.insert(link.clone(), owner.clone()) {
            let empty = self.owners.get_mut(&previous).is_some_and(|links| {
                links.retain(|old| old != &link);
                links.is_empty()
            });
            if empty {
                self.owners.remove(&previous);
            }
        }
        self.owners.entry(owner).or_default().push(link);
        true
    }

    pub(super) fn forget_owner(&mut self, owner: &RecordKey) {
        if let Some(links) = self.owners.remove(owner) {
            for link in links {
                self.targets.remove(&link);
            }
        }
    }

    pub(super) fn clear(&mut self) {
        self.targets.clear();
        self.owners.clear();
    }
}

fn link_key(namespace: &str, alias: &str) -> LinkKey {
    LinkKey {
        namespace: namespace.to_string(),
        alias: alias.to_string(),
    }
}
