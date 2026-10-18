//! No new query-visible physical change escapes its typed publication.

use crate::resident::{CoreModelChange, PreparedCoreMemory};
use crate::storage::laststore::native_routing;
use std::collections::HashSet;

struct Coverage<'a> {
    exact: HashSet<(&'a str, &'a str)>,
    wildcard: HashSet<&'a str>,
}

pub(super) fn covers(
    changes: &[CoreModelChange],
    definitions: &[PreparedCoreMemory],
    sources: &[(String, String)],
) -> bool {
    let Some(coverage) = coverage(changes, definitions) else {
        return false;
    };
    sources.iter().all(|(collection, key)| {
        if matches!(
            collection.as_str(),
            "schemas"
                | "schema_states"
                | "schema_superseded_by"
                | "molecule_keys"
                | "share_subscriptions"
                | "sync_foreign_share_bindings"
                | "db_catalog"
        ) {
            return false;
        }
        let Ok(raw) = native_routing::decode(key) else {
            return false;
        };
        if !native_routing::read_candidates(&raw).contains(&collection.as_str()) {
            // Derived markers and queue/accounting records do not resolve
            // logical values. Other namespaces stay disk-first.
            return matches!(
                collection.as_str(),
                "idempotency" | "schema_index" | "keep_small"
            );
        }
        let Ok(raw) = std::str::from_utf8(&raw) else {
            return false;
        };
        // These controls can change complete membership. Their existing
        // durable-first routes remain authoritative.
        if ["mgp", "mgr", "mgd", "ref"]
            .into_iter()
            .any(|kind| crate::kind_partition::rest_of(raw, kind).is_some())
        {
            return false;
        }
        if !["atom", "aloc", "mk", "rdel", "mh", "hcu"]
            .into_iter()
            .any(|kind| crate::kind_partition::rest_of(raw, kind).is_some())
        {
            // The typed model supplies current membership. These rows only
            // track update order or rebuildable liveness/GC bookkeeping.
            return ["mord", "moc", "aref", "mref", "bref", "gcatoms-probe-ref"]
                .into_iter()
                .any(|kind| crate::kind_partition::rest_of(raw, kind).is_some());
        }
        coverage
            .exact
            .contains(&(collection.as_str(), key.as_str()))
            || coverage.wildcard.contains(collection.as_str())
    })
}

/// Temporary borrowed facts avoid a full typed-effect search for every change.
fn coverage<'a>(
    changes: &'a [CoreModelChange],
    definitions: &'a [PreparedCoreMemory],
) -> Option<Coverage<'a>> {
    let paths = changes.iter().try_fold(0usize, |count, change| {
        count.checked_add(change.sources.len())
    })?;
    let (exact, wildcard) = definitions
        .iter()
        .flat_map(PreparedCoreMemory::sources)
        .try_fold((paths, 0usize), |(exact, wildcard), source| {
            if source.key.is_some() {
                Some((exact.checked_add(1)?, wildcard))
            } else {
                Some((exact, wildcard.checked_add(1)?))
            }
        })?;
    let mut coverage = Coverage {
        exact: HashSet::new(),
        wildcard: HashSet::new(),
    };
    coverage.exact.try_reserve(exact).ok()?;
    coverage.wildcard.try_reserve(wildcard).ok()?;
    for change in changes {
        coverage.exact.extend(
            change
                .sources
                .iter()
                .map(|(collection, key)| (collection.as_str(), key.as_str())),
        );
    }
    for source in definitions.iter().flat_map(PreparedCoreMemory::sources) {
        if let Some(key) = &source.key {
            coverage
                .exact
                .insert((source.collection.as_str(), key.as_str()));
        } else {
            coverage.wildcard.insert(source.collection.as_str());
        }
    }
    Some(coverage)
}
