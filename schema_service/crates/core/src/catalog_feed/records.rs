//! Apply one run to the catalog records: upsert the definitions read, then
//! recompute every record's listings from the verified lists.

use std::collections::{HashMap, HashSet};

use schema_types::Schema;

use super::{
    manifest_key, CatalogFeedImage, CatalogFeedReport, CatalogFeedState, CatalogListing,
    CatalogOrigin, CatalogRecord,
};

/// The listings of each identity hash in the current lists: each listing
/// once, in list order.
fn listings_by_identity(state: &CatalogFeedState) -> HashMap<String, Vec<CatalogListing>> {
    let mut listings: HashMap<String, Vec<CatalogListing>> = HashMap::new();
    for list in state.lists.values() {
        for entry in &list.entries {
            let Some(manifest) = state
                .manifests
                .get(&manifest_key(&entry.app_id, &entry.sha))
            else {
                continue;
            };
            for identity in &manifest.identity_hashes {
                let listing = CatalogListing {
                    channel: list.channel.clone(),
                    api_version: list.api_version,
                    sha: entry.sha.clone(),
                    source: entry.source.clone(),
                };
                let carried = listings.entry(identity.clone()).or_default();
                if !carried.contains(&listing) {
                    carried.push(listing);
                }
            }
        }
    }
    listings
}

/// The identity hashes that the lists changed in this run carry.
fn seen_identities(state: &CatalogFeedState, changed_lists: &[String]) -> HashSet<String> {
    let mut seen = HashSet::new();
    for list in changed_lists.iter().filter_map(|key| state.lists.get(key)) {
        for entry in &list.entries {
            if let Some(manifest) = state
                .manifests
                .get(&manifest_key(&entry.app_id, &entry.sha))
            {
                seen.extend(manifest.identity_hashes.iter().cloned());
            }
        }
    }
    seen
}

/// Upsert the definitions read this run by identity hash, in read order, so
/// the last definition read for a hash wins. Returns the definitions that
/// are new or changed.
pub(super) fn upsert_definitions(
    image: &mut CatalogFeedImage,
    definitions: Vec<Schema>,
    now: &str,
    report: &mut CatalogFeedReport,
) -> Vec<Schema> {
    let mut touched = Vec::new();
    for definition in definitions {
        let Some(identity_hash) = definition.identity_hash.clone() else {
            continue;
        };
        if let Some(record) = image.records.get_mut(&identity_hash) {
            if record.definition != definition {
                record.definition = definition.clone();
                report.records_updated += 1;
                touched.push(definition);
            }
            continue;
        }
        let owner_app_id = definition.owner_app_id.clone().unwrap_or_default();
        image.records.insert(
            identity_hash.clone(),
            CatalogRecord {
                identity_hash,
                definition: definition.clone(),
                owner_app_id,
                listings: Vec::new(),
                listed: false,
                first_seen: now.to_string(),
                last_seen: now.to_string(),
                origin: CatalogOrigin::Registry,
            },
        );
        report.records_added += 1;
        touched.push(definition);
    }
    touched
}

/// Recompute each record's listings. A record that no current list carries
/// stays in the catalog with `listed: false` (decision A6). A record that a
/// list changed in this run carries gets `last_seen = now`.
pub(super) fn relist(image: &mut CatalogFeedImage, now: &str, report: &mut CatalogFeedReport) {
    let mut current = listings_by_identity(&image.state);
    let seen = seen_identities(&image.state, &report.lists_changed);
    for (identity_hash, record) in &mut image.records {
        let listings = current.remove(identity_hash).unwrap_or_default();
        let listed = !listings.is_empty();
        if record.listed && !listed {
            report.records_unlisted += 1;
        }
        if seen.contains(identity_hash) {
            record.last_seen = now.to_string();
        }
        record.listings = listings;
        record.listed = listed;
    }
    report.records_total = image.records.len();
}
