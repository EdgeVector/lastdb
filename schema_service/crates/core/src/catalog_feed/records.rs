//! Apply one run to the catalog records: upsert the definitions read, then
//! recompute every record's listings from the verified lists.

use std::collections::{BTreeMap, BTreeSet};

use schema_types::Schema;

use super::{
    manifest_key, CatalogFeedImage, CatalogFeedReport, CatalogFeedState, CatalogListing,
    CatalogOrigin, CatalogRecord,
};

/// The listings of each identity hash in the current lists, or only in the
/// lists named by `only`.
fn listings_by_identity(
    state: &CatalogFeedState,
    only: Option<&BTreeSet<String>>,
) -> BTreeMap<String, BTreeSet<CatalogListing>> {
    let mut listings: BTreeMap<String, BTreeSet<CatalogListing>> = BTreeMap::new();
    for (key, list) in &state.lists {
        if only.is_some_and(|keys| !keys.contains(key)) {
            continue;
        }
        for entry in &list.entries {
            let Some(manifest) = state
                .manifests
                .get(&manifest_key(&entry.app_id, &entry.sha))
            else {
                continue;
            };
            for identity in &manifest.identity_hashes {
                listings
                    .entry(identity.clone())
                    .or_default()
                    .insert(CatalogListing {
                        channel: list.channel.clone(),
                        api_version: list.api_version,
                        sha: entry.sha.clone(),
                        source: entry.source.clone(),
                    });
            }
        }
    }
    listings
}

/// Upsert the definitions read this run by identity hash. Returns the
/// definitions that are new or changed.
pub(super) fn upsert_definitions(
    image: &mut CatalogFeedImage,
    definitions: BTreeMap<String, Schema>,
    now: &str,
    report: &mut CatalogFeedReport,
) -> Vec<Schema> {
    let mut touched = Vec::new();
    for (identity_hash, definition) in definitions {
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
                listings: BTreeSet::new(),
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
/// stays in the catalog with `listed: false` (decision A6).
pub(super) fn relist(
    image: &mut CatalogFeedImage,
    changed_lists: &BTreeSet<String>,
    now: &str,
    report: &mut CatalogFeedReport,
) {
    let mut current = listings_by_identity(&image.state, None);
    let seen = listings_by_identity(&image.state, Some(changed_lists));
    for (identity_hash, record) in &mut image.records {
        let listings = current.remove(identity_hash).unwrap_or_default();
        let listed = !listings.is_empty();
        if record.listed && !listed {
            report.records_unlisted += 1;
        }
        if seen.contains_key(identity_hash) {
            record.last_seen = now.to_string();
        }
        record.listings = listings;
        record.listed = listed;
    }
    report.records_total = image.records.len();
}
