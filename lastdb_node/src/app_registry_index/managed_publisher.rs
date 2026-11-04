//! A first-party CI signer is an explicit trust grant, independent of the list key.

use super::{managed::ManagedReceipt, ListEntry, Resolution};
use folddb_profile::endpoints::{
    registry_ci_publisher_keys_for, registry_index_url_for, Environment,
};

pub(super) fn authorized_transition(
    prior: &ManagedReceipt,
    resolution: &Resolution,
    entry: &ListEntry,
) -> bool {
    if !prior.trust.starts_with("pinned:")
        || !resolution.trust.starts_with("pinned:")
        || prior.app_id != resolution.app_id
        || prior.publisher != prior.listing.publisher_key
        || prior.listing.source != entry.source
    {
        return false;
    }
    [Environment::Dev, Environment::Prod]
        .into_iter()
        .any(|environment| {
            let source = match (environment, resolution.app_id.as_str()) {
                (Environment::Prod, "brain" | "lastsecrets" | "search") => {
                    format!("https://github.com/EdgeVector/{}.git", resolution.app_id)
                }
                (Environment::Dev, "registry-daily-dev") => {
                    "https://github.com/EdgeVector/lastdb-app-registry.git".to_owned()
                }
                _ => return false,
            };
            let base = registry_index_url_for(environment);
            let (owner, delegate) = registry_ci_publisher_keys_for(environment);
            let allowed_keys = (prior.publisher == owner && entry.publisher_key == delegate)
                || (prior.publisher == delegate && entry.publisher_key == owner);
            !delegate.is_empty()
                && !owner.is_empty()
                && allowed_keys
                && entry.source == source
                && prior.index == base
                && resolution.index
                    == format!("{base}/{}/api/{}.json", entry.channel, entry.api_version)
        })
}
