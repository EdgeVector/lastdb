//! Pinned publisher-key rotation rule for former CI-published first-party apps.

use super::{managed, Resolution};

pub(crate) fn authorized_transition(
    prior: &managed::ManagedReceipt,
    resolution: &Resolution,
) -> bool {
    use lastdb_node::profile::endpoints::{
        registry_ci_publisher_keys_for, registry_index_url_for, Environment,
    };
    let e = &resolution.entry;
    if !prior.trust.starts_with("pinned:")
        || !resolution.trust.starts_with("pinned:")
        || prior.app_id != e.app_id
        || prior.publisher != prior.listing.publisher_key
        || prior.listing.source != e.source
    {
        return false;
    }
    for env in [Environment::Dev, Environment::Prod] {
        let src = match (env, e.app_id.as_str()) {
            (Environment::Prod, "brain" | "lastsecrets") => {
                format!("https://github.com/EdgeVector/{}.git", e.app_id)
            }
            (Environment::Dev, "registry-daily-dev") => {
                "https://github.com/EdgeVector/lastdb-app-registry.git".into()
            }
            _ => continue,
        };
        let base = registry_index_url_for(env);
        let (owner, del) = registry_ci_publisher_keys_for(env);
        let rotated = (prior.publisher == owner && e.publisher_key == del)
            || (prior.publisher == del && e.publisher_key == owner);
        if !del.is_empty()
            && !owner.is_empty()
            && rotated
            && e.source == src
            && prior.index == base
            && resolution.index == format!("{base}/{}/api/{}.json", e.channel, e.api_version)
        {
            return true;
        }
    }
    false
}
