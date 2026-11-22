//! Relay base URL for signed-history sync.

use std::path::Path;

/// The relay this node syncs with, if any.
///
/// `LASTDB_RELAY_URL` names a relay explicitly, for a reference relay or a
/// deploy under test. Otherwise a node with saved cloud credentials uses the
/// Exemem API it is connected to, which serves `/relay/v1`. A node with
/// neither has no relay, and the sync loop idles without erroring.
pub fn relay_base_url(home: &Path) -> Option<String> {
    if let Ok(url) = std::env::var("LASTDB_RELAY_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    match crate::cloud::load_saved_credentials(home) {
        Ok(Some(saved)) => {
            let url = saved.api_url.trim().trim_end_matches('/');
            (!url.is_empty()).then(|| url.to_owned())
        }
        _ => None,
    }
}
