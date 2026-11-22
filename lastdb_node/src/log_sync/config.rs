//! Relay base URL for signed-history sync.

use std::path::Path;

/// The relay that `LASTDB_RELAY_URL` names, if it names one.
#[must_use]
pub fn explicit_relay_url() -> Option<String> {
    let url = std::env::var("LASTDB_RELAY_URL").ok()?;
    let trimmed = url.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// The relay this node syncs with, if any.
///
/// `LASTDB_RELAY_URL` names a relay explicitly, for a reference relay or a
/// deploy under test. Otherwise a node with saved cloud credentials uses the
/// Exemem API it is connected to, which serves `/relay/v1`. A node with
/// neither has no relay, and the sync loop idles without erroring.
pub fn relay_base_url(home: &Path) -> Option<String> {
    if let Some(url) = explicit_relay_url() {
        return Some(url);
    }
    match crate::cloud::load_saved_credentials(home) {
        Ok(Some(saved)) => {
            let url = saved.api_url.trim().trim_end_matches('/');
            (!url.is_empty()).then(|| url.to_owned())
        }
        _ => None,
    }
}
