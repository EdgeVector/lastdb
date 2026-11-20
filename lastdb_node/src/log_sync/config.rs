//! Relay URL and sync identity configuration for signed history.

use std::path::Path;

use crate::profile::private_fs as disk;

const RELAY_URL_FILE: &str = "log-sync/relay-url";
const LIMIT: usize = 4096;

/// Read the configured relay base URL, if any.
///
/// Absent or empty → sync loop idles without erroring the daemon.
pub fn relay_base_url(home: &Path) -> Option<String> {
    if let Ok(url) = std::env::var("LASTDB_RELAY_URL") {
        let trimmed = url.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_owned());
        }
    }
    let path = home.join(RELAY_URL_FILE);
    let Ok(Some(bytes)) = disk::read_limited(&path, LIMIT) else {
        return None;
    };
    let text = std::str::from_utf8(&bytes).ok()?.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

/// Persist a relay base URL under the home (for restore / bootstrap tools).
pub fn store_relay_base_url(home: &Path, url: &str) -> Result<(), String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err("relay URL is empty".into());
    }
    disk::write_private(
        &home.join(RELAY_URL_FILE),
        format!("{trimmed}\n").as_bytes(),
    )
    .map_err(|_| "cannot write relay URL".to_owned())
}
