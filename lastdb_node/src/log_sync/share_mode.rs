//! Whether this node may append to a received database.
//!
//! `read` and `write` live on the stored access view. The pull loop records
//! that view. This module does not fetch the settings again. A link
//! registration has no member entry and cannot append.

use crate::host::Host;
use crate::registered_share::RegisteredShare;

use super::received;

pub(super) enum WriteMode {
    Direct,
    Closed,
    Unknown,
}

pub(crate) fn direct_write_allowed(host: &Host, share: &RegisteredShare) -> bool {
    matches!(mode(host, share), WriteMode::Direct)
}

/// `Unknown` keeps a local spool. `Closed` retracts it. A link is `Closed`.
pub(super) fn mode(host: &Host, share: &RegisteredShare) -> WriteMode {
    if share.link || !share.signed_log {
        return WriteMode::Closed;
    }
    match received::view(&host.home, share.id.as_str()).direct_submit() {
        Some(true) => WriteMode::Direct,
        Some(false) => WriteMode::Closed,
        None => WriteMode::Unknown,
    }
}
