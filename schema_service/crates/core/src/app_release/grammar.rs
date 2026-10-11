//! Grammar bounds and key helpers.

/// A channel name is a short lowercase label (`stable`, `beta`, `canary-1`).
pub(super) const CHANNEL_MAX_LEN: usize = 32;
/// `release_id` and `artifact_digest` are both SHA-256 hex.
pub(super) const SHA256_HEX_LEN: usize = 64;
/// A git commit id: 40 hex for SHA-1, 64 for SHA-256 repositories.
pub(super) const COMMIT_MIN_LEN: usize = 7;
pub(super) const COMMIT_MAX_LEN: usize = 64;
/// The app UUID that anchors the release execution identity.
pub(super) const APP_UUID_MAX_LEN: usize = 64;
/// Artifact URL bound — the same ceiling the app metadata URLs use.
pub(super) const ARTIFACT_URL_MAX_LEN: usize = 300;
/// Base64 Ed25519 signature over the artifact digest.
pub(super) const ARTIFACT_SIGNATURE_MAX_LEN: usize = 200;
/// A release manifest may lock at most this many schemas. Generous for any
/// realistic app, and bounded so one manifest cannot bloat the registry.
pub(super) const MANIFEST_SCHEMAS_MAX: usize = 256;
/// Revocation reason bound.
pub(super) const REVOCATION_REASON_MAX_LEN: usize = 500;

/// The record separator that joins an app id and a channel name into one
/// exact key. Neither grammar admits `0x1f`, so the join is unambiguous.
pub(super) const CHANNEL_KEY_SEPARATOR: char = '\u{1f}';

/// Exact key for a channel record: `<app_id>\x1f<channel>`.
///
/// A channel is scoped to its app, so its key needs both. Joining them with
/// a byte neither grammar allows keeps the key a single point-get string
/// rather than a prefix query.
#[must_use]
pub fn channel_key(app_id: &str, channel: &str) -> String {
    format!("{app_id}{CHANNEL_KEY_SEPARATOR}{channel}")
}

/// `^[a-z][a-z0-9-]{0,31}$` — the channel-name grammar, hand-checked so the
/// crate keeps its zero-regex dependency profile.
#[must_use]
pub fn is_valid_channel(channel: &str) -> bool {
    let bytes = channel.as_bytes();
    if bytes.is_empty() || bytes.len() > CHANNEL_MAX_LEN {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes[1..]
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

pub(super) fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Is `value` a well-formed release id or artifact digest (SHA-256 hex)?
#[must_use]
pub fn is_sha256_hex(value: &str) -> bool {
    is_lower_hex(value, SHA256_HEX_LEN)
}
