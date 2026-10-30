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

/// `contract_range` bound, in characters. The verifier enforces the same one.
pub const CONTRACT_RANGE_MAX_LEN: usize = 128;
/// The most new releases one publisher dev key may publish in
/// [`PUBLISH_RATE_WINDOW_SECS`]. Each new release costs the verifier a
/// sandbox run per supported contract version; an idempotent re-publish
/// costs nothing and does not count.
pub const PUBLISH_RATE_LIMIT: usize = 20;
/// The window [`PUBLISH_RATE_LIMIT`] counts over: 24 hours.
pub const PUBLISH_RATE_WINDOW_SECS: i64 = 24 * 60 * 60;

/// Check a manifest `contract_range`: 1..=128 characters of comma-separated
/// comparators, each `>=V`, `>V`, `<=V`, `<V`, or `=V`, where `V` is a SemVer
/// version with an optional pre-release and no build metadata
/// (`>=0.1.0-0, <0.2.0`).
///
/// This is the verifier's grammar, so a range the registry accepts is one
/// the verifier can read.
///
/// # Errors
/// Names the first comparator that does not parse.
pub fn validate_contract_range(range: &str) -> Result<(), String> {
    if range.is_empty() || range.chars().count() > CONTRACT_RANGE_MAX_LEN {
        return Err(format!(
            "contract_range must be 1..={CONTRACT_RANGE_MAX_LEN} characters"
        ));
    }
    for comparator in range.split(',') {
        let trimmed = comparator.trim();
        let version = [">=", "<=", ">", "<", "="]
            .iter()
            .find_map(|op| trimmed.strip_prefix(op))
            .map(str::trim_start)
            .filter(|version| is_contract_version(version));
        if version.is_none() {
            return Err(format!(
                "contract_range comparator '{trimmed}' is not one of >=V, >V, <=V, <V, =V \
                 with V a version such as 0.1.0 or 0.1.0-0"
            ));
        }
    }
    Ok(())
}

/// `MAJOR.MINOR.PATCH` with an optional `-pre.release`: numeric parts have
/// no leading zero, and pre-release identifiers are `[0-9A-Za-z-]+`.
fn is_contract_version(version: &str) -> bool {
    let (core, pre) = match version.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (version, None),
    };
    core.split('.').count() == 3
        && core.split('.').all(is_numeric_identifier)
        && pre.is_none_or(|pre| pre.split('.').all(is_prerelease_identifier))
}

fn is_numeric_identifier(part: &str) -> bool {
    !part.is_empty()
        && part.bytes().all(|b| b.is_ascii_digit())
        && (part == "0" || !part.starts_with('0'))
}

fn is_prerelease_identifier(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
