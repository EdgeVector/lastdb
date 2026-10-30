//! Manifest validation.

use super::*;

pub(super) fn validate_manifest(manifest: &ReleaseManifest) -> Result<(), String> {
    if !is_valid_app_id(&manifest.app_id) {
        return Err("manifest app_id does not match ^[a-z][a-z0-9-]{0,39}$".to_string());
    }
    if manifest.app_uuid.trim().is_empty() || manifest.app_uuid.len() > APP_UUID_MAX_LEN {
        return Err(format!(
            "manifest app_uuid must be 1..={APP_UUID_MAX_LEN} characters"
        ));
    }
    if manifest.schemas.is_empty() {
        return Err("manifest schemas must lock at least one schema identity".to_string());
    }
    if manifest.schemas.len() > MANIFEST_SCHEMAS_MAX {
        return Err(format!(
            "manifest locks {} schemas; the bound is {MANIFEST_SCHEMAS_MAX}",
            manifest.schemas.len()
        ));
    }
    for (name, identity) in &manifest.schemas {
        if name.trim().is_empty() {
            return Err("manifest schemas contains an empty schema name".to_string());
        }
        if identity.trim().is_empty() {
            return Err(format!("manifest schema '{name}' has an empty identity"));
        }
    }
    let commit_len = manifest.source_commit.len();
    if !(COMMIT_MIN_LEN..=COMMIT_MAX_LEN).contains(&commit_len)
        || !manifest
            .source_commit
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "manifest source_commit must be {COMMIT_MIN_LEN}..={COMMIT_MAX_LEN} lowercase hex characters"
        ));
    }
    if !is_sha256_hex(&manifest.artifact_digest) {
        return Err("manifest artifact_digest must be 64 lowercase hex characters".to_string());
    }
    if manifest.artifact_url.trim().is_empty() || manifest.artifact_url.len() > ARTIFACT_URL_MAX_LEN
    {
        return Err(format!(
            "manifest artifact_url must be 1..={ARTIFACT_URL_MAX_LEN} characters"
        ));
    }
    if manifest.artifact_signature.trim().is_empty()
        || manifest.artifact_signature.len() > ARTIFACT_SIGNATURE_MAX_LEN
    {
        return Err(format!(
            "manifest artifact_signature must be 1..={ARTIFACT_SIGNATURE_MAX_LEN} characters"
        ));
    }
    if let Some(range) = &manifest.contract_range {
        validate_contract_range(range).map_err(|e| format!("manifest {e}"))?;
    }
    Ok(())
}

/// Refuse a manifest whose signed JSON holds more than the registry stores.
///
/// The release id is the digest of the signed manifest, but the registry
/// stores and serves the parsed [`ReleaseManifest`]. A node hashes the
/// served manifest and refuses a mismatch, so a field this registry does not
/// know would make the release uninstallable. Refuse it at publish instead.
pub(super) fn validate_manifest_round_trip(
    manifest: &ReleaseManifest,
    signed: &Value,
) -> Result<(), String> {
    let stored =
        serde_json::to_value(manifest).map_err(|e| format!("manifest does not serialize: {e}"))?;
    if stored == *signed {
        Ok(())
    } else {
        Err("manifest carries fields or values this registry does not store".to_string())
    }
}

/// Refuse a new release when its publisher key already published
/// [`PUBLISH_RATE_LIMIT`] releases in the last [`PUBLISH_RATE_WINDOW_SECS`].
///
/// Counts the in-memory release store, so a registry instance counts the
/// releases it loaded at start and the ones it accepted since. A record
/// whose `published_at` does not parse does not count.
pub(super) fn check_publish_rate(
    releases: &HashMap<String, ReleaseRecord>,
    publisher_dev_pubkey: &str,
    clock: &dyn crate::clock::Clock,
) -> Result<(), ReleaseError> {
    let recent = releases
        .values()
        .filter(|record| record.publisher_dev_pubkey == publisher_dev_pubkey)
        .filter(|record| {
            clock
                .whole_seconds_since_rfc3339(&record.published_at, None)
                .is_ok_and(|age| age < PUBLISH_RATE_WINDOW_SECS)
        })
        .count();
    if recent >= PUBLISH_RATE_LIMIT {
        Err(ReleaseError::PublishRateLimited)
    } else {
        Ok(())
    }
}
