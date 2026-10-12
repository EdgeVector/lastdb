//! Resolve: pick the one proved compat row for this node.

use app_identity_crypto::VerifyingKey;
use serde::Serialize;

use super::location::IndexLocation;
use super::signing::{load_verified_index, CompatRow, IndexApp, RegistryIndex, TrustSource};
use super::TRUST_KEY_ENV;

/// What `lastdb app resolve` answers: the one proved row for this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolution {
    pub app_id: String,
    pub channel: String,
    pub source: String,
    pub lastdb_version: String,
    pub lastdb_version_source: String,
    pub app_version: String,
    pub sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
    pub proved_at: String,
    pub proof_run: String,
    pub index: String,
    pub trust: String,
}

/// Pick the newest compat row of `app_id` that names `lastdb_version`
/// exactly. Newest is by `proved_at`, then `app_version` as a tie-break.
///
/// # Errors
/// Names the app when it is absent, and lists the node builds the app was
/// proved with when none matches, so the operator sees the remedy.
pub fn pick_row<'a>(
    index: &'a RegistryIndex,
    app_id: &str,
    lastdb_version: &str,
) -> Result<(&'a IndexApp, &'a CompatRow), String> {
    let app = index
        .apps
        .iter()
        .find(|a| a.app_id == app_id)
        .ok_or_else(|| format!("app '{app_id}' is not in the {} index", index.channel))?;
    let mut matches: Vec<&CompatRow> = app
        .compat
        .iter()
        .filter(|row| row.lastdb_version == lastdb_version)
        .collect();
    if matches.is_empty() {
        let mut proved: Vec<&str> = app
            .compat
            .iter()
            .map(|r| r.lastdb_version.as_str())
            .collect();
        proved.sort_unstable();
        proved.dedup();
        return Err(format!(
            "no {} row for app '{app_id}' was proved with lastdb {lastdb_version} (proved with: {}); run `brew upgrade lastdb` or wait for the next proved set",
            index.channel,
            if proved.is_empty() {
                "none".to_string()
            } else {
                proved.join(", ")
            }
        ));
    }
    matches.sort_by(|a, b| {
        a.proved_at
            .cmp(&b.proved_at)
            .then_with(|| a.app_version.cmp(&b.app_version))
    });
    let row = matches
        .pop()
        .ok_or_else(|| "compat row selection produced no row".to_string())?;
    Ok((app, row))
}

/// Fetch, verify, and pick: the whole anonymous resolve in one call.
///
/// # Errors
/// Any fetch, signature, parse, or selection failure, with its cause.
pub async fn resolve(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
    trust_source: &TrustSource,
    app_id: &str,
    lastdb_version: &str,
    lastdb_version_source: &str,
) -> Result<Resolution, String> {
    let (index_bytes, sig_bytes) = location.fetch(channel).await?;
    let index = load_verified_index(trust, &index_bytes, &sig_bytes)?;
    if index.channel != channel {
        return Err(format!(
            "index at {} says channel '{}' but '{channel}' was requested",
            location.describe(channel),
            index.channel
        ));
    }
    let (app, row) = pick_row(&index, app_id, lastdb_version)?;
    Ok(Resolution {
        app_id: app.app_id.clone(),
        channel: channel.to_string(),
        source: app.source.clone(),
        lastdb_version: lastdb_version.to_string(),
        lastdb_version_source: lastdb_version_source.to_string(),
        app_version: row.app_version.clone(),
        sha: row.sha.clone(),
        artifact: row.artifact.clone(),
        proved_at: row.proved_at.clone(),
        proof_run: row.proof_run.clone(),
        index: location.describe(channel),
        trust: match trust_source {
            TrustSource::Pinned => "pinned".to_string(),
            TrustSource::Flag => "flag".to_string(),
            TrustSource::Env => format!("env:{TRUST_KEY_ENV}"),
        },
    })
}

/// Fetch and verify a whole channel for `list` / `info`.
///
/// # Errors
/// Any fetch, signature, or parse failure.
pub async fn fetch_verified(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
) -> Result<RegistryIndex, String> {
    let (index_bytes, sig_bytes) = location.fetch(channel).await?;
    load_verified_index(trust, &index_bytes, &sig_bytes)
}
