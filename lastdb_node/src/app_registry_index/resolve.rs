//! Resolve one app from the signed interface list for this node's API version.

use app_identity_crypto::VerifyingKey;
use serde::Serialize;

use super::location::IndexLocation;
use super::signing::TrustSource;
use super::TRUST_KEY_ENV;

/// Contract version ranges this node supports.
#[derive(Debug, Clone, Copy)]
pub struct ContractVersions {
    pub api_version: u32,
    pub min_supported_api_version: u32,
    pub outbox_format_version: u32,
    pub min_supported_outbox_format_version: u32,
}

/// The running LastDB version and how it was determined.
#[derive(Debug, Clone)]
pub struct NodeVersion<'a> {
    pub version: &'a str,
    pub source: &'a str,
}

/// What `lastdb app list APP` answers: the one proved row for this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolution {
    #[serde(skip)]
    pub list_entry: Option<super::ListEntry>,
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

/// Fetch the signed interface list and select one app.
///
/// # Errors
/// Any fetch, signature, parse, or selection failure, with its cause.
pub async fn resolve(
    location: &IndexLocation,
    channel: &str,
    trust: &VerifyingKey,
    trust_source: &TrustSource,
    app_id: &str,
    node_version: &NodeVersion<'_>,
    versions: &ContractVersions,
) -> Result<Resolution, String> {
    let index = super::fetch_list(location, channel, versions.api_version, trust).await?;
    let entry = index
        .apps
        .into_iter()
        .find(|entry| {
            entry.app_id == app_id
                && entry
                    .outbox_format_version
                    .is_none_or(|v| v == versions.outbox_format_version)
        })
        .ok_or_else(|| {
            format!(
                "no {channel} app '{app_id}' was proved with lastdb API {} (build {})",
                versions.api_version, node_version.version
            )
        })?;
    Ok(Resolution {
        list_entry: Some(entry.clone()),
        app_id: entry.app_id.clone(),
        channel: channel.to_string(),
        source: entry.source.clone(),
        lastdb_version: node_version.version.to_string(),
        lastdb_version_source: node_version.source.to_string(),
        app_version: entry.app_version,
        sha: entry.sha,
        artifact: None,
        proved_at: entry.published_at,
        proof_run: String::new(),
        index: location.describe_interface(channel, versions.api_version),
        trust: format!(
            "{}:{}",
            match trust_source {
                TrustSource::Pinned => "pinned".to_string(),
                TrustSource::Flag => "flag".to_string(),
                TrustSource::Env => format!("env:{TRUST_KEY_ENV}"),
            },
            app_identity_crypto::key_id(trust)
        ),
    })
}
