//! Groups of parsed `lastdb app` CLI fields that travel together, so the
//! handlers take one value instead of a long positional list.

use super::*;

/// Where a registry-writing command publishes and with which credentials.
pub(crate) struct RegistryFlags {
    pub(crate) env: Option<String>,
    pub(crate) schema_url: Option<String>,
    pub(crate) api_url: Option<String>,
    pub(crate) key_file: Option<PathBuf>,
    pub(crate) api_key: Option<String>,
}

/// What `release-publish` signs and uploads.
pub(crate) struct ReleasePublishInput<'a> {
    pub(crate) manifest_path: &'a Path,
    pub(crate) app_uuid: &'a str,
    pub(crate) source_commit: &'a str,
    pub(crate) artifact: &'a Path,
    pub(crate) artifact_url: &'a str,
}

/// Where `install` / `upgrade` resolve an app from: the live registry service
/// (`env`, `schema_url`) or the signed public index.
pub(crate) struct RegistrySource {
    pub(crate) env: Option<String>,
    pub(crate) schema_url: Option<String>,
    pub(crate) channel: Option<String>,
    pub(crate) index: Option<String>,
    pub(crate) trust_key: Option<String>,
    pub(crate) lastdb_version: Option<String>,
}

/// How `release-check` repeats and reports.
pub(crate) struct CheckRun {
    pub(crate) watch: bool,
    pub(crate) interval_secs: Option<u64>,
    pub(crate) cycles: Option<u64>,
    pub(crate) json: bool,
}
