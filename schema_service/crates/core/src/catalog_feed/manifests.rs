//! Read `lastdb-app.json` once for each listed `(app_id, sha)` and normalize
//! its schemas with the node's own code.
//!
//! `schema_types::manifest_schema_definition` parses an entry as the node's
//! declare route does (`retention` dropped, as `lastdb app check` drops it),
//! and `owned_catalog_definition` is the node's owned normalization with
//! namespace = `app_id`. The catalog therefore computes the identity hash and
//! field hashes every node computes when it installs the app.

use std::collections::HashSet;

use app_identity_crypto::github_repo;
use schema_types::{manifest_schema_definition, owned_catalog_definition, Schema};
use serde_json::Value;

use super::run::FeedRun;
use super::{
    manifest_key, CatalogFeedState, FeedListEntry, FeedManifestState, ManifestFetch,
    MAX_MANIFESTS_PER_RUN,
};

/// The outcome of one manifest read.
enum Read {
    /// Final for this commit: its definitions, or why it is skipped.
    Done(Result<Vec<Schema>, String>),
    /// Transient: the commit stays unread for the next run.
    Later,
    /// The host refused for its rate limit: stop reading this run.
    StopRun,
}

/// Every listed `(app_id, sha)` the feed has not read, once each, in list order.
fn unread_entries(state: &CatalogFeedState) -> Vec<FeedListEntry> {
    let mut keys = HashSet::new();
    let mut unread = Vec::new();
    for entry in state.lists.values().flat_map(|list| &list.entries) {
        let key = manifest_key(&entry.app_id, &entry.sha);
        if !state.manifests.contains_key(&key) && keys.insert(key) {
            unread.push(entry.clone());
        }
    }
    unread
}

/// The catalog definitions of one manifest of `app_id`. Any entry that does
/// not normalize skips the whole manifest, as `lastdb app check` fails it.
///
/// # Errors
/// A different `app_id`, a `schemas` member that is not a list, or an
/// entry the node would refuse.
pub(super) fn parse_manifest(app_id: &str, manifest: &Value) -> Result<Vec<Schema>, String> {
    let declared = manifest.get("app_id").and_then(Value::as_str);
    if declared != Some(app_id) {
        return Err(format!(
            "lastdb-app.json names app_id '{}', not the listed '{app_id}'",
            declared.unwrap_or("<none>")
        ));
    }
    let entries = match manifest.get("schemas") {
        None => return Ok(Vec::new()),
        Some(Value::Array(entries)) => entries,
        Some(_) => return Err("lastdb-app.json `schemas` is not a list".into()),
    };
    entries
        .iter()
        .map(|entry| {
            let label = entry
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>");
            let schema = manifest_schema_definition(entry).map_err(|error| {
                format!("manifest schema '{label}' is not a schema declaration: {error}")
            })?;
            owned_catalog_definition(app_id, schema)
                .map_err(|error| format!("manifest schema '{label}' cannot be declared: {error}"))
        })
        .collect()
}

impl FeedRun<'_> {
    /// Read the manifests of newly listed commits, up to the per-run cap.
    /// A transient failure leaves that commit for the next run; a rate limit
    /// ends the reads for this run. Returns the definitions read, in read
    /// order.
    pub(super) async fn read_manifests(&mut self, state: &mut CatalogFeedState) -> Vec<Schema> {
        let mut definitions = Vec::new();
        let unread = unread_entries(state);
        let mut pending = unread.len();
        for entry in unread.into_iter().take(MAX_MANIFESTS_PER_RUN) {
            let result = match self.read_manifest(&entry).await {
                Read::Done(result) => result,
                Read::Later => continue,
                Read::StopRun => break,
            };
            pending -= 1;
            let manifest = self.record_manifest(&entry, result, &mut definitions);
            state
                .manifests
                .insert(manifest_key(&entry.app_id, &entry.sha), manifest);
        }
        self.report.manifests_pending = pending;
        definitions
    }

    async fn read_manifest(&self, entry: &FeedListEntry) -> Read {
        let Some((owner, repo)) = github_repo(&entry.source) else {
            return Read::Done(Err(format!(
                "source {} is not a GitHub repository",
                entry.source
            )));
        };
        match self.source.fetch_manifest(owner, repo, &entry.sha).await {
            Ok(ManifestFetch::Found(manifest)) => {
                Read::Done(parse_manifest(&entry.app_id, &manifest))
            }
            Ok(ManifestFetch::Missing(reason)) => Read::Done(Err(reason)),
            Ok(ManifestFetch::RateLimited) => {
                tracing::warn!(
                    target: "schema_service::catalog_feed",
                    "manifest host rate limit; the next run reads the rest"
                );
                Read::StopRun
            }
            Err(error) => {
                tracing::warn!(
                    target: "schema_service::catalog_feed",
                    app_id = %entry.app_id,
                    sha = %entry.sha,
                    error = %error,
                    "manifest read failed; the next run tries again"
                );
                Read::Later
            }
        }
    }

    /// Record the outcome of one manifest. A skipped manifest is logged with
    /// the app, the commit, and the error.
    fn record_manifest(
        &mut self,
        entry: &FeedListEntry,
        result: Result<Vec<Schema>, String>,
        definitions: &mut Vec<Schema>,
    ) -> FeedManifestState {
        let label = manifest_key(&entry.app_id, &entry.sha);
        let (identity_hashes, error) = match result {
            Ok(schemas) => {
                let mut hashes = Vec::new();
                for schema in schemas {
                    let Some(identity) = schema.identity_hash.clone() else {
                        continue;
                    };
                    if !hashes.contains(&identity) {
                        hashes.push(identity);
                    }
                    definitions.push(schema);
                }
                self.report.manifests_read.push(label);
                (hashes, None)
            }
            Err(error) => {
                tracing::warn!(
                    target: "schema_service::catalog_feed",
                    app_id = %entry.app_id,
                    sha = %entry.sha,
                    error = %error,
                    "manifest skipped"
                );
                self.report
                    .manifest_errors
                    .push(format!("{label}: {error}"));
                (Vec::new(), Some(error))
            }
        };
        FeedManifestState {
            app_id: entry.app_id.clone(),
            sha: entry.sha.clone(),
            source: entry.source.clone(),
            identity_hashes,
            error,
            read_at: self.now.clone(),
        }
    }
}
