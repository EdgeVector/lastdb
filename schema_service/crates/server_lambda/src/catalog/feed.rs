use std::collections::BTreeMap;
use std::time::Duration;

use app_identity_crypto::{
    github_repo, hex_lower, interface_list_path, InterfaceList, LIST_CHANNELS, MAX_LIST_BYTES,
};
use futures::{stream, StreamExt};
use http::{header, StatusCode};
use schema_types::{DeclarativeSchemaDefinition, FieldValueType};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::storage::bounded_body;
use super::{now, Catalog, Image, ListState, ManifestRef, ManifestState};

const LIST_FLOOR: u32 = 21;
const INITIAL_API: u32 = 34;
const LOOK_AHEAD: u32 = 4;
const MAX_DISCOVERY_API: u32 = 256;
const MANIFEST_LIMIT: usize = 16;
const MAX_MANIFEST_BYTES: usize = 512 * 1024;
const RETRY_AFTER_SECONDS: u64 = 15 * 60;

#[derive(Debug, Default, Serialize)]
pub(super) struct Report {
    pub(super) list_errors: usize,
    pub(super) manifests_deferred: usize,
    manifests_rejected: usize,
    lists_changed: usize,
    lists_verified: usize,
    active_schema_listings: usize,
}

enum Read {
    Accepted(Vec<DeclarativeSchemaDefinition>),
    Rejected(String),
    Retry(String),
    RateLimited,
}

impl Catalog {
    pub(super) async fn refresh(&self) -> Result<Report, String> {
        let previous = self
            .store
            .read(&self.client, Duration::from_secs(8))
            .await?;
        let (mut image, etag) = previous.map_or_else(
            || (Image::default(), None),
            |(image, etag)| (image, Some(etag)),
        );
        let mut report = self.refresh_lists(&mut image).await;
        if image.lists.is_empty() {
            return Err("no signed public registry lists could be verified".into());
        }
        self.refresh_manifests(&mut image, &mut report).await;
        image.checked_at = now();
        image.complete = report.list_errors == 0 && report.manifests_deferred == 0;
        report.active_schema_listings = image.schemas().count();
        self.store
            .write(&self.client, &image, etag.as_deref())
            .await?;
        Ok(report)
    }

    async fn refresh_lists(&self, image: &mut Image) -> Report {
        let highest = image
            .lists
            .values()
            .map(|list| list.api_version)
            .max()
            .unwrap_or(INITIAL_API)
            .max(INITIAL_API);
        let first = LIST_FLOOR;
        let last = highest.saturating_add(LOOK_AHEAD).min(MAX_DISCOVERY_API);
        let keys = (first..=last)
            .flat_map(|api| LIST_CHANNELS.map(move |channel| (channel.to_owned(), api)));
        let prior = &image.lists;
        let observed: Vec<_> = stream::iter(keys)
            .map(|(channel, api)| {
                let key = interface_list_path(&channel, api);
                let previous = prior.get(&key);
                async move { (key, self.read_list(&channel, api, previous).await) }
            })
            .buffer_unordered(8)
            .collect()
            .await;
        let mut report = Report::default();
        for (key, outcome) in observed {
            match outcome {
                Ok(Some(list)) => {
                    image.lists.insert(key, list);
                    report.lists_changed += 1;
                    report.lists_verified += 1;
                }
                Ok(None) if image.lists.contains_key(&key) => report.lists_verified += 1,
                Ok(None) => {}
                Err(reason) => {
                    tracing::warn!(list = %key, %reason, "schema_catalog_registry_list_skipped");
                    report.list_errors += 1;
                }
            }
        }
        report
    }

    async fn read_list(
        &self,
        channel: &str,
        api: u32,
        previous: Option<&ListState>,
    ) -> Result<Option<ListState>, String> {
        let url = format!(
            "{}/{}",
            self.registry_base,
            interface_list_path(channel, api)
        );
        let mut request = self.client.get(&url);
        if let Some(etag) = previous.and_then(|list| list.etag.as_deref()) {
            request = request.header(header::IF_NONE_MATCH, etag);
        }
        let response = request.send().await.map_err(|error| error.to_string())?;
        match response.status() {
            StatusCode::NOT_MODIFIED if previous.is_some() => return Ok(None),
            StatusCode::FORBIDDEN | StatusCode::NOT_FOUND if previous.is_none() => return Ok(None),
            StatusCode::OK => {}
            status => return Err(format!("registry list returned HTTP {status}")),
        }
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|etag| etag.to_str().ok())
            .map(str::to_owned);
        let bytes = bounded_body(response, MAX_LIST_BYTES).await?;
        let signature = self
            .client
            .get(format!("{url}.sig"))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| error.to_string())?;
        let signature = bounded_body(signature, 4096).await?;
        let signed = InterfaceList::load(&bytes, &signature, &self.trust, channel, api)?;
        let sha256 = hex_lower(&Sha256::digest(&bytes));
        if previous.is_some_and(|prior| {
            signed.seq < prior.seq || signed.seq == prior.seq && prior.sha256 != sha256
        }) {
            return Err("registry list sequence regressed or changed its signed bytes".into());
        }
        let entries = signed
            .apps
            .into_iter()
            .filter(|entry| {
                entry
                    .audience
                    .as_deref()
                    .is_none_or(|audience| audience == "public")
            })
            .map(|entry| ManifestRef {
                app_id: entry.app_id,
                source: entry.source,
                sha: entry.sha,
            })
            .collect();
        Ok(Some(ListState {
            etag,
            channel: signed.channel,
            api_version: api,
            seq: signed.seq,
            sha256,
            entries,
        }))
    }

    async fn refresh_manifests(&self, image: &mut Image, report: &mut Report) {
        let listed: BTreeMap<_, _> = image
            .lists
            .values()
            .flat_map(|list| &list.entries)
            .map(|entry| (entry.key(), entry))
            .collect();
        let time = now();
        let unread = listed
            .iter()
            .filter(|(key, _)| match image.manifests.get(*key) {
                None => true,
                Some(ManifestState::Retry { after }) => *after <= time,
                _ => false,
            })
            .take(MANIFEST_LIMIT)
            .map(|(key, entry)| (key.clone(), (**entry).clone()))
            .collect::<Vec<_>>();
        for (key, entry) in unread {
            let stored = match self.read_manifest(&entry).await {
                Read::Accepted(definitions) => ManifestState::Accepted(definitions),
                Read::Rejected(reason) => {
                    tracing::warn!(app_id = %entry.app_id, sha = %entry.sha, %reason, "schema_catalog_manifest_skipped");
                    report.manifests_rejected += 1;
                    ManifestState::Rejected
                }
                Read::Retry(reason) => {
                    tracing::warn!(app_id = %entry.app_id, sha = %entry.sha, %reason, "schema_catalog_manifest_deferred");
                    ManifestState::Retry {
                        after: time + RETRY_AFTER_SECONDS,
                    }
                }
                Read::RateLimited => break,
            };
            image.manifests.insert(key, stored);
        }
        report.manifests_deferred = listed
            .keys()
            .filter(|key| {
                matches!(
                    image.manifests.get(*key),
                    None | Some(ManifestState::Retry { .. })
                )
            })
            .count();
    }

    async fn read_manifest(&self, entry: &ManifestRef) -> Read {
        let Some((owner, repo)) = github_repo(&entry.source)
            .filter(|(owner, repo)| !matches!(*owner, "." | "..") && !matches!(*repo, "." | ".."))
        else {
            return Read::Rejected("invalid public GitHub repository".into());
        };
        // trace-egress: skip-3p (public manifest fetch, no credentials)
        let url = format!("https://api.github.com/repos/{owner}/{repo}/contents/lastdb-app.json");
        let response = match self
            .client
            .get(url)
            .query(&[("ref", &entry.sha)])
            .header(header::ACCEPT, "application/vnd.github.raw+json")
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => return Read::Retry(error.to_string()),
        };
        match response.status() {
            StatusCode::OK => {}
            StatusCode::NOT_FOUND => {
                return Read::Rejected("public pinned app manifest not found".into())
            }
            StatusCode::TOO_MANY_REQUESTS => return Read::RateLimited,
            StatusCode::FORBIDDEN
                if response.headers().contains_key(header::RETRY_AFTER)
                    || response
                        .headers()
                        .get("x-ratelimit-remaining")
                        .is_some_and(|v| v == "0") =>
            {
                return Read::RateLimited;
            }
            StatusCode::FORBIDDEN => {
                return Read::Rejected("pinned app manifest is not public".into())
            }
            status => return Read::Retry(format!("manifest request returned HTTP {status}")),
        }
        let bytes = match bounded_body(response, MAX_MANIFEST_BYTES).await {
            Ok(bytes) => bytes,
            Err(reason) => return Read::Retry(reason),
        };
        match parse_manifest(entry, &bytes) {
            Ok(definitions) => Read::Accepted(definitions),
            Err(reason) => Read::Rejected(reason),
        }
    }
}

fn parse_manifest(
    entry: &ManifestRef,
    bytes: &[u8],
) -> Result<Vec<DeclarativeSchemaDefinition>, String> {
    let manifest: Value =
        serde_json::from_slice(bytes).map_err(|_| "manifest is not valid JSON")?;
    if manifest["app_id"].as_str() != Some(&entry.app_id) {
        return Err("manifest app_id differs from the signed listing".into());
    }
    if manifest
        .get("audience")
        .is_some_and(|audience| audience.as_str() != Some("public"))
    {
        return Err("manifest must be public".into());
    }
    let definitions = manifest["schemas"]
        .as_array()
        .ok_or("manifest does not list schemas")?;
    definitions
        .iter()
        .map(|declaration| {
            let mut value = declaration.clone();
            if let Some(object) = value.as_object_mut() {
                object.remove("retention");
            }
            let mut schema: DeclarativeSchemaDefinition = serde_json::from_value(value)
                .map_err(|_| "manifest contains an invalid schema declaration")?;
            schema.normalize_wire();
            schema
                .normalize_owned_declaration("app_id", &entry.app_id)
                .map_err(|_| "manifest contains a schema that LastDB would refuse")?;
            if schema.field_types.values().any(contains_any) {
                return Err("manifest contains a non-concrete Any type".into());
            }
            schema.field_hashes.clear();
            schema.ensure_field_hashes();
            Ok(schema)
        })
        .collect()
}

fn contains_any(kind: &FieldValueType) -> bool {
    match kind {
        FieldValueType::Any => true,
        FieldValueType::Array(inner) | FieldValueType::Map(inner) => contains_any(inner),
        FieldValueType::Object(fields) => fields.values().any(contains_any),
        FieldValueType::OneOf(choices) => choices.iter().any(contains_any),
        _ => false,
    }
}
