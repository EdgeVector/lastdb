use std::collections::BTreeMap;
use std::time::Duration;

use app_identity_crypto::github_repo;
use http::{header, StatusCode};
use schema_types::{DeclarativeSchemaDefinition, FieldValueType};
use serde_json::Value;

use super::feed::Report;
use super::storage::bounded_body;
use super::{now, Catalog, Image, ManifestRef, ManifestState};

const MANIFEST_LIMIT: usize = 16;
const MAX_MANIFEST_BYTES: usize = 512 * 1024;
const RETRY_AFTER_SECONDS: u64 = 15 * 60;

enum Read {
    Accepted(Vec<DeclarativeSchemaDefinition>),
    Rejected(String),
    Retry(String),
    RateLimited,
}

impl Catalog {
    pub(super) async fn refresh_manifests(&self, image: &mut Image, report: &mut Report) {
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
