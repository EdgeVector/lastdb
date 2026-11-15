use std::time::Duration;

use app_identity_crypto::{
    hex_lower, interface_list_path, verifying_key_from_base64, InterfaceList, VerifyingKey,
    LIST_CHANNELS, MAX_LIST_BYTES, REGISTRY_LIST_PUBKEY_B64,
};
use futures::{stream, StreamExt};
use http::{header, StatusCode};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::storage::bounded_body;
use super::{now, Catalog, Image, ListState, ManifestRef};

const LIST_FLOOR: u32 = 21;
const INITIAL_API: u32 = 34;
const LOOK_AHEAD: u32 = 4;
const MAX_DISCOVERY_API: u32 = 256;

#[derive(Debug, Default, Serialize)]
pub(super) struct Report {
    pub(super) list_errors: usize,
    pub(super) manifests_deferred: usize,
    pub(super) manifests_rejected: usize,
    lists_changed: usize,
    lists_verified: usize,
    active_schema_listings: usize,
}

impl Catalog {
    pub(super) async fn refresh(&self) -> Result<Report, String> {
        let trust = verifying_key_from_base64(REGISTRY_LIST_PUBKEY_B64)
            .map_err(|_| "invalid pinned registry trust key")?;
        let previous = self
            .store
            .read(&self.client, Duration::from_secs(8))
            .await?;
        let (mut image, etag) = previous.map_or_else(
            || (Image::default(), None),
            |(image, etag)| (image, Some(etag)),
        );
        let mut report = self.refresh_lists(&mut image, &trust).await;
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

    async fn refresh_lists(&self, image: &mut Image, trust: &VerifyingKey) -> Report {
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
                async move { (key, self.read_list(&channel, api, previous, trust).await) }
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
        trust: &VerifyingKey,
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
        let signed = InterfaceList::load(&bytes, &signature, trust, channel, api)?;
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
}
