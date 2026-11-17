//! One signed registry list, read to check a schema push.

use std::time::Duration;

use app_identity_crypto::{interface_list_path, InterfaceList, ListEntry, MAX_LIST_BYTES};
use http::StatusCode;
use reqwest::Client;

use super::storage::bounded_body;
use super::Catalog;

const REGISTRY_TIMEOUT: Duration = Duration::from_secs(3);
const SIGNATURE_LIMIT: usize = 4096;

pub(super) enum ListLookup {
    NotListed,
    Unavailable,
}

enum Get {
    Bytes(Vec<u8>),
    Missing,
    Failed,
}

impl Catalog {
    pub(super) async fn listed_app(
        &self,
        channel: &str,
        api_version: u32,
        app_id: &str,
    ) -> Result<ListEntry, ListLookup> {
        let url = format!(
            "{}/{}",
            self.registry_base,
            interface_list_path(channel, api_version)
        );
        let bytes = match get_bytes(&self.client, &url, MAX_LIST_BYTES).await {
            Get::Bytes(bytes) => bytes,
            // The list bucket has no ListBucket grant, so a missing object is 403.
            Get::Missing => return Err(ListLookup::NotListed),
            Get::Failed => return Err(ListLookup::Unavailable),
        };
        // A missing signature means the list cannot be verified, not that the
        // app is absent.
        let signature = match get_bytes(&self.client, &format!("{url}.sig"), SIGNATURE_LIMIT).await
        {
            Get::Bytes(bytes) => bytes,
            Get::Missing | Get::Failed => return Err(ListLookup::Unavailable),
        };
        let list = InterfaceList::load(&bytes, &signature, &self.trust, channel, api_version)
            .map_err(|_| ListLookup::Unavailable)?;
        list.apps
            .into_iter()
            .find(|entry| entry.app_id == app_id)
            .ok_or(ListLookup::NotListed)
    }
}

async fn get_bytes(client: &Client, url: &str, limit: usize) -> Get {
    let Ok(response) = client.get(url).timeout(REGISTRY_TIMEOUT).send().await else {
        return Get::Failed;
    };
    match response.status() {
        StatusCode::OK => {}
        StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => return Get::Missing,
        _ => return Get::Failed,
    }
    match bounded_body(response, limit).await {
        Ok(bytes) => Get::Bytes(bytes),
        Err(_) => Get::Failed,
    }
}
