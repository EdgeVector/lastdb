//! Optional registry-backed examples. Scheduled writes and HTTP reads share a
//! private, conditional S3 image. Vocabulary advice remains independent.

mod feed;
mod storage;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};
use lambda_http::request::RequestContext;
use lambda_http::{Body, Error, Request, Response};
use reqwest::Client;
use schema_service_core::advice::{CatalogSchema, SchemaAdviceRequest, SchemaAdviceResponse};
use schema_types::DeclarativeSchemaDefinition;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::json_response;
use storage::Store;

pub(crate) const FEED_PATH: &str = "/v1/internal/catalog-feed";
const SCHEDULE_API_ID: &str = "catalog-feed-schedule";
const IMAGE_VERSION: u32 = 1;
const CACHE_REFRESH: Duration = Duration::from_secs(60);
const FEED_TIMEOUT: Duration = Duration::from_secs(45);
const STALE_AFTER_SECONDS: u64 = 15 * 60;

#[derive(Serialize, Deserialize)]
struct Image {
    version: u32,
    checked_at: u64,
    complete: bool,
    lists: BTreeMap<String, ListState>,
    manifests: BTreeMap<String, ManifestState>,
}

impl Default for Image {
    fn default() -> Self {
        Self {
            version: IMAGE_VERSION,
            checked_at: 0,
            complete: false,
            lists: BTreeMap::new(),
            manifests: BTreeMap::new(),
        }
    }
}

impl Image {
    fn validate(&self) -> Result<(), String> {
        if self.version != IMAGE_VERSION {
            return Err("catalog storage uses an unsupported image version".into());
        }
        Ok(())
    }

    fn schemas(&self) -> impl Iterator<Item = CatalogSchema<'_>> {
        self.lists.values().flat_map(move |list| {
            list.entries.iter().flat_map(move |entry| {
                let stored = self.manifests.get(&entry.key());
                let definitions = match stored {
                    Some(ManifestState::Accepted(definitions)) => definitions.as_slice(),
                    _ => &[],
                };
                definitions.iter().map(move |definition| CatalogSchema {
                    definition,
                    app_id: &entry.app_id,
                    source: &entry.source,
                    sha: &entry.sha,
                    channel: &list.channel,
                    api_version: list.api_version,
                })
            })
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ManifestRef {
    app_id: String,
    source: String,
    sha: String,
}

impl ManifestRef {
    fn key(&self) -> String {
        format!("{}@{}:{}", self.app_id, self.sha, self.source)
    }
}

#[derive(Serialize, Deserialize)]
struct ListState {
    etag: Option<String>,
    channel: String,
    api_version: u32,
    seq: u64,
    sha256: String,
    entries: Vec<ManifestRef>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
enum ManifestState {
    Accepted(Vec<DeclarativeSchemaDefinition>),
    Rejected,
    Retry { after: u64 },
}

#[derive(Default)]
struct ReadCache {
    last_read: Option<Instant>,
    image: Option<Arc<Image>>,
    read_ok: bool,
}

pub(crate) struct Catalog {
    client: Client,
    store: Store,
    registry_base: String,
    trust: VerifyingKey,
    cached: Mutex<ReadCache>,
}

impl Catalog {
    pub(crate) fn from_environment() -> Result<Option<Self>, String> {
        let Ok(bucket) = std::env::var("SCHEMA_CATALOG_BUCKET") else {
            return Ok(None);
        };
        let environment = std::env::var("ENVIRONMENT").map_err(|_| "missing schema environment")?;
        if !matches!(environment.as_str(), "dev" | "prod") {
            return Err("catalog environment must be dev or prod".into());
        }
        let profile: Value =
            serde_json::from_str(include_str!("../../../../../lastdb_node/environments.json"))
                .map_err(|_| "invalid built-in registry configuration")?;
        let env_profile = &profile["environments"][environment.as_str()];
        let registry_base = env_profile["registry_index"]
            .as_str()
            .filter(|value| value.starts_with("https://"))
            .ok_or("missing HTTPS registry list base")?
            .trim_end_matches('/')
            .to_owned();
        let list_key = env_profile["registry_list_key"]
            .as_str()
            .ok_or("missing registry list trust key")?;
        let trust =
            verifying_key_from_base64(list_key).map_err(|_| "invalid pinned registry trust key")?;
        let region = std::env::var("AWS_REGION").map_err(|_| "missing catalog storage region")?;
        let client = Client::builder()
            .timeout(Duration::from_secs(8))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .user_agent("lastdb-schema-service-catalog")
            .build()
            .map_err(|_| "could not initialize catalog HTTP client")?;
        Ok(Some(Self {
            client,
            store: Store::new(&bucket, region)?,
            registry_base,
            trust,
            cached: Mutex::new(ReadCache::default()),
        }))
    }

    pub(crate) async fn examples(
        &self,
        request: &SchemaAdviceRequest,
        reply: &mut SchemaAdviceResponse,
    ) -> Value {
        let (image, read_ok) = self.read_catalog().await;
        let Some(image) = image else {
            return unavailable();
        };
        reply.add_catalog_examples(request, image.schemas());
        let status = if read_ok
            && image.complete
            && now().saturating_sub(image.checked_at) <= STALE_AFTER_SECONDS
        {
            "current"
        } else {
            "stale"
        };
        json!({"status": status, "checked_at": image.checked_at})
    }

    async fn read_catalog(&self) -> (Option<Arc<Image>>, bool) {
        let mut cache = self.cached.lock().await;
        if cache
            .last_read
            .is_none_or(|last| last.elapsed() >= CACHE_REFRESH)
        {
            cache.read_ok = false;
            match self.store.read(&self.client, Duration::from_secs(1)).await {
                Ok(Some((image, _))) => {
                    cache.image = Some(Arc::new(image));
                    cache.read_ok = true;
                }
                Ok(None) => {}
                Err(reason) => tracing::warn!(%reason, "schema_catalog_read_unavailable"),
            }
            cache.last_read = Some(Instant::now());
        }
        (cache.image.clone(), cache.read_ok)
    }
}

pub(crate) fn unavailable() -> Value {
    json!({"status": "unavailable"})
}

pub(crate) async fn scheduled(
    catalog: Option<&Catalog>,
    event: &Request,
) -> Result<Response<Body>, Error> {
    let from_schedule = matches!(
        event.extensions().get::<RequestContext>(),
        Some(RequestContext::ApiGatewayV2(context))
            if context.apiid.as_deref() == Some(SCHEDULE_API_ID)
    );
    if !from_schedule {
        return json_response(404, &json!({"error": "Not found"}));
    }
    let Some(catalog) = catalog else {
        tracing::error!("schema_catalog_feed_failure: catalog is not configured");
        return json_response(503, &unavailable());
    };
    let refreshed = tokio::time::timeout(FEED_TIMEOUT, catalog.refresh())
        .await
        .unwrap_or_else(|_| Err("catalog refresh exceeded its time limit".into()));
    match refreshed {
        Ok(report) => {
            if report.list_errors > 0 || report.manifests_deferred > 0 {
                tracing::warn!(?report, "schema_catalog_feed_failure: incomplete refresh");
            } else {
                tracing::info!(?report, "schema_catalog_feed_success");
            }
            json_response(200, &json!(report))
        }
        Err(reason) => {
            tracing::error!(%reason, "schema_catalog_feed_failure");
            json_response(503, &unavailable())
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
