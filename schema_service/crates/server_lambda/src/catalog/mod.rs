//! Schemas publishers push for listed apps.
//!
//! `POST /v1/schemas` and `POST /v1/advice/schema` share one private,
//! conditional S3 object. Vocabulary advice does not read that object.
//! The retired feed object `advisory-catalog-v1/catalog.json` is not read,
//! written, or converted.

mod listing;
mod push;
mod storage;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use app_identity_crypto::{verifying_key_from_base64, VerifyingKey};
use reqwest::Client;
use schema_service_core::advice::{CatalogSchema, SchemaAdviceRequest, SchemaAdviceResponse};
use schema_types::DeclarativeSchemaDefinition;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use storage::Store;

pub(crate) use push::{push as register_schemas, SCHEMAS_PATH};

const IMAGE_VERSION: u32 = 2;
const CACHE_REFRESH: Duration = Duration::from_secs(60);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Image {
    version: u32,
    apps: BTreeMap<String, AppRecord>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppRecord {
    source: String,
    sha: String,
    api_version: u32,
    registered_at: u64,
    definitions: Vec<DeclarativeSchemaDefinition>,
}

impl Image {
    fn empty() -> Self {
        Self {
            version: IMAGE_VERSION,
            apps: BTreeMap::new(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != IMAGE_VERSION {
            return Err("catalog storage uses an unsupported image version".into());
        }
        Ok(())
    }

    fn schema_rows(&self) -> Vec<CatalogSchema<'_>> {
        let mut rows = Vec::new();
        for (key, record) in &self.apps {
            let Some((app_id, channel)) = key.split_once('/') else {
                continue;
            };
            if app_id.is_empty() || channel.is_empty() {
                continue;
            }
            for definition in &record.definitions {
                rows.push(CatalogSchema {
                    definition,
                    app_id,
                    source: &record.source,
                    sha: &record.sha,
                    channel,
                    api_version: record.api_version,
                });
            }
        }
        rows
    }
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
        let Some(image) = image.filter(|_| read_ok) else {
            return unavailable();
        };
        reply.add_catalog_examples(request, image.schema_rows());
        json!({ "status": "current" })
    }

    async fn read_catalog(&self) -> (Option<Arc<Image>>, bool) {
        let mut cache = self.cached.lock().await;
        if cache
            .last_read
            .is_none_or(|last| last.elapsed() >= CACHE_REFRESH)
        {
            match self.store.read(&self.client, Duration::from_secs(1)).await {
                Ok(Some((image, _))) => {
                    cache.image = Some(Arc::new(image));
                    cache.read_ok = true;
                }
                Ok(None) => {
                    cache.image = Some(Arc::new(Image::empty()));
                    cache.read_ok = true;
                }
                Err(reason) => {
                    tracing::warn!(%reason, "schema_catalog_read_unavailable");
                    cache.read_ok = false;
                }
            }
            cache.last_read = Some(Instant::now());
        }
        (cache.image.clone(), cache.read_ok)
    }
}

pub(crate) fn unavailable() -> Value {
    json!({ "status": "unavailable" })
}

pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
