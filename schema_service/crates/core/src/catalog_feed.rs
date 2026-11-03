//! The catalog feed (`design-lastdb-advisory-schema-service` section 8).
//!
//! On a schedule the feed reads the app registry's signed interface lists by
//! key, fetches `lastdb-app.json` once for each newly listed `(app_id, sha)`,
//! normalizes its schemas with the node's own code, and upserts one catalog
//! record per identity hash with its provenance. Only the feed and the
//! reference seeds write the catalog (decision A2). An app that leaves every
//! list keeps its records, marked `listed: false` (decision A6).
//!
//! The records live in their own image (`catalog_feed.json`), not in the
//! schema registry. A registry schema must match the canonical vocabulary at
//! every cold start, and an app declaration need not. The feed offers each
//! new field name to the vocabulary through the admission policy the seeds
//! use, and skips a field that admission refuses.

use std::collections::BTreeMap;

use async_trait::async_trait;
use schema_types::{FoldDbError, Schema};
use serde::{Deserialize, Serialize};

mod lists;
mod manifests;
mod records;
mod run;

pub use app_identity_crypto::{interface_list_path, MAX_LIST_BYTES};

/// The lowest node API version whose lists the feed reads.
pub const MIN_LIST_API_VERSION: u32 = 21;
/// How many API versions above the highest list seen the feed probes.
pub const LIST_API_PROBE_AHEAD: u32 = 2;
/// The most manifests one run reads. The rest wait for the next run.
pub const MAX_MANIFESTS_PER_RUN: usize = 20;
/// The largest `lastdb-app.json` the feed reads, in bytes.
pub const MAX_MANIFEST_BYTES: usize = 1_048_576;
/// The image grammar this build reads and writes.
pub const CATALOG_FEED_IMAGE_VERSION: u32 = 1;
/// Error prefix when another run wrote the image first.
pub const CATALOG_FEED_CONFLICT: &str = "catalog_feed_write_conflict";

/// Where a catalog record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogOrigin {
    /// A `lastdb-app.json` schema of a listed app, read by this feed.
    Registry,
    /// A reference seed (built-in or Schema.org). Seeds keep their registry
    /// entries and their `source` marker; the feed never writes one.
    Seed,
}

/// One place a definition is listed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogListing {
    pub channel: String,
    pub api_version: u32,
    pub sha: String,
    pub source: String,
}

/// One catalog record, keyed by the node-computed identity hash.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogRecord {
    pub identity_hash: String,
    /// The definition exactly as the node normalizes the declaration: name
    /// `<app_id>/<Name>`, `owner_app_id`, identity hash, and field hashes
    /// computed from the declaration alone.
    pub definition: Schema,
    pub owner_app_id: String,
    /// The current lists that carry this definition: each listing once, in
    /// list order.
    pub listings: Vec<CatalogListing>,
    /// True while at least one current list carries the definition.
    pub listed: bool,
    pub first_seen: String,
    /// The last run that read a changed list carrying this definition.
    pub last_seen: String,
    pub origin: CatalogOrigin,
}

/// One public entry of a verified list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedListEntry {
    pub app_id: String,
    pub sha: String,
    pub source: String,
}

/// The last verified state of one list key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedListState {
    pub channel: String,
    pub api_version: u32,
    /// The ETag of the verified bytes, sent as `If-None-Match`.
    pub etag: Option<String>,
    pub seq: u64,
    pub payload_sha256: String,
    pub generated_at: String,
    pub fetched_at: String,
    /// Public entries only. A `private` audience entry is skipped.
    pub entries: Vec<FeedListEntry>,
}

/// The outcome of reading one `(app_id, sha)` manifest. Each pair is read once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedManifestState {
    pub app_id: String,
    pub sha: String,
    pub source: String,
    pub identity_hashes: Vec<String>,
    /// Why the manifest was skipped, when it was.
    pub error: Option<String>,
    pub read_at: String,
}

/// What the feed has read: list keys and manifests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogFeedState {
    /// Keyed by the list path, `<channel>/api/<api_version>.json`.
    pub lists: BTreeMap<String, FeedListState>,
    /// Keyed by `<app_id>@<sha>`.
    pub manifests: BTreeMap<String, FeedManifestState>,
}

/// The stored feed image: state and records, written as one object.
///
/// The backend returns the image with an opaque version tag (an S3 ETag) and
/// writes it only while the stored version is still that tag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogFeedImage {
    pub version: u32,
    /// Incremented on every write, for readers of the image.
    pub revision: u64,
    pub state: CatalogFeedState,
    /// Keyed by identity hash.
    pub records: BTreeMap<String, CatalogRecord>,
}

impl Default for CatalogFeedImage {
    fn default() -> Self {
        Self {
            version: CATALOG_FEED_IMAGE_VERSION,
            revision: 0,
            state: CatalogFeedState::default(),
            records: BTreeMap::new(),
        }
    }
}

/// The manifest key of one listed commit.
#[must_use]
pub fn manifest_key(app_id: &str, sha: &str) -> String {
    format!("{app_id}@{sha}")
}

/// The error for a write that lost to another run.
#[must_use]
pub fn catalog_feed_conflict() -> FoldDbError {
    FoldDbError::Config(format!(
        "{CATALOG_FEED_CONFLICT}: another run wrote the catalog feed image; the next run starts from it"
    ))
}

/// One list read.
#[derive(Debug)]
pub enum ListFetch {
    /// HTTP 304: the list has not changed since the stored ETag.
    NotModified,
    /// No list at this key (HTTP 403 or 404 from the anonymous bucket).
    Absent,
    /// The list bytes, its detached signature, and the list ETag.
    Fetched {
        body: Vec<u8>,
        signature: Vec<u8>,
        etag: Option<String>,
    },
}

/// One manifest read.
#[derive(Debug)]
pub enum ManifestFetch {
    /// `lastdb-app.json` at the commit, parsed as JSON. The source parses
    /// it with the JSON reader the Lambda already has for request bodies.
    Found(serde_json::Value),
    /// The commit has no usable root `lastdb-app.json`: absent, larger
    /// than [`MAX_MANIFEST_BYTES`], not JSON, or the repository is gone.
    /// Final for this commit.
    Missing(String),
    /// The host refused for its rate limit. The run reads no more manifests.
    RateLimited,
}

/// The network side of the feed. The Lambda shell implements it over HTTPS.
#[async_trait]
pub trait CatalogFeedSource: Send + Sync {
    /// Read one list by key, with `If-None-Match` when `etag` is set.
    ///
    /// # Errors
    /// A transient failure. The run keeps the previous state of the key.
    async fn fetch_list(
        &self,
        channel: &str,
        api_version: u32,
        etag: Option<&str>,
    ) -> Result<ListFetch, String>;

    /// Read `lastdb-app.json` at `sha` in the public GitHub repository
    /// `owner/repo`.
    ///
    /// # Errors
    /// A transient failure. The commit stays unread for the next run, and
    /// the run goes on to the next commit.
    async fn fetch_manifest(
        &self,
        owner: &str,
        repo: &str,
        sha: &str,
    ) -> Result<ManifestFetch, String>;
}

/// What one run did.
#[derive(Debug, Default, Serialize)]
pub struct CatalogFeedReport {
    pub lists_changed: Vec<String>,
    pub lists_unchanged: usize,
    /// List keys the run could not use, with the reason. Each keeps its
    /// previous state.
    pub lists_failed: Vec<String>,
    pub manifests_read: Vec<String>,
    /// Manifests skipped, as `<app_id>@<sha>: <error>`.
    pub manifest_errors: Vec<String>,
    /// Listed commits left unread for the next run.
    pub manifests_pending: usize,
    pub records_added: usize,
    pub records_updated: usize,
    pub records_unlisted: usize,
    pub records_total: usize,
    pub canonical_fields_added: Vec<String>,
    pub canonical_fields_skipped: usize,
    pub embeddings_computed: usize,
    pub image_written: bool,
}
