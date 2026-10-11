//! S3-backed implementation of `fold_db::schema_service::ExternalSchemaPersistence`.
//!
//! Layout (see `fold_db_node/docs/designs/schema_service_s3.md`):
//!
//! ```text
//! s3://{bucket}/
//! ├── schemas.json              # single JSON doc, every schema keyed by identity_hash
//! ├── canonical_fields.json     # every canonical field keyed by name
//! ├── near_misses.json          # Phase C shadow audit log, keyed by registration_id
//! ```
//!
//! ## Concurrency model
//!
//! **Domain blobs (schemas, canonical_fields)** use
//! read-modify-write with an `If-Match: {etag}` precondition on PUT.
//! If another Lambda wrote in between our GET and our PUT, the ETag
//! changes and S3 returns 412 Precondition Failed; we re-read, re-merge,
//! and retry. Bounded retry count prevents retry storms.
//!
//! ## On the append-only invariant
//!
//! The schema service is append-only — nothing is ever deleted. This
//! means a cache populated from a past `load_all_*` call is always
//! correct for the keys it holds; it can only be *incomplete*, never
//! *wrong*. Writes go directly to S3 via conditional PUT and S3 is the
//! sole arbiter of who wins a race. See the design doc for the full
//! discussion.

mod persistence_impl;
mod quota_store;
pub mod store_config;

pub use quota_store::DynamoDbSchemaMutationGateStore;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_dynamodb::error::SdkError as DdbSdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DdbClient;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use schema_service_core::declared_fields::DeclaredFieldRecord;
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_core::near_miss::NearMissRecord;
use schema_service_core::snapshot::AppRecord;
use schema_service_core::types::CanonicalField;
use schema_service_core::{
    SchemaMutationGateError, SchemaMutationGateQuotaStore, SchemaMutationGateStore,
};
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// Blob keys inside the bucket.
const SCHEMAS_KEY: &str = "schemas.json";
const CANONICAL_FIELDS_KEY: &str = "canonical_fields.json";
const NEAR_MISSES_KEY: &str = "near_misses.json";
const APPS_KEY: &str = "apps.json";
const DECLARED_FIELDS_KEY: &str = "declared_fields.json";

// Embeddings live in DynamoDB, not S3 — a single-blob approach would
// make every `add_schema` re-upload the full blob (~1.5KB × N entries).
// DDB's per-item R/W scales O(1) regardless of total count. Single
// table with `kind` as partition key lets one `Query(kind=...)` pull
// every embedding of one class in sorted, paginated order.
//
// Key: `kind` (S) = "descriptive_name" | "canonical_field"
// Sort key: `key` (S) = schema identity_hash | canonical field name
// Data: `embedding` (B) = raw f32 little-endian bytes (384 × 4 = 1536)
//        `model` (S) = embedding model version tag (reserved — always
//                      the same constant for now, but schema is ready
//                      for a future model bump to invalidate stale
//                      entries)
//        `updated_at` (N) = epoch seconds, for observability
const EMBEDDING_KIND_DESCRIPTIVE: &str = "descriptive_name";
const EMBEDDING_KIND_CANONICAL: &str = "canonical_field";
/// Model version tag. Bump when fastembed's model.onnx changes in a
/// way that invalidates existing embeddings; the load path can then
/// treat mismatches as missing and the write path can re-embed on
/// next access. Today's model: Qdrant-hosted all-MiniLM-L6-v2
/// (384-dim) via the fastembed Layer.
const EMBEDDING_MODEL_VERSION: &str = "qdrant-miniLM-v2-384d";

/// How many times we retry a domain-blob PUT when the ETag precondition
/// fails. At alpha write rates (< 1/sec per domain) actual contention
/// is ~zero; this cap exists to bound retry storms if the service ever
/// approaches the blob contention ceiling.
const MAX_RMW_RETRIES: usize = 10;

/// Versioned envelope for each domain blob. The `version` field is
/// reserved for future on-disk format migrations; items inside are
/// free-form per domain type.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DomainBlob<T> {
    version: u32,
    #[serde(default = "HashMap::new")]
    items: HashMap<String, T>,
}

impl<T> Default for DomainBlob<T> {
    fn default() -> Self {
        Self {
            version: 1,
            items: HashMap::new(),
        }
    }
}

/// Persist schema-service state in S3 domain blobs (`schemas.json`,
/// `canonical_fields.json`, `near_misses.json`, `apps.json`) and a
/// DynamoDB embeddings table. Implements `ExternalSchemaPersistence`
/// so the fold_db schema service can delegate all persistence without
/// knowing the split.
///
/// The name is historical — embeddings moved to DDB after the single
/// S3 blob-per-cache approach hit its growth ceiling (every write
/// re-uploads every prior embedding). Rename tracked as a follow-up.
pub struct S3BlobPersistence {
    client: S3Client,
    bucket: String,
    ddb: DdbClient,
    embeddings_table: String,
}

impl S3BlobPersistence {
    pub fn new(client: S3Client, bucket: String, ddb: DdbClient, embeddings_table: String) -> Self {
        Self {
            client,
            bucket,
            ddb,
            embeddings_table,
        }
    }

    /// Encode a `Vec<f32>` into a `Blob` for DynamoDB. Little-endian
    /// so byte order is portable across architectures. 384 floats
    /// × 4 bytes = 1536 bytes — well under DDB's 400KB item cap.
    fn encode_embedding(embedding: &[f32]) -> Blob {
        let mut bytes = Vec::with_capacity(embedding.len() * 4);
        for f in embedding {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        Blob::new(bytes)
    }

    /// Decode bytes from DynamoDB back into `Vec<f32>`. Returns
    /// `None` if the length isn't a multiple of 4 (corruption /
    /// truncation); caller treats `None` as missing.
    fn decode_embedding(bytes: &[u8]) -> Option<Vec<f32>> {
        if !bytes.len().is_multiple_of(4) {
            return None;
        }
        let mut result = Vec::with_capacity(bytes.len() / 4);
        for chunk in bytes.chunks_exact(4) {
            result.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Some(result)
    }

    /// Persist one embedding to DynamoDB. Conditional-put semantics
    /// aren't needed because each entry is idempotent: writing the
    /// same `(kind, key)` with a new embedding value overwrites
    /// atomically, which is what we want — the new write supersedes
    /// the old.
    async fn put_embedding(&self, kind: &str, key: &str, embedding: &[f32]) -> FoldDbResult<()> {
        let now = schema_types::clock::unix_secs();
        self.ddb
            .put_item()
            .table_name(&self.embeddings_table)
            .item("kind", AttributeValue::S(kind.to_string()))
            .item("key", AttributeValue::S(key.to_string()))
            .item(
                "embedding",
                AttributeValue::B(Self::encode_embedding(embedding)),
            )
            .item(
                "model",
                AttributeValue::S(EMBEDDING_MODEL_VERSION.to_string()),
            )
            .item("updated_at", AttributeValue::N(now.to_string()))
            .send()
            .await
            .map_err(|e| {
                FoldDbError::Config(format!(
                    "Failed to put embedding (kind={kind}, key={key}): {e}"
                ))
            })?;
        Ok(())
    }

    /// Load every embedding of one kind. Paginates Query results —
    /// each page is bounded at 1MB, at ~1.6KB per item that's ~600
    /// items per page, which covers current and ~10x scale in a
    /// single round-trip. Entries whose `model` doesn't match the
    /// current runtime version are skipped (treated as missing —
    /// next write-path embedding will replace them).
    async fn query_embeddings(&self, kind: &str) -> FoldDbResult<HashMap<String, Vec<f32>>> {
        let mut result = HashMap::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = self
                .ddb
                .query()
                .table_name(&self.embeddings_table)
                .key_condition_expression("#k = :kind")
                .expression_attribute_names("#k", "kind")
                .expression_attribute_values(":kind", AttributeValue::S(kind.to_string()));
            if let Some(start) = exclusive_start_key {
                req = req.set_exclusive_start_key(Some(start));
            }
            let resp = req.send().await.map_err(|e| {
                FoldDbError::Config(format!("Failed to query embeddings (kind={kind}): {e}"))
            })?;

            for item in resp.items() {
                // Skip entries with mismatched or missing model tag —
                // treat as stale, write path will repopulate on next
                // access.
                let model_ok = matches!(
                    item.get("model"),
                    Some(AttributeValue::S(s)) if s == EMBEDDING_MODEL_VERSION
                );
                if !model_ok {
                    continue;
                }
                let Some(AttributeValue::S(key)) = item.get("key") else {
                    continue;
                };
                let Some(AttributeValue::B(blob)) = item.get("embedding") else {
                    continue;
                };
                let Some(vec) = Self::decode_embedding(blob.as_ref()) else {
                    continue;
                };
                result.insert(key.clone(), vec);
            }

            match resp.last_evaluated_key {
                Some(k) if !k.is_empty() => exclusive_start_key = Some(k),
                _ => break,
            }
        }
        Ok(result)
    }

    /// GET a domain blob. Returns `(blob, etag)`. On 404 (NoSuchKey),
    /// returns an empty default blob with `None` etag so the next PUT
    /// uses `If-None-Match: *` to create the key.
    async fn get_blob<T>(&self, key: &str) -> FoldDbResult<(DomainBlob<T>, Option<String>)>
    where
        T: DeserializeOwned,
    {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(resp) => {
                let etag = resp.e_tag().map(String::from);
                let body = resp.body.collect().await.map_err(|e| {
                    FoldDbError::Config(format!("Failed to read S3 body for '{key}': {e}"))
                })?;
                let bytes = body.into_bytes();
                if bytes.is_empty() {
                    return Ok((DomainBlob::default(), etag));
                }
                let blob: DomainBlob<T> = serde_json::from_slice(&bytes).map_err(|e| {
                    FoldDbError::Serialization(format!("Failed to parse S3 blob '{key}': {e}"))
                })?;
                Ok((blob, etag))
            }
            Err(e) => {
                if error_code(&e).as_deref() == Some("NoSuchKey") {
                    Ok((DomainBlob::default(), None))
                } else {
                    Err(FoldDbError::Config(format!(
                        "Failed to get S3 object '{}': {}",
                        key,
                        short_error(&e)
                    )))
                }
            }
        }
    }

    /// Read-modify-write loop for a domain blob with ETag-based
    /// optimistic concurrency. Bounded retries on precondition failure.
    async fn update_blob<T, F>(&self, key: &str, modify: F) -> FoldDbResult<()>
    where
        T: Serialize + DeserializeOwned,
        F: Fn(&mut DomainBlob<T>) -> FoldDbResult<()>,
    {
        for attempt in 0..MAX_RMW_RETRIES {
            let (mut blob, etag) = self.get_blob::<T>(key).await?;
            modify(&mut blob)?;
            let body = serde_json::to_vec(&blob).map_err(|e| {
                FoldDbError::Serialization(format!("Failed to serialize blob '{key}': {e}"))
            })?;

            let mut put = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(body))
                .content_type("application/json");

            // Optimistic concurrency:
            //   - blob existed  → require matching ETag (If-Match)
            //   - blob missing  → require key to still not exist (If-None-Match: *)
            put = match etag.as_deref() {
                Some(tag) => put.if_match(tag),
                None => put.if_none_match("*"),
            };

            match put.send().await {
                Ok(_) => return Ok(()),
                Err(e) if is_precondition_failed(&e) => {
                    tracing::warn!(
                        key = key,
                        attempt = attempt + 1,
                        "S3 blob ETag precondition failed — re-reading and retrying"
                    );
                    continue;
                }
                Err(e) => {
                    return Err(FoldDbError::Config(format!(
                        "Failed to put S3 object '{}': {}",
                        key,
                        short_error(&e)
                    )));
                }
            }
        }
        Err(FoldDbError::Config(format!(
            "Exceeded {MAX_RMW_RETRIES} retries updating S3 blob '{key}' (ETag contention)"
        )))
    }
}

// -------- error introspection helpers --------
//
// aws-sdk-s3 v1 surfaces service errors via `ProvideErrorMetadata`,
// which returns an error code string like "NoSuchKey" or
// "PreconditionFailed". We match on the code rather than the SdkError
// variant so these helpers are operation-agnostic (GetObject, PutObject,
// etc. all surface the same shape).

fn error_code<E, R>(err: &aws_sdk_s3::error::SdkError<E, R>) -> Option<String>
where
    E: ProvideErrorMetadata,
{
    err.code().map(String::from)
}

fn is_precondition_failed<E, R>(err: &aws_sdk_s3::error::SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    matches!(err.code(), Some("PreconditionFailed"))
}

fn short_error<E, R>(err: &aws_sdk_s3::error::SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata,
{
    match err.code() {
        Some(code) => format!("{} — {}", code, err.message().unwrap_or("no error message")),
        None => "unknown S3 error (no error code in response)".to_string(),
    }
}
