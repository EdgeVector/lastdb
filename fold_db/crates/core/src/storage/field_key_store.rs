//! Durable per-field-key key bundles and access-domain wraps.
//!
//! A field key's three-key bundle is minted once; its blind-index key blinds
//! the field key hashes. The plaintext bundle stays only in the process cache.
//! Durable rows contain a small metadata record (`fkb:`) and one encrypted
//! wrap per access domain (`fkw:`). Catalog sharing adds a wrap; it never
//! reseals field key data.

use crate::crypto::{seal_at_rest_utf8, AT_REST_ENC_PREFIX};
use crate::schema::types::Schema;
use crate::storage::error::StorageResult;
use crate::storage::traits::KvStore;
use crate::storage::{StorageError, TypedKvStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

const BUNDLE_VERSION: u8 = 1;
const NODE_DOMAIN: &str = "node";

mod batch_load;
mod bundle;
mod cache;
mod contracts;
mod establish;
mod frozen;
mod metadata_batch;
mod share;

pub(crate) use frozen::{
    node_bundle_storage_keys, verify_frozen_typed_profile, NODE_KEY_ROW_MAX_BYTES,
};

pub(crate) type FieldKeyCache = Arc<RwLock<HashMap<String, CachedFieldKeyContract>>>;
pub use bundle::FieldKeyBundle;

#[derive(Clone)]
pub(crate) struct CachedFieldKeyContract {
    range_profile: schema_types::field_range_profile::FieldRangeProfile,
    bundle: FieldKeyBundle,
}

/// Every field key owns an independent key bundle under its node wrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum KeyAuthority {
    Bundle,
}

/// Every conflict ID names the stored field key coordinate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ConflictKeyIdentity {
    StoredV1,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BundleMetadata {
    version: u8,
    field_key_id: String,
    range_profile: schema_types::field_range_profile::FieldRangeProfile,
    key_authority: KeyAuthority,
    conflict_identity: ConflictKeyIdentity,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BundleWrap {
    version: u8,
    field_key_id: String,
    domain: String,
    sealed_bundle: String,
}

/// Exact-key persistence and the shared in-process plaintext cache.
#[derive(Clone)]
pub struct FieldKeyStore {
    entries: Arc<TypedKvStore<dyn KvStore>>,
    node_wrap_key: Option<[u8; 32]>,
    cache: FieldKeyCache,
    mint_lock: Arc<Mutex<()>>,
    runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
}

impl FieldKeyStore {
    pub(crate) fn new(
        entries: Arc<TypedKvStore<dyn KvStore>>,
        node_wrap_key: Option<[u8; 32]>,
        runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
    ) -> Self {
        Self {
            entries,
            node_wrap_key,
            cache: Arc::new(RwLock::new(HashMap::new())),
            mint_lock: Arc::new(Mutex::new(())),
            runtime,
        }
    }

    pub(crate) const fn is_enabled(&self) -> bool {
        self.node_wrap_key.is_some()
    }

    /// True when this store serves a node home. Only a node home mints and
    /// loads the record space key bundle of a schema.
    fn record_home(&self) -> bool {
        self.runtime.is_some()
    }

    /// Resolve the exact field key contract and its key bundle.
    pub async fn load(&self, field_key_id: &str) -> StorageResult<Option<FieldKeyBundle>> {
        validate_identity(field_key_id, NODE_DOMAIN)?;
        if let Some(contract) = self.cached_contract(field_key_id)? {
            return Ok(Some(contract.bundle));
        }
        self.load_existing(&[field_key_id.to_owned()]).await?;
        self.cached_bundle(field_key_id)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.entries.inner().flush().await
    }

    /// Load every final field key's established keys. Only schema
    /// publication mints keys.
    pub async fn ensure_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.load_schema(schema).await
    }

    /// Only schema publication may mint new typed contracts. Existing fields
    /// must resolve their old contracts before any replacement can publish.
    pub(crate) async fn establish_schema_for_publication(
        &self,
        schema: &Schema,
        existing: Option<&Schema>,
    ) -> StorageResult<()> {
        if let Some(existing) = existing {
            self.load_schema(existing).await?;
        }
        self.establish_schema(schema).await
    }

    /// Load established field bundles for a catalog read without minting keys.
    pub async fn load_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.load_existing(&schema_field_key_ids(schema, self.record_home()))
            .await?;
        self.require_schema_profiles(schema)
    }
}

fn make_wrap(
    field_key: &str,
    domain: &str,
    bundle: &FieldKeyBundle,
    key: &[u8; 32],
) -> StorageResult<BundleWrap> {
    let plaintext = zeroize::Zeroizing::new(
        serde_json::to_vec(bundle)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
    );
    let sealed = seal_at_rest_utf8(key, &plaintext)
        .map_err(|_| StorageError::InvalidOperation("cannot wrap the field key bundle".into()))?;
    let sealed_bundle = String::from_utf8(sealed)
        .map_err(|_| StorageError::InvalidOperation("the field key wrap is not UTF-8".into()))?;
    Ok(BundleWrap {
        version: BUNDLE_VERSION,
        field_key_id: field_key.into(),
        domain: domain.into(),
        sealed_bundle,
    })
}

fn open_wrap(
    wrap: &BundleWrap,
    field_key_id: &str,
    domain: &str,
    wrapping_key: &[u8; 32],
) -> StorageResult<FieldKeyBundle> {
    if wrap.version != BUNDLE_VERSION || wrap.field_key_id != field_key_id || wrap.domain != domain
    {
        return Err(StorageError::InvalidOperation(format!(
            "field key wrap metadata mismatch for {field_key_id} in {domain}"
        )));
    }
    // `make_wrap` writes only the `ENC:` form of `seal_at_rest_utf8`.
    if !wrap.sealed_bundle.starts_with(AT_REST_ENC_PREFIX) {
        return Err(StorageError::InvalidOperation(
            "field key bundle wrap is not encrypted".into(),
        ));
    }
    // The generated bundle contains only three fixed-size keys, so a wrap
    // above this bound is not one that `make_wrap` wrote.
    let plaintext = zeroize::Zeroizing::new(
        crate::crypto::at_rest::open_at_rest_bounded(
            wrapping_key,
            wrap.sealed_bundle.as_bytes(),
            4096,
        )
        .map_err(|error| {
            StorageError::InvalidOperation(format!("unwrap field key bundle: {error}"))
        })?,
    );
    serde_json::from_slice(&plaintext).map_err(|error| {
        StorageError::SerializationError(format!("decode field key bundle: {error}"))
    })
}

fn validate_identity(field_key_id: &str, domain: &str) -> StorageResult<()> {
    if field_key_id.trim().is_empty() || domain.trim().is_empty() {
        return Err(StorageError::InvalidOperation(
            "field key bundle requires non-empty field key and domain".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn metadata_key(field_key_id: &str) -> String {
    format!("fkb:{field_key_id}")
}

fn metadata_memory_owner(cache: &FieldKeyCache) -> usize {
    Arc::as_ptr(cache) as usize
}

/// Absent metadata charges 256. A present record charges the field key id capacity plus 256.
fn metadata_memory_bytes(metadata: Option<&BundleMetadata>) -> usize {
    metadata.map_or(256, |value| {
        value.field_key_id.capacity().saturating_add(256)
    })
}

pub(crate) fn wrap_key_for(field_key_id: &str, domain: &str) -> String {
    let domain_hash = Sha256::digest(domain.as_bytes());
    format!("fkw:{field_key_id}:{domain_hash:x}")
}

/// Every field and alternate field key of a schema. With `records` (a node
/// home), the record space of the schema too.
pub(crate) fn schema_field_key_ids(schema: &Schema, records: bool) -> Vec<String> {
    let mut field_keys = schema
        .runtime_fields
        .iter()
        .map(|(field_name, field)| {
            field.common().field_key_id().cloned().unwrap_or_else(|| {
                lastdb_model::field_key_id::deterministic_field_key_id(&schema.name, field_name)
            })
        })
        .collect::<Vec<_>>();
    field_keys.extend(
        schema
            .alternate_keys
            .keys()
            .map(|name| lastdb_model::field_key_id::alternate_field_key_id(schema, name)),
    );
    if records {
        field_keys.push(lastdb_model::record_space::record_space_uuid(&schema.name));
    }
    field_keys.sort();
    field_keys.dedup();
    field_keys
}

/// Every field and alternate owns its own declared range profile. With
/// `records` (a node home), the record space of the schema uses the range
/// profile of the primary key.
pub(crate) fn schema_field_key_profiles(
    schema: &Schema,
    records: bool,
) -> StorageResult<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>> {
    use schema_types::field_range_profile::FieldRangeProfile;
    let kind = schema
        .key
        .as_ref()
        .and_then(|key| key.range_field.as_deref())
        .map(|name| schema.key_field_type(name));
    let primary =
        FieldRangeProfile::for_field_type(kind.as_ref()).map_err(StorageError::InvalidOperation)?;
    let mut result: Vec<_> = schema
        .runtime_fields
        .iter()
        .map(|(name, field)| {
            (
                field.common().field_key_id().cloned().unwrap_or_else(|| {
                    lastdb_model::field_key_id::deterministic_field_key_id(&schema.name, name)
                }),
                primary,
            )
        })
        .collect();
    for (name, key) in &schema.alternate_keys {
        let field_type = key.range_type(&schema_types::DeclarativeSchemaDefinition::from(schema));
        let profile = FieldRangeProfile::for_field_type(field_type.as_ref())
            .map_err(StorageError::InvalidOperation)?;
        result.push((
            lastdb_model::field_key_id::alternate_field_key_id(schema, name),
            profile,
        ));
    }
    if records {
        result.push((
            lastdb_model::record_space::record_space_uuid(&schema.name),
            primary,
        ));
    }
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup();
    Ok(result)
}
