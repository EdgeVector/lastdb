//! Field-key contracts and their shared process cache.
//!
//! A record-space or alternate-key bundle is minted once. Its blind key blinds
//! that key's hashes. A payload field has no bundle. An older home may still
//! hold payload-field bundles. Opening the catalog deletes those rows by the
//! exact ids the old writer used. The record-space bundle and each alternate
//! bundle stay. The plaintext bundle stays only in
//! the process cache. The format owns the metadata and encrypted node-wrap
//! wire definitions. This provider holds only a read view and typed format
//! capability.

use crate::schema::types::Schema;
use crate::storage::error::StorageResult;
use crate::storage::format::field_keys::{
    metadata_key, validate_identity, wrap_key_for, BundleMetadata, BundleWrap, FieldKeyFormat,
};
use crate::storage::format::read::ReadStore;
use crate::storage::StorageError;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

mod batch_load;
mod cache;
mod contracts;
mod establish;
mod metadata_batch;
mod release;

pub(crate) use crate::storage::format::field_keys::{
    node_bundle_storage_keys, verify_frozen_typed_profile, NODE_KEY_ROW_MAX_BYTES,
};

pub(crate) type FieldKeyCache = Arc<RwLock<HashMap<String, CachedFieldKeyContract>>>;
pub use crate::storage::format::field_keys::FieldKeyBundle;

#[derive(Clone)]
pub(crate) struct CachedFieldKeyContract {
    range_profile: schema_types::field_range_profile::FieldRangeProfile,
    bundle: FieldKeyBundle,
}

/// Exact-key persistence and the shared in-process plaintext cache.
#[derive(Clone)]
pub struct FieldKeyStore {
    entries: ReadStore,
    format: Arc<FieldKeyFormat>,
    cache: FieldKeyCache,
    mint_lock: Arc<Mutex<()>>,
    runtime: Arc<crate::storage::disk_pack::PackRuntime>,
}

impl FieldKeyStore {
    pub(crate) fn new(format: FieldKeyFormat) -> Self {
        let entries = format.reader();
        let runtime = format.runtime();
        Self {
            entries,
            format: Arc::new(format),
            cache: Arc::new(RwLock::new(HashMap::new())),
            mint_lock: Arc::new(Mutex::new(())),
            runtime,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.format.is_enabled()
    }

    /// Resolve the exact field key contract and its key bundle.
    pub async fn load(&self, field_key_id: &str) -> StorageResult<Option<FieldKeyBundle>> {
        validate_identity(field_key_id)?;
        if let Some(contract) = self.cached_contract(field_key_id)? {
            return Ok(Some(contract.bundle));
        }
        self.load_existing(&[field_key_id.to_owned()]).await?;
        self.cached_bundle(field_key_id)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.format.flush().await
    }

    /// Load every final field key's established keys. Only schema
    /// publication mints keys.
    pub async fn ensure_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.load_schema(schema).await
    }

    /// Only schema publication may mint new bundles. An existing record-space
    /// or alternate bundle must resolve before a replacement can publish.
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

    /// Load the record-space and alternate bundles without minting keys.
    pub async fn load_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.load_existing(&schema_field_key_ids(schema)).await?;
        self.require_schema_profiles(schema)
    }
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

/// Payload-field ids still stored on catalog rows from the retired field-key map.
///
/// The key is the schema name. The inner map is the old `field_key_ids` object:
/// field name to the id the old writer stored. An empty map is not recorded.
#[derive(Default)]
pub(crate) struct StoredPayloadFieldKeys {
    by_schema: HashMap<String, BTreeMap<String, String>>,
}

impl StoredPayloadFieldKeys {
    pub(crate) fn insert(&mut self, schema_name: String, ids: BTreeMap<String, String>) {
        if !ids.is_empty() {
            self.by_schema.insert(schema_name, ids);
        }
    }

    fn get(&self, schema_name: &str) -> Option<&BTreeMap<String, String>> {
        self.by_schema.get(schema_name)
    }
}

/// The record space and every named alternate key of a schema.
pub(crate) fn schema_field_key_ids(schema: &Schema) -> Vec<String> {
    let mut field_keys = schema
        .record_keys
        .iter()
        .flat_map(|keys| keys.alternates.keys())
        .map(|name| lastdb_model::field_key_id::alternate_field_key_id(schema, name))
        .collect::<Vec<_>>();
    field_keys.push(lastdb_model::record_space::record_space_uuid(&schema.name));
    field_keys.sort();
    field_keys.dedup();
    field_keys
}

/// Each alternate uses its own range profile. The record space uses the
/// primary key's range profile. A payload field has no profile here.
pub(crate) fn schema_field_key_profiles(
    schema: &Schema,
) -> StorageResult<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>> {
    let definitions = schema
        .require_record_keys()
        .map_err(|error| StorageError::InvalidOperation(error.to_string()))?;
    let primary = definitions
        .primary
        .range_profile()
        .map_err(StorageError::InvalidOperation)?;
    let mut result = Vec::new();
    for (name, key) in &definitions.alternates {
        let profile = key
            .range_profile()
            .map_err(StorageError::InvalidOperation)?;
        result.push((
            lastdb_model::field_key_id::alternate_field_key_id(schema, name),
            profile,
        ));
    }
    result.push((
        lastdb_model::record_space::record_space_uuid(&schema.name),
        primary,
    ));
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup();
    Ok(result)
}
