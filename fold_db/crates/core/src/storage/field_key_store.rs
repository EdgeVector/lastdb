//! Field-key contracts and their shared process cache.
//!
//! A field key's three-key bundle is minted once; its blind-index key blinds
//! the field key hashes. The plaintext bundle stays only in the process cache.
//! The format owns the metadata and encrypted node-wrap wire definitions.
//! This provider holds only a read view and typed format capability.

use crate::schema::types::Schema;
use crate::storage::error::StorageResult;
use crate::storage::format::field_keys::{
    metadata_key, validate_identity, wrap_key_for, BundleMetadata, BundleWrap, FieldKeyFormat,
};
use crate::storage::format::read::ReadStore;
use crate::storage::StorageError;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

mod batch_load;
mod cache;
mod contracts;
mod establish;
mod metadata_batch;

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

/// Every field and alternate field key of a schema, and its record space.
pub(crate) fn schema_field_key_ids(schema: &Schema) -> Vec<String> {
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
            .record_keys
            .iter()
            .flat_map(|keys| keys.alternates.keys())
            .map(|name| lastdb_model::field_key_id::alternate_field_key_id(schema, name)),
    );
    field_keys.push(lastdb_model::record_space::record_space_uuid(&schema.name));
    field_keys.sort();
    field_keys.dedup();
    field_keys
}

/// Every field and alternate owns its own declared range profile. The record
/// space of the schema uses the range profile of the primary key.
pub(crate) fn schema_field_key_profiles(
    schema: &Schema,
) -> StorageResult<Vec<(String, schema_types::field_range_profile::FieldRangeProfile)>> {
    use schema_types::field_range_profile::FieldRangeProfile;
    let definitions = schema
        .require_record_keys()
        .map_err(|error| StorageError::InvalidOperation(error.to_string()))?;
    let primary = definitions
        .primary
        .range_profile()
        .map_err(StorageError::InvalidOperation)?;
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
