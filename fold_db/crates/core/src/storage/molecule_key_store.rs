//! Durable per-molecule key bundles and access-domain wraps.
//!
//! A molecule's three-key bundle is minted once; its blind-index key blinds
//! the molecule hashes. The plaintext bundle stays only in the process cache.
//! Durable rows contain a small metadata record (`mkb:`) and one encrypted
//! wrap per access domain (`mkw:`). Catalog sharing adds a wrap; it never
//! reseals molecule data.

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
mod metadata;
mod metadata_batch;
mod share;

pub(crate) use frozen::{
    node_bundle_storage_keys, verify_frozen_typed_profile, NODE_KEY_ROW_MAX_BYTES,
};

pub(crate) type MoleculeKeyCache = Arc<RwLock<HashMap<String, CachedMoleculeContract>>>;
pub use bundle::MoleculeKeyBundle;

#[derive(Clone)]
pub(crate) struct CachedMoleculeContract {
    range_profile: schema_types::molecule_range_profile::MoleculeRangeProfile,
    bundle: MoleculeKeyBundle,
}

/// Every molecule owns an independent key bundle under its node wrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum KeyAuthority {
    Bundle,
}

/// Every conflict ID names the stored molecule coordinate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ConflictKeyIdentity {
    StoredV1,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BundleMetadata {
    version: u8,
    molecule_uuid: String,
    range_profile: schema_types::molecule_range_profile::MoleculeRangeProfile,
    key_authority: KeyAuthority,
    conflict_identity: ConflictKeyIdentity,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BundleWrap {
    version: u8,
    molecule_uuid: String,
    domain: String,
    sealed_bundle: String,
}

/// Exact-key persistence and the shared in-process plaintext cache.
#[derive(Clone)]
pub struct MoleculeKeyStore {
    entries: Arc<TypedKvStore<dyn KvStore>>,
    node_wrap_key: Option<[u8; 32]>,
    cache: MoleculeKeyCache,
    mint_lock: Arc<Mutex<()>>,
    runtime: Option<Arc<crate::storage::disk_pack::PackRuntime>>,
}

impl MoleculeKeyStore {
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

    /// True when this store serves a format 7 home. Only such a home mints and
    /// loads the record space key bundle of a schema.
    fn record_home(&self) -> bool {
        self.runtime
            .as_deref()
            .is_some_and(crate::storage::disk_pack::PackRuntime::record_home)
    }

    /// Resolve the exact molecule contract and its key bundle.
    pub async fn load(&self, molecule_uuid: &str) -> StorageResult<Option<MoleculeKeyBundle>> {
        validate_identity(molecule_uuid, NODE_DOMAIN)?;
        if let Some(contract) = self.cached_contract(molecule_uuid)? {
            return Ok(Some(contract.bundle));
        }
        self.load_existing(&[molecule_uuid.to_owned()]).await?;
        self.cached_bundle(molecule_uuid)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.entries.inner().flush().await
    }

    /// Load every final field molecule's established keys. Only schema
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
        self.load_existing(&schema_molecule_uuids(schema, self.record_home()))
            .await?;
        self.require_schema_profiles(schema)
    }
}

fn make_wrap(
    molecule: &str,
    domain: &str,
    bundle: &MoleculeKeyBundle,
    key: &[u8; 32],
) -> StorageResult<BundleWrap> {
    let plaintext = zeroize::Zeroizing::new(
        serde_json::to_vec(bundle)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
    );
    let sealed = seal_at_rest_utf8(key, &plaintext).map_err(|_| {
        StorageError::InvalidOperation("cannot wrap the molecule key bundle".into())
    })?;
    let sealed_bundle = String::from_utf8(sealed)
        .map_err(|_| StorageError::InvalidOperation("the molecule key wrap is not UTF-8".into()))?;
    Ok(BundleWrap {
        version: BUNDLE_VERSION,
        molecule_uuid: molecule.into(),
        domain: domain.into(),
        sealed_bundle,
    })
}

fn open_wrap(
    wrap: &BundleWrap,
    molecule_uuid: &str,
    domain: &str,
    wrapping_key: &[u8; 32],
) -> StorageResult<MoleculeKeyBundle> {
    if wrap.version != BUNDLE_VERSION
        || wrap.molecule_uuid != molecule_uuid
        || wrap.domain != domain
    {
        return Err(StorageError::InvalidOperation(format!(
            "molecule key wrap metadata mismatch for {molecule_uuid} in {domain}"
        )));
    }
    // `make_wrap` writes only the `ENC:` form of `seal_at_rest_utf8`.
    if !wrap.sealed_bundle.starts_with(AT_REST_ENC_PREFIX) {
        return Err(StorageError::InvalidOperation(
            "molecule key bundle wrap is not encrypted".into(),
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
            StorageError::InvalidOperation(format!("unwrap molecule key bundle: {error}"))
        })?,
    );
    serde_json::from_slice(&plaintext).map_err(|error| {
        StorageError::SerializationError(format!("decode molecule key bundle: {error}"))
    })
}

fn validate_identity(molecule_uuid: &str, domain: &str) -> StorageResult<()> {
    if molecule_uuid.trim().is_empty() || domain.trim().is_empty() {
        return Err(StorageError::InvalidOperation(
            "molecule key bundle requires non-empty molecule and domain".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn metadata_key(molecule_uuid: &str) -> String {
    format!("mkb:{molecule_uuid}")
}

fn metadata_memory_owner(cache: &MoleculeKeyCache) -> usize {
    Arc::as_ptr(cache) as usize
}

/// Absent metadata charges 256. A present record charges the molecule id capacity plus 256.
fn metadata_memory_bytes(metadata: Option<&BundleMetadata>) -> usize {
    metadata.map_or(256, |value| {
        value.molecule_uuid.capacity().saturating_add(256)
    })
}

pub(crate) fn wrap_key_for(molecule_uuid: &str, domain: &str) -> String {
    let domain_hash = Sha256::digest(domain.as_bytes());
    format!("mkw:{molecule_uuid}:{domain_hash:x}")
}

/// Every field and alternate molecule of a schema. With `records` (a format 7
/// home), the record space of the schema too.
pub(crate) fn schema_molecule_uuids(schema: &Schema, records: bool) -> Vec<String> {
    let mut molecules = schema
        .runtime_fields
        .iter()
        .map(|(field_name, field)| {
            field.common().molecule_uuid().cloned().unwrap_or_else(|| {
                crate::atom::deterministic_molecule_uuid(&schema.name, field_name)
            })
        })
        .collect::<Vec<_>>();
    molecules.extend(
        schema
            .alternate_keys
            .keys()
            .map(|name| crate::atom::molecule_uuid::alternate_molecule_uuid(schema, name)),
    );
    if records {
        molecules.push(lastdb_model::record_space::record_space_uuid(&schema.name));
    }
    molecules.sort();
    molecules.dedup();
    molecules
}

/// Every field and alternate owns its own declared range profile. With
/// `records` (a format 7 home), the record space of the schema uses the range
/// profile of the primary key.
pub(crate) fn schema_molecule_profiles(
    schema: &Schema,
    records: bool,
) -> StorageResult<
    Vec<(
        String,
        schema_types::molecule_range_profile::MoleculeRangeProfile,
    )>,
> {
    use schema_types::molecule_range_profile::MoleculeRangeProfile;
    let kind = schema
        .key
        .as_ref()
        .and_then(|key| key.range_field.as_deref())
        .map(|name| schema.key_field_type(name));
    let primary = MoleculeRangeProfile::for_field_type(kind.as_ref())
        .map_err(StorageError::InvalidOperation)?;
    let mut result: Vec<_> = schema
        .runtime_fields
        .iter()
        .map(|(name, field)| {
            (
                field.common().molecule_uuid().cloned().unwrap_or_else(|| {
                    crate::atom::deterministic_molecule_uuid(&schema.name, name)
                }),
                primary,
            )
        })
        .collect();
    for (name, key) in &schema.alternate_keys {
        let field_type = key.range_type(&schema_types::DeclarativeSchemaDefinition::from(schema));
        let profile = MoleculeRangeProfile::for_field_type(field_type.as_ref())
            .map_err(StorageError::InvalidOperation)?;
        result.push((
            crate::atom::molecule_uuid::alternate_molecule_uuid(schema, name),
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
