//! Durable per-molecule key bundles and access-domain wraps.
//!
//! A molecule's content, blind-index, and OPE keys are minted once. The
//! plaintext bundle stays only in the process cache. Durable rows contain a
//! small metadata record (`mkb:`) and one encrypted wrap per access domain
//! (`mkw:`). Catalog sharing adds a wrap; it never reseals molecule data.

use crate::atom::molecule_key_codec::ConflictKeyIdentity;
use crate::crypto::{is_sealed_at_rest, seal_at_rest_utf8};
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
    node_bundle_storage_keys, open_frozen_node_bundle, typed_frozen_metadata,
    NODE_KEY_ROW_MAX_BYTES,
};

pub(crate) type MoleculeKeyCache = Arc<RwLock<HashMap<String, CachedMoleculeContract>>>;
pub use bundle::MoleculeKeyBundle;

#[derive(Clone)]
pub(crate) struct CachedMoleculeContract {
    range_profile: Option<schema_types::molecule_range_profile::MoleculeRangeProfile>,
    bundle: Option<MoleculeKeyBundle>,
    conflict_identity: Option<ConflictKeyIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum KeyAuthority {
    Bundle,
    Node,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BundleMetadata {
    version: u8,
    molecule_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    range_profile: Option<schema_types::molecule_range_profile::MoleculeRangeProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_authority: Option<KeyAuthority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conflict_identity: Option<ConflictKeyIdentity>,
}

impl BundleMetadata {
    /// Current homes have no typed profile. Refuse a profile rather than ignore
    /// it until the complete typed runtime and logical format are published.
    fn is_legacy_for(&self, molecule: &str) -> bool {
        self.version == BUNDLE_VERSION
            && self.molecule_uuid == molecule
            && self.range_profile.is_none()
            && self.key_authority.is_none()
            && self.conflict_identity.is_none()
    }
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

    /// Resolve the exact molecule contract. Node authority has no molecule bundle.
    pub async fn load(&self, molecule_uuid: &str) -> StorageResult<Option<MoleculeKeyBundle>> {
        validate_identity(molecule_uuid, NODE_DOMAIN)?;
        if let Some(contract) = self.cached_contract(molecule_uuid)? {
            return Ok(contract.bundle);
        }
        self.load_existing(&[molecule_uuid.to_owned()]).await?;
        self.cached_bundle(molecule_uuid)
    }

    /// Load an existing bundle or mint one. Callers use this on new instances.
    pub async fn ensure(&self, molecule_uuid: &str) -> StorageResult<MoleculeKeyBundle> {
        if let Some(bundle) = self.load(molecule_uuid).await? {
            return Ok(bundle);
        }
        if self.is_typed() {
            return Err(StorageError::InvalidOperation(
                "a new typed molecule requires its schema range profile".into(),
            ));
        }
        // Every mint takes the native write gate before the shared mint lock.
        // An enclosing schema publication already owns that same gate.
        crate::storage::disk_pack::transaction::scope(
            self.runtime.clone(),
            self.ensure_in_scope(molecule_uuid),
        )
        .await
    }

    async fn ensure_in_scope(&self, molecule_uuid: &str) -> StorageResult<MoleculeKeyBundle> {
        let _guard = self.mint_lock.lock().await;
        if let Some(bundle) = self.load(molecule_uuid).await? {
            return Ok(bundle);
        }
        let wrap_key = self.node_wrap_key.as_ref().ok_or_else(|| {
            StorageError::InvalidOperation(
                "per-molecule key bundles require a node wrapping key".to_string(),
            )
        })?;
        let bundle = MoleculeKeyBundle::generate();
        self.put_wrap(molecule_uuid, NODE_DOMAIN, &bundle, wrap_key)
            .await?;
        self.entries
            .put_item(
                &metadata_key(molecule_uuid),
                &BundleMetadata {
                    version: BUNDLE_VERSION,
                    molecule_uuid: molecule_uuid.to_string(),
                    range_profile: None,
                    key_authority: None,
                    conflict_identity: None,
                },
            )
            .await?;
        self.entries.inner().flush().await?;
        self.remember(molecule_uuid, bundle.clone())?;
        Ok(bundle)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.entries.inner().flush().await
    }

    /// Establish every final field molecule's keys before schema publication.
    pub async fn ensure_schema(&self, schema: &Schema) -> StorageResult<()> {
        if self.is_typed() {
            return self.load_schema(schema).await;
        }
        self.establish_schema(schema).await
    }

    /// Only schema publication may mint new typed contracts. Existing fields
    /// must resolve their old contracts before any replacement can publish.
    pub(crate) async fn establish_schema_for_publication(
        &self,
        schema: &Schema,
        existing: Option<&Schema>,
    ) -> StorageResult<()> {
        if self.is_typed() {
            if let Some(existing) = existing {
                self.load_schema(existing).await?;
            }
        }
        self.establish_schema(schema).await
    }

    /// Load established field bundles for a catalog read without minting keys.
    pub async fn load_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.load_existing(&schema_molecule_uuids(schema)).await?;
        self.require_schema_profiles(schema)
    }

    async fn put_wrap(
        &self,
        molecule_uuid: &str,
        domain: &str,
        bundle: &MoleculeKeyBundle,
        wrapping_key: &[u8; 32],
    ) -> StorageResult<()> {
        let wrap = make_wrap(molecule_uuid, domain, bundle, wrapping_key)?;
        self.entries
            .put_item(&wrap_key_for(molecule_uuid, domain), &wrap)
            .await
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
    if !is_sealed_at_rest(wrap.sealed_bundle.as_bytes()) {
        return Err(StorageError::InvalidOperation(
            "molecule key bundle wrap is not encrypted".into(),
        ));
    }
    // The generated bundle contains only three fixed-size keys. Bound any
    // historical compressed envelope before its plaintext can reach JSON.
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

pub(crate) fn wrap_key_for(molecule_uuid: &str, domain: &str) -> String {
    let domain_hash = Sha256::digest(domain.as_bytes());
    format!("mkw:{molecule_uuid}:{domain_hash:x}")
}

pub(crate) fn schema_molecule_uuids(schema: &Schema) -> Vec<String> {
    let mut molecules = schema
        .runtime_fields
        .iter()
        .map(|(field_name, field)| {
            field.common().molecule_uuid().cloned().unwrap_or_else(|| {
                crate::atom::deterministic_molecule_uuid(&schema.name, field_name)
            })
        })
        .collect::<Vec<_>>();
    molecules.sort();
    molecules.dedup();
    molecules
}
