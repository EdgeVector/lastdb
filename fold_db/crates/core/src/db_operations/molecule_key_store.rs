//! Durable per-molecule key bundles and access-domain wraps.
//!
//! A molecule's content, blind-index, and OPE keys are minted once. The
//! plaintext bundle stays only in the process cache. Durable rows contain a
//! small metadata record (`mkb:`) and one encrypted wrap per access domain
//! (`mkw:`). Catalog sharing adds a wrap; it never reseals molecule data.

use crate::atom::MoleculeKeyCodec;
use crate::crypto::{is_sealed_at_rest, seal_at_rest_utf8};
use crate::schema::types::Schema;
use crate::storage::error::StorageResult;
use crate::storage::traits::KvStore;
use crate::storage::{StorageError, TypedKvStore};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use zeroize::{Zeroize, ZeroizeOnDrop};

const BUNDLE_VERSION: u8 = 1;
const NODE_DOMAIN: &str = "node";

mod batch_load;
mod cache;
mod frozen;
mod metadata;
mod metadata_batch;

pub(crate) use frozen::{
    node_bundle_storage_keys, open_frozen_node_bundle, NODE_KEY_ROW_MAX_BYTES,
};

pub(crate) type MoleculeKeyCache = Arc<RwLock<HashMap<String, MoleculeKeyBundle>>>;

/// The three independent keys that define one molecule's sealed byte form.
#[derive(Clone, Deserialize, Serialize, Zeroize, ZeroizeOnDrop)]
pub struct MoleculeKeyBundle {
    content_dek: [u8; 32],
    blind_key: [u8; 32],
    ope_key: [u8; 32],
}

impl std::fmt::Debug for MoleculeKeyBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MoleculeKeyBundle(<redacted>)")
    }
}

impl MoleculeKeyBundle {
    fn generate() -> Self {
        let mut bundle = Self {
            content_dek: [0; 32],
            blind_key: [0; 32],
            ope_key: [0; 32],
        };
        OsRng.fill_bytes(&mut bundle.content_dek);
        OsRng.fill_bytes(&mut bundle.blind_key);
        OsRng.fill_bytes(&mut bundle.ope_key);
        bundle
    }

    pub(crate) fn content_dek(&self) -> [u8; 32] {
        self.content_dek
    }

    pub(crate) fn key_codec(&self, configured: &MoleculeKeyCodec) -> MoleculeKeyCodec {
        MoleculeKeyCodec::with_encodings(
            configured.encoding(),
            configured.range_encoding(),
            Some(self.blind_key),
            Some(self.ope_key),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BundleMetadata {
    version: u8,
    molecule_uuid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    range_profile: Option<schema_types::molecule_range_profile::MoleculeRangeProfile>,
}

impl BundleMetadata {
    /// Current homes have no typed profile. Refuse a profile rather than ignore
    /// it until the complete typed runtime and logical format are published.
    fn is_legacy_for(&self, molecule: &str) -> bool {
        self.version == BUNDLE_VERSION
            && self.molecule_uuid == molecule
            && self.range_profile.is_none()
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

    /// Resolve the node wrap with two exact reads. Missing metadata means no bundle.
    pub async fn load(&self, molecule_uuid: &str) -> StorageResult<Option<MoleculeKeyBundle>> {
        validate_identity(molecule_uuid, NODE_DOMAIN)?;
        if let Some(bundle) = self.cached_bundle(molecule_uuid)? {
            return Ok(Some(bundle));
        }
        let Some(wrap_key) = self.node_wrap_key.as_ref() else {
            return Ok(None);
        };
        if self.metadata_from_memory(molecule_uuid).await?.is_none() {
            return Ok(None);
        }
        let Some(wrap) = self
            .entries
            .get_item::<BundleWrap>(&wrap_key_for(molecule_uuid, NODE_DOMAIN))
            .await?
        else {
            return Err(StorageError::InvalidOperation(format!(
                "molecule key metadata has no node wrap for {molecule_uuid}"
            )));
        };
        let bundle = open_wrap(&wrap, molecule_uuid, NODE_DOMAIN, wrap_key)?;
        self.remember(molecule_uuid, bundle.clone())?;
        Ok(Some(bundle))
    }

    /// Load an existing bundle or mint one. Callers use this on new instances.
    pub async fn ensure(&self, molecule_uuid: &str) -> StorageResult<MoleculeKeyBundle> {
        if let Some(bundle) = self.load(molecule_uuid).await? {
            return Ok(bundle);
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
                },
            )
            .await?;
        self.entries.inner().flush().await?;
        self.remember(molecule_uuid, bundle.clone())?;
        Ok(bundle)
    }

    /// Grant one access domain to every molecule in an existing schema.
    ///
    /// The preflight resolves every bundle before it writes a wrap. Schema
    /// publication establishes these keys; a missing bundle is an error.
    pub async fn grant_schema_domain(
        &self,
        schema: &Schema,
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<usize> {
        if !self.is_enabled() {
            return Ok(0);
        }
        validate_identity(&schema.name, domain)?;
        let molecules = schema_molecule_uuids(schema);

        let mut bundles = Vec::with_capacity(molecules.len());
        let mut missing = Vec::new();
        for molecule_uuid in molecules {
            match self.load(&molecule_uuid).await? {
                Some(bundle) => bundles.push((molecule_uuid, bundle)),
                None => missing.push(molecule_uuid),
            }
        }
        if !missing.is_empty() {
            return Err(StorageError::InvalidOperation(format!(
                "schema share requires an established molecule key bundle for {}",
                missing.join(", ")
            )));
        }

        for (molecule_uuid, bundle) in &bundles {
            self.put_wrap(molecule_uuid, domain, bundle, domain_wrap_key)
                .await?;
        }
        self.entries.inner().flush().await?;
        Ok(bundles.len())
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.entries.inner().flush().await
    }

    /// Establish every final field molecule's keys before schema publication.
    pub async fn ensure_schema(&self, schema: &Schema) -> StorageResult<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        let populated = if schema.runtime_fields.is_empty() {
            let mut populated = schema.clone();
            populated.populate_runtime_fields().map_err(|error| {
                StorageError::InvalidOperation(format!("resolve schema molecule keys: {error}"))
            })?;
            Some(populated)
        } else {
            None
        };
        for molecule_uuid in schema_molecule_uuids(populated.as_ref().unwrap_or(schema)) {
            self.ensure(&molecule_uuid).await?;
        }
        Ok(())
    }

    /// Load established field bundles for a catalog read without minting keys.
    pub async fn load_schema(&self, schema: &Schema) -> StorageResult<()> {
        self.load_existing(&schema_molecule_uuids(schema)).await
    }

    async fn put_wrap(
        &self,
        molecule_uuid: &str,
        domain: &str,
        bundle: &MoleculeKeyBundle,
        wrapping_key: &[u8; 32],
    ) -> StorageResult<()> {
        let plaintext = serde_json::to_vec(bundle).map_err(|error| {
            StorageError::SerializationError(format!("serialize molecule key bundle: {error}"))
        })?;
        let sealed = seal_at_rest_utf8(wrapping_key, &plaintext).map_err(|error| {
            StorageError::InvalidOperation(format!("wrap molecule key bundle: {error}"))
        })?;
        let sealed_bundle = String::from_utf8(sealed).map_err(|error| {
            StorageError::InvalidOperation(format!("molecule key wrap is not UTF-8: {error}"))
        })?;
        self.entries
            .put_item(
                &wrap_key_for(molecule_uuid, domain),
                &BundleWrap {
                    version: BUNDLE_VERSION,
                    molecule_uuid: molecule_uuid.to_string(),
                    domain: domain.to_string(),
                    sealed_bundle,
                },
            )
            .await
    }
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
