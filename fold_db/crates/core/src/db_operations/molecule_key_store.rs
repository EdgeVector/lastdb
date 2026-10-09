//! Durable per-molecule key bundles and access-domain wraps.
//!
//! A molecule's content, blind-index, and OPE keys are minted once. The
//! plaintext bundle stays only in the process cache. Durable rows contain a
//! small metadata record (`mkb:`) and one encrypted wrap per access domain
//! (`mkw:`). Catalog sharing adds a wrap; it never reseals molecule data.

use crate::atom::MoleculeKeyCodec;
use crate::crypto::{open_at_rest, seal_at_rest_utf8};
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

    pub(crate) fn key_codec(&self, legacy: &MoleculeKeyCodec) -> MoleculeKeyCodec {
        legacy.with_primary_keys_and_read_fallback(self.blind_key, self.ope_key)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BundleMetadata {
    version: u8,
    molecule_uuid: String,
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
}

impl MoleculeKeyStore {
    pub(crate) fn new(
        entries: Arc<TypedKvStore<dyn KvStore>>,
        node_wrap_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            entries,
            node_wrap_key,
            cache: Arc::new(RwLock::new(HashMap::new())),
            mint_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn cache(&self) -> MoleculeKeyCache {
        Arc::clone(&self.cache)
    }

    pub(crate) const fn is_enabled(&self) -> bool {
        self.node_wrap_key.is_some()
    }

    /// Resolve the node wrap with two exact reads. A missing row is legacy.
    pub async fn load(&self, molecule_uuid: &str) -> StorageResult<Option<MoleculeKeyBundle>> {
        validate_identity(molecule_uuid, NODE_DOMAIN)?;
        if let Some(bundle) = self.cached(molecule_uuid) {
            return Ok(Some(bundle));
        }
        let Some(wrap_key) = self.node_wrap_key.as_ref() else {
            return Ok(None);
        };
        let Some(metadata) = self
            .entries
            .get_item::<BundleMetadata>(&metadata_key(molecule_uuid))
            .await?
        else {
            return Ok(None);
        };
        if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != molecule_uuid {
            return Err(StorageError::InvalidOperation(format!(
                "molecule key metadata mismatch for {molecule_uuid}"
            )));
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
        self.remember(molecule_uuid, bundle.clone());
        Ok(Some(bundle))
    }

    /// Load an existing bundle or mint one. Callers use this on new instances.
    pub async fn ensure(&self, molecule_uuid: &str) -> StorageResult<MoleculeKeyBundle> {
        if let Some(bundle) = self.load(molecule_uuid).await? {
            return Ok(bundle);
        }
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
                },
            )
            .await?;
        self.entries.inner().flush().await?;
        self.remember(molecule_uuid, bundle.clone());
        Ok(bundle)
    }

    /// Add or replace one access-domain wrap without changing the bundle.
    pub async fn grant_domain(
        &self,
        molecule_uuid: &str,
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<()> {
        validate_identity(molecule_uuid, domain)?;
        let bundle = self.ensure(molecule_uuid).await?;
        self.put_wrap(molecule_uuid, domain, &bundle, domain_wrap_key)
            .await?;
        self.entries.inner().flush().await
    }

    /// Grant one access domain to every molecule in an existing schema.
    ///
    /// The preflight resolves every bundle before it writes a wrap. A missing
    /// bundle means legacy data that still needs the one-time re-key job; the
    /// share path must fail closed instead of minting keys over old bytes.
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
        if bundles.is_empty() {
            // Legacy instance (no bundles yet): same-node zero-copy share still
            // works via the node wrap / unprefixed keys. Member restore that
            // holds only the org key needs a re-key job first.
            return Ok(0);
        }
        if !missing.is_empty() {
            return Err(StorageError::InvalidOperation(format!(
                "schema share requires a molecule key bundle for {}; re-key the legacy instance first",
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

    /// Drop every non-node wrap for a schema after its last catalog reference
    /// disappears. The node wrap remains the durable owner capability.
    pub async fn drop_schema_domain_wraps(&self, schema: &Schema) -> StorageResult<usize> {
        if !self.is_enabled() {
            return Ok(0);
        }

        let mut dropped = 0usize;
        for molecule_uuid in schema_molecule_uuids(schema) {
            let prefix = format!("mkw:{molecule_uuid}:");
            let node_key = wrap_key_for(&molecule_uuid, NODE_DOMAIN);
            for key in self.entries.list_keys_with_prefix(&prefix).await? {
                if key == node_key {
                    continue;
                }
                if self.entries.delete_item(&key).await? {
                    dropped += 1;
                }
            }
        }
        self.entries.inner().flush().await?;
        Ok(dropped)
    }

    /// Open one named domain wrap. This supports future member restore paths.
    pub async fn load_for_domain(
        &self,
        molecule_uuid: &str,
        domain: &str,
        domain_wrap_key: &[u8; 32],
    ) -> StorageResult<Option<MoleculeKeyBundle>> {
        validate_identity(molecule_uuid, domain)?;
        let Some(wrap) = self
            .entries
            .get_item::<BundleWrap>(&wrap_key_for(molecule_uuid, domain))
            .await?
        else {
            return Ok(None);
        };
        let bundle = open_wrap(&wrap, molecule_uuid, domain, domain_wrap_key)?;
        self.remember(molecule_uuid, bundle.clone());
        Ok(Some(bundle))
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.entries.inner().flush().await
    }

    /// Mint every molecule in a new catalog schema before its first write.
    pub async fn ensure_schema(&self, schema: &Schema) -> StorageResult<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        for (field_name, field) in &schema.runtime_fields {
            let molecule_uuid = field.common().molecule_uuid().cloned().unwrap_or_else(|| {
                crate::atom::deterministic_molecule_uuid(&schema.name, field_name)
            });
            self.ensure(&molecule_uuid).await?;
        }
        Ok(())
    }

    /// Warm present bundles for a catalog read. Missing rows mean legacy data.
    pub async fn load_schema(&self, schema: &Schema) -> StorageResult<()> {
        for (field_name, field) in &schema.runtime_fields {
            let molecule_uuid = field.common().molecule_uuid().cloned().unwrap_or_else(|| {
                crate::atom::deterministic_molecule_uuid(&schema.name, field_name)
            });
            let _ = self.load(&molecule_uuid).await?;
        }
        Ok(())
    }

    fn cached(&self, molecule_uuid: &str) -> Option<MoleculeKeyBundle> {
        self.cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(molecule_uuid)
            .cloned()
    }

    fn remember(&self, molecule_uuid: &str, bundle: MoleculeKeyBundle) {
        self.cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(molecule_uuid.to_string(), bundle);
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
    let plaintext = open_at_rest(wrapping_key, wrap.sealed_bundle.as_bytes()).map_err(|error| {
        StorageError::InvalidOperation(format!("unwrap molecule key bundle: {error}"))
    })?;
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

fn metadata_key(molecule_uuid: &str) -> String {
    format!("mkb:{molecule_uuid}")
}

fn wrap_key_for(molecule_uuid: &str, domain: &str) -> String {
    let domain_hash = Sha256::digest(domain.as_bytes());
    format!("mkw:{molecule_uuid}:{domain_hash:x}")
}

fn schema_molecule_uuids(schema: &Schema) -> Vec<String> {
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
