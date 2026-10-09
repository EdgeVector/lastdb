//! Compact signed registry index for local-first schema dedup caches.

use std::collections::BTreeMap;

use app_identity_crypto::{canonicalize, key_id, sign, SigningKey};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};
use serde::{Deserialize, Serialize};

use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;

pub const REGISTRY_INDEX_FORMAT_VERSION: u32 = 1;
pub const REGISTRY_INDEX_SIGNING_KEY_ENV: &str = "REGISTRY_INDEX_SIGNING_KEY_B64";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryIndexEnvelope {
    pub format_version: u32,
    pub full: bool,
    pub registry_version: u64,
    pub merkle_root: String,
    pub embedder_version: String,
    pub entries: Vec<RegistryIndexEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub supersessions: BTreeMap<String, String>,
    pub signature: RegistryIndexSignature,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryIndexEntry {
    pub identity_hash: String,
    pub descriptive_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_app_id: Option<String>,
    pub source: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_descriptions: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryIndexSignature {
    pub alg: String,
    pub key_id: String,
    pub sig: String,
}

impl SchemaServiceState {
    pub fn export_registry_index(&self) -> FoldDbResult<RegistryIndexEnvelope> {
        let registry_version = self.current_state_version();
        let embedder_version = self.embedder.embedder_id().to_string();
        let (entries, supersessions) = self.registry_index_entries()?;
        let merkle_root = registry_index_merkle_root(&entries)?;

        let unsigned = RegistryIndexUnsigned {
            format_version: REGISTRY_INDEX_FORMAT_VERSION,
            full: true,
            registry_version,
            merkle_root,
            embedder_version,
            entries,
            supersessions,
        };
        let signature = sign_registry_index(&unsigned)?;

        Ok(RegistryIndexEnvelope {
            format_version: unsigned.format_version,
            full: unsigned.full,
            registry_version: unsigned.registry_version,
            merkle_root: unsigned.merkle_root,
            embedder_version: unsigned.embedder_version,
            entries: unsigned.entries,
            supersessions: unsigned.supersessions,
            signature,
        })
    }

    fn registry_index_entries(
        &self,
    ) -> FoldDbResult<(Vec<RegistryIndexEntry>, BTreeMap<String, String>)> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        let mut entries = Vec::with_capacity(schemas.len());
        let mut supersessions = BTreeMap::new();

        for (schema_name, schema) in schemas.iter() {
            let identity_hash = schema
                .identity_hash
                .clone()
                .unwrap_or_else(|| schema_name.clone());

            if let Some(canonical) = schema.superseded_by.as_deref().filter(|s| !s.is_empty()) {
                supersessions.insert(identity_hash, canonical.to_string());
                continue;
            }

            entries.push(registry_index_entry(schema, schema_name));
        }

        entries.sort_by(|a, b| {
            a.identity_hash
                .cmp(&b.identity_hash)
                .then_with(|| a.owner_app_id.cmp(&b.owner_app_id))
                .then_with(|| a.descriptive_name.cmp(&b.descriptive_name))
        });

        Ok((entries, supersessions))
    }
}

#[derive(Debug, Serialize)]
struct RegistryIndexUnsigned {
    format_version: u32,
    full: bool,
    registry_version: u64,
    merkle_root: String,
    embedder_version: String,
    entries: Vec<RegistryIndexEntry>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    supersessions: BTreeMap<String, String>,
}

fn registry_index_entry(schema: &Schema, schema_name: &str) -> RegistryIndexEntry {
    let mut fields = schema.fields.clone().unwrap_or_default();
    fields.sort();
    fields.dedup();

    let field_descriptions = fields
        .iter()
        .filter_map(|field| {
            schema
                .field_descriptions
                .get(field)
                .map(|description| (field.clone(), description.clone()))
        })
        .collect();

    RegistryIndexEntry {
        identity_hash: schema
            .identity_hash
            .clone()
            .unwrap_or_else(|| schema_name.to_string()),
        descriptive_name: schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone()),
        owner_app_id: schema
            .owner_app_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        source: serde_json::to_value(schema.source)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "user".to_string()),
        fields,
        field_descriptions,
        purpose_statement: schema
            .purpose_statement
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    }
}

fn registry_index_merkle_root(entries: &[RegistryIndexEntry]) -> FoldDbResult<String> {
    let mut leaf_hashes = entries
        .iter()
        .map(|entry| {
            let value = serde_json::to_value(entry).map_err(|e| {
                FoldDbError::Config(format!("failed to serialize registry index entry: {e}"))
            })?;
            let canonical = canonicalize(&value).map_err(|e| {
                FoldDbError::Config(format!("failed to canonicalize registry index entry: {e}"))
            })?;
            Ok(blake3::hash(&canonical).to_hex().to_string())
        })
        .collect::<FoldDbResult<Vec<_>>>()?;

    leaf_hashes.sort();
    let mut hasher = blake3::Hasher::new();
    for leaf in leaf_hashes {
        hasher.update(leaf.as_bytes());
        hasher.update(b"\n");
    }
    Ok(format!("b3:{}", hasher.finalize().to_hex()))
}

fn sign_registry_index(unsigned: &RegistryIndexUnsigned) -> FoldDbResult<RegistryIndexSignature> {
    let signing_key = registry_index_signing_key()?;
    let payload = serde_json::to_value(unsigned).map_err(|e| {
        FoldDbError::Config(format!(
            "failed to serialize registry index for signing: {e}"
        ))
    })?;
    let canonical = canonicalize(&payload).map_err(|e| {
        FoldDbError::Config(format!(
            "failed to canonicalize registry index for signing: {e}"
        ))
    })?;
    let sig = sign(&signing_key, &canonical);
    Ok(RegistryIndexSignature {
        alg: "Ed25519".to_string(),
        key_id: key_id(&signing_key.verifying_key()),
        sig: BASE64.encode(sig),
    })
}

fn registry_index_signing_key() -> FoldDbResult<SigningKey> {
    match std::env::var(REGISTRY_INDEX_SIGNING_KEY_ENV) {
        Ok(raw) if !raw.trim().is_empty() => {
            let bytes = BASE64.decode(raw.trim().as_bytes()).map_err(|_| {
                FoldDbError::Config(format!("{REGISTRY_INDEX_SIGNING_KEY_ENV} is not base64"))
            })?;
            let seed: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                FoldDbError::Config(format!(
                    "{REGISTRY_INDEX_SIGNING_KEY_ENV} must decode to a 32-byte Ed25519 seed"
                ))
            })?;
            Ok(SigningKey::from_bytes(&seed))
        }
        _ => {
            if crate::app_identity::deployment_env_from_process() == app_identity_crypto::Env::Prod
            {
                return Err(FoldDbError::Config(format!(
                    "{REGISTRY_INDEX_SIGNING_KEY_ENV} must be set in prod"
                )));
            }
            Ok(SigningKey::from_bytes(&[0x42; 32]))
        }
    }
}
