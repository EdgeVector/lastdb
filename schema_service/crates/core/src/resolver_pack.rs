//! Signed schema resolver pack artifact contract (format_version 2).
//!
//! Resolver packs are immutable bundles published to R2: a declarative
//! `resolver_config.json`, a schema snapshot, and a registry embedding
//! artifact. The manifest is the trust root for the bundle: it names every
//! artifact by SHA-256 and carries an app-identity-style Ed25519 envelope
//! whose payload is the canonical manifest JSON without the `signature`
//! field.

use app_identity_crypto::{
    compute_payload_hash, verify_envelope, verifying_key_from_base64, Env, KeyParseError, Purpose,
    SignatureEnvelope, VerifyError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::resolver_config::{
    AlgorithmSelector, ResolverConfig, ResolverConfigError, RESOLVER_CONFIG_FORMAT_VERSION,
};

pub const RESOLVER_PACK_FORMAT_VERSION: u32 = 2;
pub const RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION: u32 = 1;
pub const RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION: u32 = 1;
/// Supported native resolver input/output contract version.
pub const SUPPORTED_RESOLVER_CONTRACT_VERSION: u32 = 1;

const R2_PREFIX: &str = "schema-resolver-packs";

/// Public key pinned by clients for resolver-pack release manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedResolverPackKey {
    pub key_id: String,
    pub public_key_b64: String,
}

/// `manifest.json` wire format for one resolver pack (v2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolverPackManifest {
    pub format_version: u32,
    pub resolver_contract_version: u32,
    pub algorithm: AlgorithmSelector,
    pub env: Env,
    pub embedder_id: String,
    /// RFC 3339 UTC timestamp produced by the pack builder.
    pub generated_at: String,
    /// RFC 3339 UTC pack expiry claim (dashboard/client freshness).
    pub expires_at: String,
    pub schema_snapshot_hash: String,
    pub embedding_artifact_hash: String,
    pub resolver_config_hash: String,
    /// From `resolver_config.policy_version` for dashboards/telemetry.
    pub policy_version: String,
    pub counts: ResolverPackCounts,
    pub artifact_sizes: ResolverPackArtifactSizes,
    /// Must equal `signature.key_id`; duplicated so object stores and
    /// dashboards can route on the key id without decoding envelope claims.
    pub signing_key_id: String,
    pub signature: SignatureEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolverPackCounts {
    pub schemas: u32,
    pub canonical_fields: u32,
    pub descriptive_name_embeddings: u32,
    pub schema_field_context_embeddings: u32,
    pub canonical_field_embeddings: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolverPackArtifactSizes {
    pub manifest_bytes: u64,
    pub schema_snapshot_bytes: u64,
    pub embedding_artifact_bytes: u64,
    pub resolver_config_bytes: u64,
}

/// Schema snapshot artifact consumed by local nodes and the native resolver.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaSnapshotArtifact {
    pub format_version: u32,
    pub captured_at: String,
    pub source_snapshot_format_version: u32,
    pub schemas: Vec<ResolverSchemaRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub canonical_fields: Vec<ResolverCanonicalFieldRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolverSchemaRecord {
    pub schema_id: String,
    pub descriptive_name: String,
    pub purpose_statement: String,
    pub lifecycle: SchemaLifecycle,
    pub fields: Vec<ResolverFieldRecord>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaLifecycle {
    Seed,
    Active,
    Deprecated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResolverFieldRecord {
    pub field_id: String,
    pub field_name: String,
    pub field_type: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub canonical_field_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolverCanonicalFieldRecord {
    pub canonical_field_id: String,
    pub description: String,
    pub field_type: String,
}

/// Service-computed registry embeddings. Local nodes compute only proposal
/// embeddings and compare them with these downloaded vectors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingArtifact {
    pub format_version: u32,
    pub embedder_id: String,
    pub generated_at: String,
    pub dimensions: u32,
    pub descriptive_names: Vec<EmbeddingVectorRecord>,
    pub schema_field_contexts: Vec<EmbeddingVectorRecord>,
    pub canonical_fields: Vec<EmbeddingVectorRecord>,
}

/// Shared with the pure match engine (`schema_core_resolver`).
pub use schema_core_resolver::EmbeddingVectorRecord;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResolverPackVerifyError {
    #[error("unsupported resolver pack format version: {0}")]
    UnsupportedFormatVersion(u32),
    #[error("unsupported resolver contract version: {0}")]
    UnsupportedResolverContractVersion(u32),
    #[error("manifest env {manifest:?} does not match expected env {expected:?}")]
    WrongEnv { manifest: Env, expected: Env },
    #[error("signature env {signature:?} does not match manifest env {manifest:?}")]
    SignatureEnvMismatch { signature: Env, manifest: Env },
    #[error("manifest embedder_id {manifest:?} does not match expected embedder_id {expected:?}")]
    EmbedderMismatch { manifest: String, expected: String },
    #[error("manifest signing_key_id does not match signature.key_id")]
    SigningKeyIdMismatch,
    #[error("signature purpose is not schema_resolver_pack")]
    WrongPurpose,
    #[error("untrusted resolver-pack signing key id: {0}")]
    UntrustedSigningKey(String),
    #[error("trusted resolver-pack public key could not be parsed: {0}")]
    BadTrustedKey(#[from] KeyParseError),
    #[error("resolver-pack signature verification failed: {0}")]
    BadEnvelope(#[from] VerifyError),
    #[error("{artifact} hash mismatch: manifest {expected}, actual {actual}")]
    HashMismatch {
        artifact: &'static str,
        expected: String,
        actual: String,
    },
    #[error("manifest signature payload_hash does not match manifest payload")]
    PayloadHashMismatch,
    #[error("artifact hash is not lowercase hex sha256: {field}")]
    MalformedHash { field: &'static str },
    #[error("schema snapshot artifact format version {0} is unsupported")]
    UnsupportedSchemaSnapshotFormat(u32),
    #[error("embedding artifact format version {0} is unsupported")]
    UnsupportedEmbeddingArtifactFormat(u32),
    #[error("resolver config format version {0} is unsupported")]
    UnsupportedResolverConfigFormat(u32),
    #[error("embedding artifact embedder_id {artifact:?} does not match manifest embedder_id {manifest:?}")]
    EmbeddingArtifactEmbedderMismatch { artifact: String, manifest: String },
    #[error("resolver config policy_version {artifact:?} does not match manifest policy_version {manifest:?}")]
    PolicyVersionMismatch { artifact: String, manifest: String },
    #[error("resolver config algorithm {config_id}@{config_version} does not match manifest algorithm {manifest_id}@{manifest_version}")]
    AlgorithmMismatch {
        config_id: String,
        config_version: u32,
        manifest_id: String,
        manifest_version: u32,
    },
    #[error("resolver config contract version {config} does not match manifest contract version {manifest}")]
    ConfigContractVersionMismatch { config: u32, manifest: u32 },
    #[error("policy permits local canonical schema creation")]
    PolicyAllowsLocalCanonicalSchemaCreation,
    #[error("resolver config validation failed: {0}")]
    InvalidResolverConfig(String),
    #[error("artifact JSON parse failed: {0}")]
    ArtifactJson(String),
    #[error("manifest canonicalization failed")]
    ManifestCanonicalization,
    #[error("manifest expires_at is not a valid RFC 3339 timestamp: {0}")]
    BadExpiresAt(String),
}

pub fn artifact_sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}

pub fn parse_manifest(bytes: &[u8]) -> Result<ResolverPackManifest, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn parse_schema_snapshot_artifact(
    bytes: &[u8],
) -> Result<SchemaSnapshotArtifact, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn parse_embedding_artifact(bytes: &[u8]) -> Result<EmbeddingArtifact, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn parse_resolver_config(bytes: &[u8]) -> Result<ResolverConfig, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn verify_resolver_pack_manifest(
    manifest: &ResolverPackManifest,
    resolver_config_bytes: &[u8],
    schema_snapshot: &[u8],
    embedding_artifact: &[u8],
    trusted_keys: &[TrustedResolverPackKey],
    expected_env: Env,
    expected_embedder_id: &str,
) -> Result<(), ResolverPackVerifyError> {
    validate_manifest_claims(manifest, expected_env, expected_embedder_id)?;
    verify_artifact_hash(
        "resolver_config",
        "resolver_config_hash",
        &manifest.resolver_config_hash,
        resolver_config_bytes,
    )?;
    verify_artifact_hash(
        "schema_snapshot",
        "schema_snapshot_hash",
        &manifest.schema_snapshot_hash,
        schema_snapshot,
    )?;
    verify_artifact_hash(
        "embedding_artifact",
        "embedding_artifact_hash",
        &manifest.embedding_artifact_hash,
        embedding_artifact,
    )?;

    let schema_snapshot: SchemaSnapshotArtifact =
        parse_schema_snapshot_artifact(schema_snapshot)
            .map_err(|e| ResolverPackVerifyError::ArtifactJson(e.to_string()))?;
    if schema_snapshot.format_version != RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION {
        return Err(ResolverPackVerifyError::UnsupportedSchemaSnapshotFormat(
            schema_snapshot.format_version,
        ));
    }

    let embedding_artifact: EmbeddingArtifact = parse_embedding_artifact(embedding_artifact)
        .map_err(|e| ResolverPackVerifyError::ArtifactJson(e.to_string()))?;
    if embedding_artifact.format_version != RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION {
        return Err(ResolverPackVerifyError::UnsupportedEmbeddingArtifactFormat(
            embedding_artifact.format_version,
        ));
    }
    if embedding_artifact.embedder_id != manifest.embedder_id {
        return Err(ResolverPackVerifyError::EmbeddingArtifactEmbedderMismatch {
            artifact: embedding_artifact.embedder_id,
            manifest: manifest.embedder_id.clone(),
        });
    }

    let config: ResolverConfig = parse_resolver_config(resolver_config_bytes)
        .map_err(|e| ResolverPackVerifyError::ArtifactJson(e.to_string()))?;
    if config.format_version != RESOLVER_CONFIG_FORMAT_VERSION {
        return Err(ResolverPackVerifyError::UnsupportedResolverConfigFormat(
            config.format_version,
        ));
    }
    if config.resolver_contract_version != manifest.resolver_contract_version {
        return Err(ResolverPackVerifyError::ConfigContractVersionMismatch {
            config: config.resolver_contract_version,
            manifest: manifest.resolver_contract_version,
        });
    }
    if config.algorithm.id != manifest.algorithm.id
        || config.algorithm.version != manifest.algorithm.version
    {
        return Err(ResolverPackVerifyError::AlgorithmMismatch {
            config_id: config.algorithm.id.clone(),
            config_version: config.algorithm.version,
            manifest_id: manifest.algorithm.id.clone(),
            manifest_version: manifest.algorithm.version,
        });
    }
    if config.policy_version != manifest.policy_version {
        return Err(ResolverPackVerifyError::PolicyVersionMismatch {
            artifact: config.policy_version,
            manifest: manifest.policy_version.clone(),
        });
    }
    if let Err(err) = config.validate() {
        return Err(match err {
            ResolverConfigError::CanonicalCreationForbidden => {
                ResolverPackVerifyError::PolicyAllowsLocalCanonicalSchemaCreation
            }
            other => ResolverPackVerifyError::InvalidResolverConfig(other.to_string()),
        });
    }

    verify_manifest_signature(manifest, trusted_keys)
}

pub fn resolver_pack_artifact_key(
    env: Env,
    artifact: ResolverPackArtifactKind,
    hash: &str,
) -> String {
    let filename = match artifact {
        ResolverPackArtifactKind::Manifest => "manifest.json",
        ResolverPackArtifactKind::SchemaSnapshot => "schema_snapshot.json",
        ResolverPackArtifactKind::EmbeddingArtifact => "embedding_artifact.json",
        ResolverPackArtifactKind::ResolverConfig => "resolver_config.json",
    };
    format!(
        "{R2_PREFIX}/{}/artifacts/sha256/{hash}/{filename}",
        env_segment(env)
    )
}

pub fn latest_compatible_manifest_pointer_key(
    env: Env,
    contract_version: u32,
    algorithm_id: &str,
    algorithm_version: u32,
    embedder_id: &str,
) -> String {
    let embedder_digest = artifact_sha256_hex(embedder_id.as_bytes());
    format!(
        "{R2_PREFIX}/{}/latest/contract-v{contract_version}/{algorithm_id}-v{algorithm_version}/embedder-sha256-{embedder_digest}/manifest.json",
        env_segment(env)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolverPackArtifactKind {
    Manifest,
    SchemaSnapshot,
    EmbeddingArtifact,
    ResolverConfig,
}

fn validate_manifest_claims(
    manifest: &ResolverPackManifest,
    expected_env: Env,
    expected_embedder_id: &str,
) -> Result<(), ResolverPackVerifyError> {
    if manifest.format_version != RESOLVER_PACK_FORMAT_VERSION {
        return Err(ResolverPackVerifyError::UnsupportedFormatVersion(
            manifest.format_version,
        ));
    }
    if manifest.resolver_contract_version != SUPPORTED_RESOLVER_CONTRACT_VERSION {
        return Err(ResolverPackVerifyError::UnsupportedResolverContractVersion(
            manifest.resolver_contract_version,
        ));
    }
    if manifest.env != expected_env {
        return Err(ResolverPackVerifyError::WrongEnv {
            manifest: manifest.env,
            expected: expected_env,
        });
    }
    if manifest.signature.env != manifest.env {
        return Err(ResolverPackVerifyError::SignatureEnvMismatch {
            signature: manifest.signature.env,
            manifest: manifest.env,
        });
    }
    if manifest.embedder_id != expected_embedder_id {
        return Err(ResolverPackVerifyError::EmbedderMismatch {
            manifest: manifest.embedder_id.clone(),
            expected: expected_embedder_id.to_string(),
        });
    }
    if manifest.signing_key_id != manifest.signature.key_id {
        return Err(ResolverPackVerifyError::SigningKeyIdMismatch);
    }
    if manifest.signature.purpose != Purpose::SchemaResolverPack {
        return Err(ResolverPackVerifyError::WrongPurpose);
    }
    // Reject empty / unparseable pack expiry claims early.
    chrono::DateTime::parse_from_rfc3339(&manifest.expires_at)
        .map_err(|e| ResolverPackVerifyError::BadExpiresAt(e.to_string()))?;
    validate_hash_field("resolver_config_hash", &manifest.resolver_config_hash)?;
    validate_hash_field("schema_snapshot_hash", &manifest.schema_snapshot_hash)?;
    validate_hash_field("embedding_artifact_hash", &manifest.embedding_artifact_hash)?;
    Ok(())
}

fn verify_manifest_signature(
    manifest: &ResolverPackManifest,
    trusted_keys: &[TrustedResolverPackKey],
) -> Result<(), ResolverPackVerifyError> {
    let trusted = trusted_keys
        .iter()
        .find(|key| key.key_id == manifest.signing_key_id)
        .ok_or_else(|| {
            ResolverPackVerifyError::UntrustedSigningKey(manifest.signing_key_id.clone())
        })?;
    let verifying_key = verifying_key_from_base64(&trusted.public_key_b64)?;
    let payload = manifest_payload_value(manifest)?;
    let expected_payload_hash = compute_payload_hash(&payload)
        .map_err(|_| ResolverPackVerifyError::ManifestCanonicalization)?;
    if manifest.signature.payload_hash != expected_payload_hash {
        return Err(ResolverPackVerifyError::PayloadHashMismatch);
    }
    verify_envelope(&verifying_key, &manifest.signature)?;
    Ok(())
}

pub fn manifest_payload_value(
    manifest: &ResolverPackManifest,
) -> Result<Value, ResolverPackVerifyError> {
    let mut payload = serde_json::to_value(manifest)
        .map_err(|_| ResolverPackVerifyError::ManifestCanonicalization)?;
    if let Value::Object(map) = &mut payload {
        map.remove("signature");
        Ok(payload)
    } else {
        Err(ResolverPackVerifyError::ManifestCanonicalization)
    }
}

fn verify_artifact_hash(
    artifact: &'static str,
    field: &'static str,
    expected: &str,
    bytes: &[u8],
) -> Result<(), ResolverPackVerifyError> {
    validate_hash_field(field, expected)?;
    let actual = artifact_sha256_hex(bytes);
    if expected != actual {
        return Err(ResolverPackVerifyError::HashMismatch {
            artifact,
            expected: expected.to_string(),
            actual,
        });
    }
    Ok(())
}

fn validate_hash_field(field: &'static str, hash: &str) -> Result<(), ResolverPackVerifyError> {
    if hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(ResolverPackVerifyError::MalformedHash { field })
    }
}

fn env_segment(env: Env) -> &'static str {
    match env {
        Env::Dev => "dev",
        Env::Prod => "prod",
    }
}
