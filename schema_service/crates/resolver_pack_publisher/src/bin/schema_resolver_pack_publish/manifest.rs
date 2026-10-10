//! Pack layout on disk, the signed manifest, and the default resolver config.

use std::fs;
use std::path::Path;

use super::artifacts::*;
use super::upload::PublishPlan;
use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, Env, Purpose, SignatureEnvelope, SigningKey,
    ALG_ED25519, ENVELOPE_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use schema_service_core::resolver_config::{AlgorithmSelector, ResolverConfig};
use schema_service_core::resolver_pack::{
    artifact_sha256_hex, manifest_payload_value, parse_embedding_artifact, parse_resolver_config,
    parse_schema_snapshot_artifact, EmbeddingArtifact, ResolverPackArtifactSizes,
    ResolverPackCounts, ResolverPackManifest, SchemaSnapshotArtifact, RESOLVER_PACK_FORMAT_VERSION,
};

pub(crate) fn write_pack_local_layout(
    root: &Path,
    plan: &PublishPlan,
    manifest_bytes: &[u8],
    inputs: PackInputs<'_>,
) -> Result<(), String> {
    let write = |rel: &str, bytes: &[u8]| -> Result<(), String> {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
        }
        fs::write(&path, bytes).map_err(|e| format!("write {}: {e}", path.display()))
    };
    write(&plan.resolver_config_key, inputs.resolver_config)?;
    write(&plan.schema_snapshot_key, inputs.schema_snapshot)?;
    write(&plan.embedding_artifact_key, inputs.embedding_artifact)?;
    write(&plan.manifest_key, manifest_bytes)?;
    write(&plan.latest_manifest_pointer_key, manifest_bytes)?;
    Ok(())
}

pub(crate) fn build_signed_manifest(
    inputs: PackInputs<'_>,
    env: Env,
    signing_key: &SigningKey,
    issued_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
) -> Result<ResolverPackManifest, String> {
    let schema_snapshot: SchemaSnapshotArtifact =
        parse_schema_snapshot_artifact(inputs.schema_snapshot)
            .map_err(|e| format!("schema snapshot parse failed: {e}"))?;
    let embedding: EmbeddingArtifact = parse_embedding_artifact(inputs.embedding_artifact)
        .map_err(|e| format!("embedding artifact parse failed: {e}"))?;
    let config = parse_resolver_config(inputs.resolver_config)
        .map_err(|e| format!("resolver_config parse failed: {e}"))?;
    config
        .validate()
        .map_err(|e| format!("resolver_config invalid: {e}"))?;
    let signing_key_id = key_id(&signing_key.verifying_key());
    let pack_expires_at = expires_at.unwrap_or(issued_at + Duration::days(90));

    let counts = ResolverPackCounts {
        schemas: schema_snapshot.schemas.len() as u32,
        canonical_fields: schema_snapshot.canonical_fields.len() as u32,
        descriptive_name_embeddings: embedding.descriptive_names.len() as u32,
        schema_field_context_embeddings: embedding.schema_field_contexts.len() as u32,
        canonical_field_embeddings: embedding.canonical_fields.len() as u32,
    };

    let mut manifest_bytes = 0;
    let mut signed = None;
    for _ in 0..8 {
        let placeholder = SignatureEnvelope {
            version: ENVELOPE_VERSION,
            purpose: Purpose::SchemaResolverPack,
            alg: ALG_ED25519.to_string(),
            key_id: signing_key_id.clone(),
            issued_at,
            expires_at,
            env,
            payload_hash: String::new(),
            sig: None,
        };
        let mut manifest = ResolverPackManifest {
            format_version: RESOLVER_PACK_FORMAT_VERSION,
            resolver_contract_version: config.resolver_contract_version,
            algorithm: AlgorithmSelector {
                id: config.algorithm.id.clone(),
                version: config.algorithm.version,
            },
            env,
            embedder_id: embedding.embedder_id.clone(),
            generated_at: issued_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            expires_at: pack_expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            schema_snapshot_hash: artifact_sha256_hex(inputs.schema_snapshot),
            embedding_artifact_hash: artifact_sha256_hex(inputs.embedding_artifact),
            resolver_config_hash: artifact_sha256_hex(inputs.resolver_config),
            policy_version: config.policy_version.clone(),
            counts: counts.clone(),
            artifact_sizes: ResolverPackArtifactSizes {
                manifest_bytes,
                schema_snapshot_bytes: inputs.schema_snapshot.len() as u64,
                embedding_artifact_bytes: inputs.embedding_artifact.len() as u64,
                resolver_config_bytes: inputs.resolver_config.len() as u64,
            },
            signing_key_id: signing_key_id.clone(),
            signature: placeholder,
        };
        let payload = manifest_payload_value(&manifest).map_err(|e| e.to_string())?;
        manifest.signature.payload_hash =
            compute_payload_hash(&payload).map_err(|e| e.to_string())?;
        manifest.signature =
            sign_envelope(signing_key, manifest.signature.clone()).map_err(|e| e.to_string())?;
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
        let next_size = bytes.len() as u64;
        signed = Some(manifest);
        if next_size == manifest_bytes {
            break;
        }
        manifest_bytes = next_size;
    }
    signed.ok_or_else(|| "failed to sign manifest".to_string())
}

pub(crate) fn default_resolver_config(policy_version: &str) -> ResolverConfig {
    let mut config = ResolverConfig::embedding_beam_shadow_defaults();
    config.policy_version = policy_version.to_string();
    config
}
