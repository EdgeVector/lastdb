//! Rollback validation and the key plan for a latest-pointer rollback.

use super::upload::*;
use super::{read_file, RollbackArgs};
use app_identity_crypto::Env;
use schema_service_core::resolver_pack::*;

/// Validate a rollback manifest and return the embedder id it targets.
///
/// With all three artifacts the manifest is verified against them; otherwise
/// only the pointer-level checks run.
pub(super) fn rollback_embedding_id(
    args: &RollbackArgs,
    manifest: &ResolverPackManifest,
    trusted_keys: &[schema_service_core::resolver_pack::TrustedResolverPackKey],
    env: Env,
) -> Result<String, String> {
    let (Some(resolver_config), Some(schema_snapshot), Some(embedding_artifact)) = (
        args.resolver_config.as_ref(),
        args.schema_snapshot.as_ref(),
        args.embedding_artifact.as_ref(),
    ) else {
        validate_manifest_for_pointer_only(manifest, trusted_keys, env)?;
        return Ok(manifest.embedder_id.clone());
    };
    let resolver_config = read_file("resolver_config", resolver_config)?;
    let schema_snapshot = read_file("schema_snapshot", schema_snapshot)?;
    let embedding_artifact = read_file("embedding_artifact", embedding_artifact)?;
    let embedding: EmbeddingArtifact = parse_embedding_artifact(&embedding_artifact)
        .map_err(|e| format!("embedding artifact parse failed: {e}"))?;
    verify_resolver_pack_manifest(
        manifest,
        &resolver_config,
        &schema_snapshot,
        &embedding_artifact,
        trusted_keys,
        env,
        &embedding.embedder_id,
    )
    .map_err(|e| format!("rollback manifest validation failed: {e}"))?;
    Ok(embedding.embedder_id)
}

/// Keys a rollback touches: the manifest, its artifacts, and the latest pointer.
pub(super) fn rollback_plan(
    env: Env,
    manifest: &ResolverPackManifest,
    manifest_bytes: &[u8],
    embedding_id: &str,
) -> PublishPlan {
    let pointer_key = latest_compatible_manifest_pointer_key(
        env,
        manifest.resolver_contract_version,
        &manifest.algorithm.id,
        manifest.algorithm.version,
        embedding_id,
    );
    PublishPlan {
        manifest_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::Manifest,
            &artifact_sha256_hex(manifest_bytes),
        ),
        resolver_config_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::ResolverConfig,
            &manifest.resolver_config_hash,
        ),
        schema_snapshot_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::SchemaSnapshot,
            &manifest.schema_snapshot_hash,
        ),
        embedding_artifact_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::EmbeddingArtifact,
            &manifest.embedding_artifact_hash,
        ),
        latest_manifest_pointer_key: pointer_key,
    }
}
