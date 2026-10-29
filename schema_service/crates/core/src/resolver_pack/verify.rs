use super::*;

pub fn artifact_sha256_hex(bytes: &[u8]) -> String {
    schema_types::hex::sha256_hex(bytes)
}

pub fn parse_manifest(bytes: &[u8]) -> Result<ResolverPackManifest, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn parse_schema_snapshot_artifact(
    bytes: &[u8],
) -> Result<SchemaSnapshotArtifact, serde_json::Error> {
    let snapshot = serde_json::from_slice(bytes)?;
    crate::resolver_pack_vocabulary::validate(&snapshot).map_err(serde::de::Error::custom)?;
    Ok(snapshot)
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
    schema_types::rfc3339::require_rfc3339(&manifest.expires_at)
        .map_err(ResolverPackVerifyError::BadExpiresAt)?;
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
