//! Staged pack, atomic write and fetch/verify error mapping helpers.

use super::*;

pub(super) struct StagedLoadedPack {
    pub(super) loaded: LoadedResolverPack,
    pub(super) resolver_config_bytes: Vec<u8>,
    pub(super) schema_snapshot_bytes: Vec<u8>,
    pub(super) embedding_artifact_bytes: Vec<u8>,
}

pub(super) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(tmp, path)
}

pub(super) fn fetch_error_telemetry(err: &ResolverPackFetchError) -> &'static str {
    match err {
        ResolverPackFetchError::Transport(_) => "transport_error",
        ResolverPackFetchError::Timeout => "timeout",
        ResolverPackFetchError::Truncated => "truncated",
        ResolverPackFetchError::ResponseTooLarge => "response_too_large",
        ResolverPackFetchError::HttpStatus(_) => "http_status",
        ResolverPackFetchError::InvalidUrl(_) => "invalid_url",
        ResolverPackFetchError::OriginRejected(_) => "origin_rejected",
    }
}

pub(super) fn fallback_reason_for_fetch_error(
    err: &ResolverPackFetchError,
) -> ResolverPackFallbackReason {
    match err {
        ResolverPackFetchError::OriginRejected(_) | ResolverPackFetchError::InvalidUrl(_) => {
            ResolverPackFallbackReason::ArtifactFetchFailed
        }
        _ => ResolverPackFallbackReason::ArtifactFetchFailed,
    }
}

pub(super) fn import_pack_embeddings(
    artifact: &EmbeddingArtifact,
) -> Result<ImportedResolverPackEmbeddings, ResolverPackConsumerError> {
    Ok(ImportedResolverPackEmbeddings {
        descriptive_names: collect_embeddings(
            "descriptive_names",
            artifact.dimensions,
            &artifact.descriptive_names,
        )?,
        schema_field_contexts: collect_embeddings(
            "schema_field_contexts",
            artifact.dimensions,
            &artifact.schema_field_contexts,
        )?,
        canonical_fields: collect_embeddings(
            "canonical_fields",
            artifact.dimensions,
            &artifact.canonical_fields,
        )?,
    })
}

pub(super) fn collect_embeddings(
    class: &'static str,
    dimensions: u32,
    records: &[crate::resolver_pack::EmbeddingVectorRecord],
) -> Result<BTreeMap<String, Vec<f32>>, ResolverPackConsumerError> {
    let mut out = BTreeMap::new();
    for record in records {
        if record.vector.len() != dimensions as usize {
            return Err(ResolverPackConsumerError::EmbeddingDimensionMismatch {
                class,
                target: record.target_id.clone(),
                expected: dimensions,
                actual: record.vector.len(),
            });
        }
        out.insert(record.target_id.clone(), record.vector.clone());
    }
    Ok(out)
}

pub(super) fn fallback_reason_for_error(
    err: &ResolverPackConsumerError,
) -> ResolverPackFallbackReason {
    match err {
        ResolverPackConsumerError::Fetch(fetch) => fallback_reason_for_fetch_error(fetch),
        ResolverPackConsumerError::MissingArtifact(_) => {
            ResolverPackFallbackReason::ArtifactFetchFailed
        }
        ResolverPackConsumerError::ManifestJson(_)
        | ResolverPackConsumerError::BadGeneratedAt(_)
        | ResolverPackConsumerError::EmbeddingDimensionMismatch { .. } => {
            ResolverPackFallbackReason::ArtifactVerificationFailed
        }
        ResolverPackConsumerError::CacheIo(_) => ResolverPackFallbackReason::ArtifactFetchFailed,
        ResolverPackConsumerError::StaleManifest => ResolverPackFallbackReason::StalePack,
        ResolverPackConsumerError::Verify(verify) => fallback_reason_for_verify_error(verify),
    }
}

pub(super) fn fallback_reason_for_verify_error(
    err: &ResolverPackVerifyError,
) -> ResolverPackFallbackReason {
    match err {
        ResolverPackVerifyError::UnsupportedFormatVersion(_)
        | ResolverPackVerifyError::UnsupportedResolverContractVersion(_) => {
            ResolverPackFallbackReason::UnsupportedAbi
        }
        ResolverPackVerifyError::WrongEnv { .. }
        | ResolverPackVerifyError::SignatureEnvMismatch { .. } => {
            ResolverPackFallbackReason::WrongEnv
        }
        ResolverPackVerifyError::EmbedderMismatch { .. }
        | ResolverPackVerifyError::EmbeddingArtifactEmbedderMismatch { .. } => {
            ResolverPackFallbackReason::IncompatibleEmbedder
        }
        ResolverPackVerifyError::WrongPurpose => ResolverPackFallbackReason::WrongPurpose,
        ResolverPackVerifyError::HashMismatch { .. }
        | ResolverPackVerifyError::MalformedHash { .. } => ResolverPackFallbackReason::HashMismatch,
        ResolverPackVerifyError::PolicyAllowsLocalCanonicalSchemaCreation
        | ResolverPackVerifyError::PolicyVersionMismatch { .. }
        | ResolverPackVerifyError::InvalidResolverConfig(_)
        | ResolverPackVerifyError::AlgorithmMismatch { .. }
        | ResolverPackVerifyError::ConfigContractVersionMismatch { .. } => {
            ResolverPackFallbackReason::PolicyRejected
        }
        ResolverPackVerifyError::BadEnvelope(VerifyError::Expired) => {
            ResolverPackFallbackReason::StalePack
        }
        ResolverPackVerifyError::BadEnvelope(_) | ResolverPackVerifyError::PayloadHashMismatch => {
            ResolverPackFallbackReason::BadSignature
        }
        _ => ResolverPackFallbackReason::ArtifactVerificationFailed,
    }
}
