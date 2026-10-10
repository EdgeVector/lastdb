//! Publish planning, receipts, and the S3 uploader.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use super::artifacts::PackInputs;
use app_identity_crypto::{
    compute_payload_hash, verify_envelope, verifying_key_from_base64, Env, Purpose,
};
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use schema_service_core::resolver_pack::{
    artifact_sha256_hex, latest_compatible_manifest_pointer_key, manifest_payload_value,
    resolver_pack_artifact_key, ResolverPackArtifactKind, ResolverPackManifest,
    TrustedResolverPackKey,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(crate) struct PublishPlan {
    pub(crate) manifest_key: String,
    pub(crate) resolver_config_key: String,
    pub(crate) schema_snapshot_key: String,
    pub(crate) embedding_artifact_key: String,
    pub(crate) latest_manifest_pointer_key: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct PublishReceipt {
    pub(crate) action: &'static str,
    pub(crate) env: Env,
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) key_id: String,
    pub(crate) manifest_hash: String,
    pub(crate) artifact_hashes: BTreeMap<&'static str, String>,
    pub(crate) validation: &'static str,
    pub(crate) upload: &'static str,
    pub(crate) dry_run: bool,
    pub(crate) plan: PublishPlan,
}

pub(crate) fn publish_plan(
    env: Env,
    manifest: &ResolverPackManifest,
    resolver_config: &[u8],
    schema_snapshot: &[u8],
    embedding_artifact: &[u8],
) -> PublishPlan {
    let manifest_bytes = serde_json::to_vec_pretty(manifest).unwrap_or_default();
    PublishPlan {
        manifest_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::Manifest,
            &artifact_sha256_hex(&manifest_bytes),
        ),
        resolver_config_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::ResolverConfig,
            &artifact_sha256_hex(resolver_config),
        ),
        schema_snapshot_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::SchemaSnapshot,
            &artifact_sha256_hex(schema_snapshot),
        ),
        embedding_artifact_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::EmbeddingArtifact,
            &artifact_sha256_hex(embedding_artifact),
        ),
        latest_manifest_pointer_key: latest_compatible_manifest_pointer_key(
            env,
            manifest.resolver_contract_version,
            &manifest.algorithm.id,
            manifest.algorithm.version,
            &manifest.embedder_id,
        ),
    }
}

pub(crate) fn artifact_hashes(manifest: &ResolverPackManifest) -> BTreeMap<&'static str, String> {
    BTreeMap::from([
        ("resolver_config", manifest.resolver_config_hash.clone()),
        ("schema_snapshot", manifest.schema_snapshot_hash.clone()),
        (
            "embedding_artifact",
            manifest.embedding_artifact_hash.clone(),
        ),
    ])
}

pub(crate) async fn upload_pack(
    uploader: &S3Uploader,
    bucket: &str,
    plan: &PublishPlan,
    manifest_bytes: &[u8],
    inputs: PackInputs<'_>,
) -> Result<(), String> {
    uploader
        .put(
            bucket,
            &plan.resolver_config_key,
            inputs.resolver_config.to_vec(),
            "application/json",
        )
        .await?;
    uploader
        .put(
            bucket,
            &plan.schema_snapshot_key,
            inputs.schema_snapshot.to_vec(),
            "application/json",
        )
        .await?;
    uploader
        .put(
            bucket,
            &plan.embedding_artifact_key,
            inputs.embedding_artifact.to_vec(),
            "application/json",
        )
        .await?;
    uploader
        .put(
            bucket,
            &plan.manifest_key,
            manifest_bytes.to_vec(),
            "application/json",
        )
        .await?;
    uploader
        .put(
            bucket,
            &plan.latest_manifest_pointer_key,
            manifest_bytes.to_vec(),
            "application/json",
        )
        .await?;
    Ok(())
}

pub(crate) struct S3Uploader {
    pub(crate) client: aws_sdk_s3::Client,
}

impl S3Uploader {
    pub(crate) async fn new(region: Option<&str>, endpoint_url: Option<&str>) -> Self {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(aws_sdk_s3::config::Region::new(region.to_string()));
        }
        let sdk_config = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(endpoint_url) = endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }
        Self {
            client: aws_sdk_s3::Client::from_conf(builder.build()),
        }
    }

    pub(crate) async fn put(
        &self,
        bucket: &str,
        key: &str,
        bytes: Vec<u8>,
        content_type: &str,
    ) -> Result<(), String> {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| format!("R2 put failed for key {key}: {e}"))?;
        Ok(())
    }
}

pub(crate) fn validate_manifest_for_pointer_only(
    manifest: &ResolverPackManifest,
    trusted_keys: &[TrustedResolverPackKey],
    expected_env: Env,
) -> Result<(), String> {
    if manifest.env != expected_env {
        return Err(format!(
            "manifest env {:?} does not match requested env {:?}",
            manifest.env, expected_env
        ));
    }
    if manifest.signature.purpose != Purpose::SchemaResolverPack {
        return Err("manifest signature purpose is not schema_resolver_pack".to_string());
    }
    if manifest.signature.env != manifest.env {
        return Err("manifest signature env does not match manifest env".to_string());
    }
    if manifest.signing_key_id != manifest.signature.key_id {
        return Err("manifest signing_key_id does not match envelope key_id".to_string());
    }
    let trusted = trusted_keys
        .iter()
        .find(|key| key.key_id == manifest.signing_key_id)
        .ok_or_else(|| {
            format!(
                "untrusted resolver-pack signing key id: {}",
                manifest.signing_key_id
            )
        })?;
    let verifying_key = verifying_key_from_base64(&trusted.public_key_b64)
        .map_err(|e| format!("trusted resolver-pack public key could not be parsed: {e}"))?;
    let payload = manifest_payload_value(manifest).map_err(|e| e.to_string())?;
    let expected_payload_hash = compute_payload_hash(&payload).map_err(|e| e.to_string())?;
    if manifest.signature.payload_hash != expected_payload_hash {
        return Err("manifest signature payload_hash does not match manifest payload".to_string());
    }
    verify_envelope(&verifying_key, &manifest.signature)
        .map_err(|e| format!("manifest signature verification failed: {e}"))?;
    Ok(())
}

pub(crate) fn read_file(label: &str, path: &Path) -> Result<Vec<u8>, String> {
    fs::read(path).map_err(|e| format!("read {label} at {}: {e}", path.display()))
}

pub(crate) fn print_dry_run_manifest(
    manifest: &ResolverPackManifest,
    receipt: &PublishReceipt,
) -> Result<(), String> {
    #[derive(Serialize)]
    struct DryRun<'a> {
        receipt: &'a PublishReceipt,
        manifest: &'a ResolverPackManifest,
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&DryRun { receipt, manifest }).map_err(|e| e.to_string())?
    );
    Ok(())
}

pub(crate) fn write_receipt(path: Option<&Path>, receipt: &PublishReceipt) -> Result<(), String> {
    if let Some(path) = path {
        let bytes = serde_json::to_vec_pretty(receipt).map_err(|e| e.to_string())?;
        fs::write(path, bytes).map_err(|e| format!("write audit log {}: {e}", path.display()))?;
    }
    Ok(())
}

pub(crate) fn write_artifact(path: &Path, bytes: &[u8]) -> Result<(), String> {
    fs::write(path, bytes).map_err(|e| format!("write artifact {}: {e}", path.display()))
}
