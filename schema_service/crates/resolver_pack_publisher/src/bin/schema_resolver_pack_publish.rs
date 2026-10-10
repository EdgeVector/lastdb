//! Local operator publisher for signed Schema Resolver Packs.
//!
//! This binary intentionally runs on a provisioned operator machine, not in CI:
//! it resolves local secret locators, signs the pack manifest with the
//! resolver-pack Ed25519 key, uploads immutable content-addressed artifacts,
//! and promotes the latest pointer only after validation succeeds.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use app_identity_crypto::{key_id, Env};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use schema_service_core::resolver_config::ResolverConfig;
use schema_service_core::resolver_pack::{
    artifact_sha256_hex, latest_compatible_manifest_pointer_key, parse_embedding_artifact,
    parse_resolver_config, resolver_pack_artifact_key, verify_resolver_pack_manifest,
    EmbeddingArtifact, ResolverPackArtifactKind, ResolverPackManifest,
    RESOLVER_PACK_FORMAT_VERSION,
};
use schema_service_core::snapshot::{
    offline_is_system_schema, project_snapshot_shared_only, SnapshotEnvelope,
};
use serde_json::json;

use artifacts::*;
use secrets::{load_signing_key, parse_trusted_keys};
use upload::*;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Build, sign, upload, promote, and rollback schema resolver packs."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Build resolver-pack artifact files from an exported schema-service snapshot.
    Build(BuildArgs),
    /// Print the public trusted-key tuple for a signing-key locator.
    TrustedKey(TrustedKeyArgs),
    /// Assemble, sign, validate, upload, and promote a resolver pack.
    Publish(PublishArgs),
    /// Promote a previous signed manifest as the latest pointer.
    Rollback(RollbackArgs),
}

#[derive(Debug, Clone, Parser)]
struct BuildArgs {
    /// SnapshotEnvelope JSON from GET /v1/snapshot.
    #[arg(long)]
    snapshot: PathBuf,

    /// Directory to receive resolver_config.json, schema_snapshot.json,
    /// and embedding_artifact.json.
    #[arg(long)]
    out_dir: PathBuf,

    /// Optional resolver_config.json. If omitted, writes embedding-beam
    /// shadow defaults (conservative native defaults for fixtures/shadow).
    #[arg(long)]
    resolver_config: Option<PathBuf>,

    #[arg(long, default_value = "embedding-beam-shadow-2026-07-06")]
    policy_version: String,

    /// When snapshot embeddings are empty (typical of wire export), recompute
    /// them with FastEmbed. Requires `--features recompute-embeddings`.
    #[arg(long, default_value_t = false)]
    recompute_embeddings: bool,

    /// Cap schemas after shared-only projection (dogfood / smoke packs).
    #[arg(long)]
    max_schemas: Option<usize>,
}

#[derive(Debug, Clone, Parser)]
struct TrustedKeyArgs {
    /// Secret locator for the Ed25519 signing seed.
    ///
    /// Prints only the public `key_id=base64_public_key` tuple accepted by
    /// `publish --trusted-key`.
    #[arg(long)]
    signing_key_locator: String,
}

#[derive(Debug, Clone, Parser)]
struct PublishArgs {
    #[arg(long, value_enum)]
    env: PublishEnv,

    #[arg(long)]
    resolver_config: PathBuf,

    #[arg(long)]
    schema_snapshot: PathBuf,

    #[arg(long)]
    embedding_artifact: PathBuf,

    /// Secret locator for the Ed25519 signing seed.
    ///
    /// Supported locators:
    /// - `lastsecrets://slug`
    /// - `keychain://service/account`
    /// - `envelope-file:/path/to/envelope.json`
    /// - `file:/path/to/seed.b64`
    /// - `env:VAR_NAME` (tests/dev only; avoid for routine releases)
    #[arg(long)]
    signing_key_locator: String,

    /// Trusted public key in `key_id=base64_public_key` form.
    ///
    /// Pass this more than once during key rotation. The signing key must be
    /// present in this set or publishing fails closed before upload.
    #[arg(long = "trusted-key")]
    trusted_keys: Vec<String>,

    #[arg(long, env = "SCHEMA_RESOLVER_PACK_BUCKET")]
    r2_bucket: Option<String>,

    #[arg(long, env = "SCHEMA_RESOLVER_PACK_R2_ENDPOINT")]
    r2_endpoint_url: Option<String>,

    #[arg(long, env = "AWS_REGION")]
    aws_region: Option<String>,

    /// Do not upload; print the manifest, hashes, and target keys.
    #[arg(long)]
    dry_run: bool,

    /// Optional path for a JSON audit receipt. The receipt never includes
    /// secret material.
    #[arg(long)]
    audit_log: Option<PathBuf>,

    /// Override the manifest/envelope expiry. Defaults to 90 days.
    #[arg(long)]
    expires_at: Option<DateTime<Utc>>,

    /// Write the content-addressed pack key layout under this directory
    /// (for localhost dogfood static serving). Can be used with or without R2.
    #[arg(long)]
    local_out_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Parser)]
struct RollbackArgs {
    #[arg(long, value_enum)]
    env: PublishEnv,

    /// Previously validated signed manifest to promote.
    #[arg(long)]
    manifest: PathBuf,

    /// Trusted public key in `key_id=base64_public_key` form.
    #[arg(long = "trusted-key")]
    trusted_keys: Vec<String>,

    /// Optional artifact files for full manifest verification before promote.
    #[arg(long)]
    resolver_config: Option<PathBuf>,
    #[arg(long)]
    schema_snapshot: Option<PathBuf>,
    #[arg(long)]
    embedding_artifact: Option<PathBuf>,

    #[arg(long, env = "SCHEMA_RESOLVER_PACK_BUCKET")]
    r2_bucket: Option<String>,

    #[arg(long, env = "SCHEMA_RESOLVER_PACK_R2_ENDPOINT")]
    r2_endpoint_url: Option<String>,

    #[arg(long, env = "AWS_REGION")]
    aws_region: Option<String>,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    audit_log: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum PublishEnv {
    Dev,
    Prod,
}

impl From<PublishEnv> for Env {
    fn from(value: PublishEnv) -> Self {
        match value {
            PublishEnv::Dev => Self::Dev,
            PublishEnv::Prod => Self::Prod,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("schema_resolver_pack_publish: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Commands::Build(args) => build(&args),
        Commands::TrustedKey(args) => trusted_key(&args),
        Commands::Publish(args) => publish(args).await,
        Commands::Rollback(args) => rollback(args).await,
    }
}

fn trusted_key(args: &TrustedKeyArgs) -> Result<(), String> {
    let signing_key = load_signing_key(&args.signing_key_locator)?;
    let verifying_key = signing_key.verifying_key();
    println!(
        "{}={}",
        key_id(&verifying_key),
        BASE64.encode(verifying_key.to_bytes())
    );
    Ok(())
}

fn build(args: &BuildArgs) -> Result<(), String> {
    let snapshot_bytes = read_file("snapshot", &args.snapshot)?;
    let mut snapshot: SnapshotEnvelope = serde_json::from_slice(&snapshot_bytes)
        .map_err(|e| format!("snapshot parse failed: {e}"))?;
    // `build_artifacts_from_snapshot` applies the shared-only projection so
    // private legacy bootstrap / unknown rows never enter pack artifacts.
    if args.recompute_embeddings {
        snapshot = recompute_snapshot_embeddings(snapshot, args.max_schemas)?;
    } else if let Some(max) = args.max_schemas {
        snapshot = project_snapshot_shared_only(snapshot, offline_is_system_schema);
        snapshot.schemas.truncate(max);
    }
    let artifacts = build_artifacts_from_snapshot(&snapshot, &args.policy_version)?;

    fs::create_dir_all(&args.out_dir)
        .map_err(|e| format!("create out-dir {}: {e}", args.out_dir.display()))?;

    let resolver_config = match args.resolver_config.as_deref() {
        Some(path) => {
            let bytes = read_file("resolver_config", path)?;
            let config: ResolverConfig = parse_resolver_config(&bytes)
                .map_err(|e| format!("resolver_config parse failed: {e}"))?;
            config
                .validate()
                .map_err(|e| format!("resolver_config invalid: {e}"))?;
            serde_json::to_vec_pretty(&config)
                .map_err(|e| format!("resolver_config serialization failed: {e}"))?
        }
        None => serde_json::to_vec_pretty(&artifacts.resolver_config)
            .map_err(|e| format!("resolver_config serialization failed: {e}"))?,
    };

    write_artifact(&args.out_dir.join("resolver_config.json"), &resolver_config)?;
    write_artifact(
        &args.out_dir.join("schema_snapshot.json"),
        &artifacts.schema_snapshot,
    )?;
    write_artifact(
        &args.out_dir.join("embedding_artifact.json"),
        &artifacts.embedding_artifact,
    )?;

    let receipt = json!({
        "action": "build",
        "snapshot_format_version": snapshot.format_version,
        "source_snapshot_hash": artifact_sha256_hex(&snapshot_bytes),
        "resolver_config_hash": artifact_sha256_hex(&resolver_config),
        "schema_snapshot_hash": artifact_sha256_hex(&artifacts.schema_snapshot),
        "embedding_artifact_hash": artifact_sha256_hex(&artifacts.embedding_artifact),
        "algorithm_id": artifacts.resolver_config.algorithm.id,
        "algorithm_version": artifacts.resolver_config.algorithm.version,
        "resolver_contract_version": artifacts.resolver_config.resolver_contract_version,
        "policy_version": artifacts.resolver_config.policy_version,
        "out_dir": args.out_dir.display().to_string(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&receipt).map_err(|e| e.to_string())?
    );
    Ok(())
}

async fn publish(args: PublishArgs) -> Result<(), String> {
    let resolver_config = read_file("resolver_config", &args.resolver_config)?;
    let schema_snapshot = read_file("schema_snapshot", &args.schema_snapshot)?;
    let embedding_artifact = read_file("embedding_artifact", &args.embedding_artifact)?;

    let signing_key = load_signing_key(&args.signing_key_locator)?;
    let trusted_keys = parse_trusted_keys(&args.trusted_keys)?;
    let env = Env::from(args.env);
    let issued_at = Utc::now();
    let expires_at = args.expires_at.unwrap_or(issued_at + Duration::days(90));
    let manifest = build_signed_manifest(
        PackInputs {
            resolver_config: &resolver_config,
            schema_snapshot: &schema_snapshot,
            embedding_artifact: &embedding_artifact,
        },
        env,
        &signing_key,
        issued_at,
        Some(expires_at),
    )?;

    let embedding: EmbeddingArtifact = parse_embedding_artifact(&embedding_artifact)
        .map_err(|e| format!("embedding artifact parse failed: {e}"))?;
    verify_resolver_pack_manifest(
        &manifest,
        &resolver_config,
        &schema_snapshot,
        &embedding_artifact,
        &trusted_keys,
        env,
        &embedding.embedder_id,
    )
    .map_err(|e| format!("signed manifest validation failed: {e}"))?;

    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| format!("manifest serialization failed: {e}"))?;
    let plan = publish_plan(
        env,
        &manifest,
        &resolver_config,
        &schema_snapshot,
        &embedding_artifact,
    );
    let receipt = PublishReceipt {
        action: "publish",
        env,
        timestamp: Utc::now(),
        key_id: manifest.signing_key_id.clone(),
        manifest_hash: artifact_sha256_hex(&manifest_bytes),
        artifact_hashes: artifact_hashes(&manifest),
        validation: "passed",
        upload: if args.dry_run { "dry_run" } else { "uploaded" },
        dry_run: args.dry_run,
        plan,
    };

    if let Some(local_dir) = args.local_out_dir.as_ref() {
        write_pack_local_layout(
            local_dir,
            &receipt.plan,
            &manifest_bytes,
            PackInputs {
                resolver_config: &resolver_config,
                schema_snapshot: &schema_snapshot,
                embedding_artifact: &embedding_artifact,
            },
        )?;
        eprintln!(
            "wrote local pack layout under {} (serve this directory as base_url)",
            local_dir.display()
        );
    }

    if args.dry_run {
        print_dry_run_manifest(&manifest, &receipt)?;
    } else if args.r2_bucket.is_some() || args.local_out_dir.is_none() {
        let bucket = args
            .r2_bucket
            .clone()
            .ok_or("--r2-bucket or SCHEMA_RESOLVER_PACK_BUCKET is required without --dry-run (or use --local-out-dir only)")?;
        let uploader =
            S3Uploader::new(args.aws_region.as_deref(), args.r2_endpoint_url.as_deref()).await;
        upload_pack(
            &uploader,
            &bucket,
            &receipt.plan,
            &manifest_bytes,
            PackInputs {
                resolver_config: &resolver_config,
                schema_snapshot: &schema_snapshot,
                embedding_artifact: &embedding_artifact,
            },
        )
        .await?;
    }
    write_receipt(args.audit_log.as_deref(), &receipt)?;
    Ok(())
}

async fn rollback(args: RollbackArgs) -> Result<(), String> {
    let manifest_bytes = read_file("manifest", &args.manifest)?;
    let manifest: ResolverPackManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| format!("manifest parse failed: {e}"))?;
    let trusted_keys = parse_trusted_keys(&args.trusted_keys)?;
    let env = Env::from(args.env);

    if manifest.format_version != RESOLVER_PACK_FORMAT_VERSION {
        return Err(format!(
            "rollback requires format_version {}, got {}",
            RESOLVER_PACK_FORMAT_VERSION, manifest.format_version
        ));
    }

    let embedding_id =
        if let (Some(resolver_config), Some(schema_snapshot), Some(embedding_artifact)) = (
            args.resolver_config.as_ref(),
            args.schema_snapshot.as_ref(),
            args.embedding_artifact.as_ref(),
        ) {
            let resolver_config = read_file("resolver_config", resolver_config)?;
            let schema_snapshot = read_file("schema_snapshot", schema_snapshot)?;
            let embedding_artifact = read_file("embedding_artifact", embedding_artifact)?;
            let embedding: EmbeddingArtifact = parse_embedding_artifact(&embedding_artifact)
                .map_err(|e| format!("embedding artifact parse failed: {e}"))?;
            verify_resolver_pack_manifest(
                &manifest,
                &resolver_config,
                &schema_snapshot,
                &embedding_artifact,
                &trusted_keys,
                env,
                &embedding.embedder_id,
            )
            .map_err(|e| format!("rollback manifest validation failed: {e}"))?;
            embedding.embedder_id
        } else {
            validate_manifest_for_pointer_only(&manifest, &trusted_keys, env)?;
            manifest.embedder_id.clone()
        };

    let pointer_key = latest_compatible_manifest_pointer_key(
        env,
        manifest.resolver_contract_version,
        &manifest.algorithm.id,
        manifest.algorithm.version,
        &embedding_id,
    );
    let plan = PublishPlan {
        manifest_key: resolver_pack_artifact_key(
            env,
            ResolverPackArtifactKind::Manifest,
            &artifact_sha256_hex(&manifest_bytes),
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
    };
    let receipt = PublishReceipt {
        action: "rollback",
        env,
        timestamp: Utc::now(),
        key_id: manifest.signing_key_id.clone(),
        manifest_hash: artifact_sha256_hex(&manifest_bytes),
        artifact_hashes: BTreeMap::from([
            ("resolver_config", manifest.resolver_config_hash.clone()),
            ("schema_snapshot", manifest.schema_snapshot_hash.clone()),
            (
                "embedding_artifact",
                manifest.embedding_artifact_hash.clone(),
            ),
        ]),
        validation: "passed",
        upload: if args.dry_run {
            "dry_run"
        } else {
            "promoted_latest_pointer"
        },
        dry_run: args.dry_run,
        plan,
    };
    if args.dry_run {
        println!(
            "{}",
            serde_json::to_string_pretty(&receipt).map_err(|e| e.to_string())?
        );
    } else {
        let bucket = args
            .r2_bucket
            .clone()
            .ok_or("--r2-bucket or SCHEMA_RESOLVER_PACK_BUCKET is required without --dry-run")?;
        let uploader =
            S3Uploader::new(args.aws_region.as_deref(), args.r2_endpoint_url.as_deref()).await;
        uploader
            .put(
                &bucket,
                &receipt.plan.latest_manifest_pointer_key,
                manifest_bytes,
                "application/json",
            )
            .await?;
    }
    write_receipt(args.audit_log.as_deref(), &receipt)?;
    Ok(())
}

#[path = "schema_resolver_pack_publish/artifacts.rs"]
mod artifacts;
#[path = "schema_resolver_pack_publish/secrets.rs"]
mod secrets;
#[path = "schema_resolver_pack_publish/upload.rs"]
mod upload;
