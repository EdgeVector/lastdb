//! Local operator publisher for signed Schema Resolver Packs.
//!
//! This binary intentionally runs on a provisioned operator machine, not in CI:
//! it resolves local secret locators, signs the pack manifest with the
//! resolver-pack Ed25519 key, uploads immutable content-addressed artifacts,
//! and promotes the latest pointer only after validation succeeds.

use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, verify_envelope, verifying_key_from_base64, Env,
    Purpose, SignatureEnvelope, SigningKey, ALG_ED25519, ENVELOPE_VERSION,
};
use aws_sdk_s3::primitives::ByteStream;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use chrono::{DateTime, Duration, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use schema_service_core::resolver_config::{AlgorithmSelector, ResolverConfig};
use schema_service_core::resolver_pack::{
    artifact_sha256_hex, latest_compatible_manifest_pointer_key, manifest_payload_value,
    parse_embedding_artifact, parse_resolver_config, parse_schema_snapshot_artifact,
    resolver_pack_artifact_key, verify_resolver_pack_manifest, EmbeddingArtifact,
    EmbeddingVectorRecord, ResolverCanonicalFieldRecord, ResolverFieldRecord,
    ResolverPackArtifactKind, ResolverPackArtifactSizes, ResolverPackCounts, ResolverPackManifest,
    ResolverSchemaRecord, SchemaLifecycle, SchemaSnapshotArtifact, TrustedResolverPackKey,
    RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION, RESOLVER_PACK_FORMAT_VERSION,
    RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION,
};
use schema_service_core::snapshot::{
    offline_is_system_schema, project_snapshot_shared_only, SnapshotEnvelope,
    SNAPSHOT_FORMAT_VERSION,
};
use schema_service_core::types::CanonicalField;
use serde::Serialize;
use serde_json::json;

use secrets::{load_signing_key, parse_trusted_keys};

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

#[derive(Debug, Serialize)]
struct PublishPlan {
    manifest_key: String,
    resolver_config_key: String,
    schema_snapshot_key: String,
    embedding_artifact_key: String,
    latest_manifest_pointer_key: String,
}

#[derive(Debug, Serialize)]
struct PublishReceipt {
    action: &'static str,
    env: Env,
    timestamp: DateTime<Utc>,
    key_id: String,
    manifest_hash: String,
    artifact_hashes: BTreeMap<&'static str, String>,
    validation: &'static str,
    upload: &'static str,
    dry_run: bool,
    plan: PublishPlan,
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

#[derive(Clone, Copy)]
struct PackInputs<'a> {
    resolver_config: &'a [u8],
    schema_snapshot: &'a [u8],
    embedding_artifact: &'a [u8],
}

#[derive(Debug)]
struct BuiltArtifacts {
    schema_snapshot: Vec<u8>,
    embedding_artifact: Vec<u8>,
    resolver_config: ResolverConfig,
}

fn build_artifacts_from_snapshot(
    snapshot: &SnapshotEnvelope,
    policy_version: &str,
) -> Result<BuiltArtifacts, String> {
    if snapshot.format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(format!(
            "snapshot format_version {} not supported (this binary supports {})",
            snapshot.format_version, SNAPSHOT_FORMAT_VERSION
        ));
    }

    // Callers that skip `build()` (unit tests) still project so private
    // schemas never reach pack artifacts.
    let snapshot = project_snapshot_shared_only(snapshot.clone(), offline_is_system_schema);
    let snapshot = &snapshot;

    let schema_snapshot = build_schema_snapshot_artifact(snapshot)?;
    let schema_snapshot_bytes = serde_json::to_vec_pretty(&schema_snapshot)
        .map_err(|e| format!("schema snapshot serialization failed: {e}"))?;
    let embedding_artifact = build_embedding_artifact(snapshot)?;
    let embedding_artifact_bytes = serde_json::to_vec_pretty(&embedding_artifact)
        .map_err(|e| format!("embedding artifact serialization failed: {e}"))?;

    Ok(BuiltArtifacts {
        schema_snapshot: schema_snapshot_bytes,
        embedding_artifact: embedding_artifact_bytes,
        resolver_config: default_resolver_config(policy_version),
    })
}

fn build_schema_snapshot_artifact(
    snapshot: &SnapshotEnvelope,
) -> Result<SchemaSnapshotArtifact, String> {
    let mut schemas = Vec::new();
    for schema in snapshot
        .schemas
        .iter()
        .filter(|schema| schema.superseded_by.is_none())
    {
        let descriptive_name = schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone());
        let purpose_statement = schema
            .purpose_statement
            .clone()
            .unwrap_or_else(|| descriptive_name.clone());
        let schema_id = schema_id(schema);
        let mut field_names = schema.fields.clone().unwrap_or_default();
        field_names.extend(
            schema
                .transform_fields
                .iter()
                .flat_map(|m| m.keys().cloned()),
        );
        field_names.sort();
        field_names.dedup();

        let mut fields = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let field_id = format!("{schema_id}#{field_name}");
            let description = schema
                .field_descriptions
                .get(&field_name)
                .cloned()
                .unwrap_or_else(|| field_name.clone());
            let mut canonical_field_ids = Vec::new();
            if snapshot.canonical_fields.contains_key(&field_name) {
                canonical_field_ids.push(canonical_field_id(&field_name));
            }
            fields.push(ResolverFieldRecord {
                field_id,
                field_name: field_name.clone(),
                field_type: schema.get_field_type(&field_name).to_string(),
                description,
                canonical_field_ids,
            });
        }

        schemas.push(ResolverSchemaRecord {
            schema_id,
            descriptive_name,
            purpose_statement,
            lifecycle: SchemaLifecycle::Active,
            fields,
        });
    }

    let mut canonical_fields: Vec<_> = snapshot
        .canonical_fields
        .iter()
        .map(|(name, field)| resolver_canonical_field(name, field))
        .collect();
    canonical_fields.sort_by(|a, b| a.canonical_field_id.cmp(&b.canonical_field_id));

    Ok(SchemaSnapshotArtifact {
        format_version: RESOLVER_PACK_SCHEMA_SNAPSHOT_FORMAT_VERSION,
        captured_at: snapshot.captured_at.clone(),
        source_snapshot_format_version: snapshot.format_version,
        schemas,
        canonical_fields,
    })
}

fn build_embedding_artifact(snapshot: &SnapshotEnvelope) -> Result<EmbeddingArtifact, String> {
    let mut descriptive_names = Vec::new();
    let mut schema_field_contexts = Vec::new();
    let mut canonical_fields = Vec::new();

    for schema in snapshot
        .schemas
        .iter()
        .filter(|schema| schema.superseded_by.is_none())
    {
        let descriptive_name = schema
            .descriptive_name
            .clone()
            .unwrap_or_else(|| schema.name.clone());
        let schema_key =
            namespaced_descriptive_name_key(schema.owner_app_id.as_deref(), &descriptive_name);
        let schema_vector = snapshot
            .embeddings
            .descriptive_names
            .get(&schema_key)
            .or_else(|| snapshot.embeddings.descriptive_names.get(&schema.name))
            .or_else(|| snapshot.embeddings.descriptive_names.get(&descriptive_name))
            .ok_or_else(|| {
                format!("missing descriptive-name embedding for schema {schema_key:?}")
            })?;
        descriptive_names.push(EmbeddingVectorRecord {
            target_id: schema_id(schema),
            vector: schema_vector.clone(),
        });

        let mut field_names = schema.fields.clone().unwrap_or_default();
        field_names.extend(
            schema
                .transform_fields
                .iter()
                .flat_map(|m| m.keys().cloned()),
        );
        field_names.sort();
        field_names.dedup();
        for field_name in field_names {
            let description = schema
                .field_descriptions
                .get(&field_name)
                .map(String::as_str);
            let key = field_embedding_cache_key(&descriptive_name, &field_name, description);
            let vector = snapshot.embeddings.fields.get(&key).ok_or_else(|| {
                format!(
                    "missing field-context embedding for schema {schema_key:?} field {field_name:?}"
                )
            })?;
            schema_field_contexts.push(EmbeddingVectorRecord {
                target_id: format!("{}#{field_name}", schema_id(schema)),
                vector: vector.clone(),
            });
        }
    }

    for name in snapshot.canonical_fields.keys() {
        let vector = snapshot
            .embeddings
            .canonical_fields
            .get(name)
            .ok_or_else(|| format!("missing canonical-field embedding for {name:?}"))?;
        canonical_fields.push(EmbeddingVectorRecord {
            target_id: canonical_field_id(name),
            vector: vector.clone(),
        });
    }

    descriptive_names.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    schema_field_contexts.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    canonical_fields.sort_by(|a, b| a.target_id.cmp(&b.target_id));
    let dimensions = infer_embedding_dimensions(&[
        &descriptive_names,
        &schema_field_contexts,
        &canonical_fields,
    ])?;

    Ok(EmbeddingArtifact {
        format_version: RESOLVER_PACK_EMBEDDING_ARTIFACT_FORMAT_VERSION,
        embedder_id: snapshot.embedder_version.clone(),
        generated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        dimensions,
        descriptive_names,
        schema_field_contexts,
        canonical_fields,
    })
}

/// Production wire snapshots strip embeddings (Lambda size). Rebuild with
/// FastEmbed so pack artifacts match Mini's proposal embedder id.
fn recompute_snapshot_embeddings(
    mut snapshot: SnapshotEnvelope,
    max_schemas: Option<usize>,
) -> Result<SnapshotEnvelope, String> {
    snapshot = project_snapshot_shared_only(snapshot, offline_is_system_schema);
    if let Some(max) = max_schemas {
        snapshot.schemas.truncate(max);
    }
    #[cfg(feature = "recompute-embeddings")]
    {
        use std::collections::HashMap;

        use schema_service_core::Embedder as _;
        use schema_service_server_shared::FoldDbFastEmbedder;

        const EMBEDDER_ID: &str = "fastembed/all-MiniLM-L6-v2";
        let model = FoldDbFastEmbedder::new();
        let mut descriptive_names = HashMap::new();
        let mut fields = HashMap::new();
        let mut canonical_fields = HashMap::new();

        for schema in snapshot
            .schemas
            .iter()
            .filter(|s| s.superseded_by.is_none())
        {
            let descriptive_name = schema
                .descriptive_name
                .clone()
                .unwrap_or_else(|| schema.name.clone());
            let schema_key =
                namespaced_descriptive_name_key(schema.owner_app_id.as_deref(), &descriptive_name);
            let name_vec = model
                .embed_text(&descriptive_name)
                .map_err(|e| format!("embed descriptive_name {descriptive_name:?}: {e}"))?;
            descriptive_names.insert(schema_key, name_vec.clone());
            descriptive_names.insert(descriptive_name.clone(), name_vec.clone());
            descriptive_names.insert(schema.name.clone(), name_vec);

            let mut field_names = schema.fields.clone().unwrap_or_default();
            field_names.extend(
                schema
                    .transform_fields
                    .iter()
                    .flat_map(|m| m.keys().cloned()),
            );
            field_names.sort();
            field_names.dedup();
            for field_name in field_names {
                let description = schema
                    .field_descriptions
                    .get(&field_name)
                    .map(String::as_str);
                let text = field_context_text(&descriptive_name, &field_name, description);
                let key = field_embedding_cache_key(&descriptive_name, &field_name, description);
                let vec = model
                    .embed_text(&text)
                    .map_err(|e| format!("embed field {field_name:?}: {e}"))?;
                fields.insert(key, vec);
            }
        }

        for (name, field) in &snapshot.canonical_fields {
            let text = if field.description.trim().is_empty() {
                name.clone()
            } else {
                format!("{name}: {}", field.description)
            };
            let vec = model
                .embed_text(&text)
                .map_err(|e| format!("embed canonical {name:?}: {e}"))?;
            canonical_fields.insert(name.clone(), vec);
        }

        snapshot.embedder_version = EMBEDDER_ID.to_string();
        snapshot.embeddings = schema_service_core::snapshot::SnapshotEmbeddings {
            descriptive_names,
            fields,
            canonical_fields,
        };
        Ok(snapshot)
    }
    #[cfg(not(feature = "recompute-embeddings"))]
    {
        let _ = max_schemas;
        Err(
            "--recompute-embeddings requires building with --features recompute-embeddings"
                .to_string(),
        )
    }
}

#[cfg(feature = "recompute-embeddings")]
fn field_context_text(
    descriptive_name: &str,
    field_name: &str,
    field_description: Option<&str>,
) -> String {
    match field_description.map(str::trim).filter(|s| !s.is_empty()) {
        Some(desc) => format!("the {field_name} of the {descriptive_name}: {desc}"),
        None => format!("the {field_name} of the {descriptive_name}"),
    }
}

fn write_pack_local_layout(
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

fn build_signed_manifest(
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

fn default_resolver_config(policy_version: &str) -> ResolverConfig {
    let mut config = ResolverConfig::embedding_beam_shadow_defaults();
    config.policy_version = policy_version.to_string();
    config
}

fn publish_plan(
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

fn artifact_hashes(manifest: &ResolverPackManifest) -> BTreeMap<&'static str, String> {
    BTreeMap::from([
        ("resolver_config", manifest.resolver_config_hash.clone()),
        ("schema_snapshot", manifest.schema_snapshot_hash.clone()),
        (
            "embedding_artifact",
            manifest.embedding_artifact_hash.clone(),
        ),
    ])
}

async fn upload_pack(
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

struct S3Uploader {
    client: aws_sdk_s3::Client,
}

impl S3Uploader {
    async fn new(region: Option<&str>, endpoint_url: Option<&str>) -> Self {
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

    async fn put(
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

fn validate_manifest_for_pointer_only(
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

fn read_file(label: &str, path: &Path) -> Result<Vec<u8>, String> {
    fs::read(path).map_err(|e| format!("read {label} at {}: {e}", path.display()))
}

fn print_dry_run_manifest(
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

fn write_receipt(path: Option<&Path>, receipt: &PublishReceipt) -> Result<(), String> {
    if let Some(path) = path {
        let bytes = serde_json::to_vec_pretty(receipt).map_err(|e| e.to_string())?;
        fs::write(path, bytes).map_err(|e| format!("write audit log {}: {e}", path.display()))?;
    }
    Ok(())
}

fn write_artifact(path: &Path, bytes: &[u8]) -> Result<(), String> {
    fs::write(path, bytes).map_err(|e| format!("write artifact {}: {e}", path.display()))
}

fn schema_id(schema: &schema_types::Schema) -> String {
    schema
        .identity_hash
        .clone()
        .unwrap_or_else(|| schema.name.clone())
}

fn canonical_field_id(name: &str) -> String {
    format!("canonical:{name}")
}

fn resolver_canonical_field(name: &str, field: &CanonicalField) -> ResolverCanonicalFieldRecord {
    ResolverCanonicalFieldRecord {
        canonical_field_id: canonical_field_id(name),
        description: field.description.clone(),
        field_type: field.field_type.to_string(),
    }
}

fn namespaced_descriptive_name_key(owner_app_id: Option<&str>, descriptive_name: &str) -> String {
    match owner_app_id.filter(|s| !s.is_empty()) {
        Some(app_id) => format!("{app_id}/{descriptive_name}"),
        None => descriptive_name.to_string(),
    }
}

fn field_embedding_cache_key(
    descriptive_name: &str,
    field_name: &str,
    field_description: Option<&str>,
) -> String {
    let desc_hash = match field_description {
        Some(desc) => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            desc.hash(&mut hasher);
            hasher.finish()
        }
        None => 0,
    };
    format!("{descriptive_name}:{field_name}:{desc_hash}")
}

fn infer_embedding_dimensions(classes: &[&[EmbeddingVectorRecord]]) -> Result<u32, String> {
    let mut dimensions = None;
    for record in classes.iter().flat_map(|records| records.iter()) {
        if record.vector.is_empty() {
            continue;
        }
        let len = record.vector.len() as u32;
        match dimensions {
            Some(existing) if existing != len => {
                return Err(format!(
                    "embedding dimension mismatch: saw {len}, expected {existing}"
                ));
            }
            Some(_) => {}
            None => dimensions = Some(len),
        }
    }
    Ok(dimensions.unwrap_or(0))
}

#[path = "schema_resolver_pack_publish/secrets.rs"]
mod secrets;
