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
    artifact_sha256_hex, parse_embedding_artifact, parse_resolver_config,
    verify_resolver_pack_manifest, EmbeddingArtifact, ResolverPackManifest,
    RESOLVER_PACK_FORMAT_VERSION,
};
use schema_service_core::snapshot::{
    offline_is_system_schema, project_snapshot_shared_only, SnapshotEnvelope,
};
use serde_json::json;

use artifacts::*;
use embeddings::*;
use manifest::*;
use rollback_plan::{rollback_embedding_id, rollback_plan};
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
    /// - `env:VAR_NAME` (dev only; avoid for routine releases)
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

#[path = "schema_resolver_pack_publish/commands.rs"]
mod commands;
use commands::*;

#[path = "schema_resolver_pack_publish/artifacts.rs"]
mod artifacts;
#[path = "schema_resolver_pack_publish/embeddings.rs"]
mod embeddings;
#[path = "schema_resolver_pack_publish/manifest.rs"]
mod manifest;
#[path = "schema_resolver_pack_publish/rollback_plan.rs"]
mod rollback_plan;
#[path = "schema_resolver_pack_publish/secrets.rs"]
mod secrets;
#[path = "schema_resolver_pack_publish/upload.rs"]
mod upload;
