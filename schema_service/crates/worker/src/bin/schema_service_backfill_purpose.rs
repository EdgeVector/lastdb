//! Phase D operator CLI: backfill `purpose_statement` on every
//! canonical schema persisted before Phase A landed.
//!
//! Phase A defaults `purpose_statement` to `descriptive_name` at
//! registration time for every *new* schema (see
//! `schema_service_core::SchemaServiceState::add_schema`), but schemas
//! already living in the dev Sled registry / prod S3 `schemas.json`
//! blob still carry `purpose_statement = None`. Phase B's dual-signal
//! canonicalization (fbrain `dual-signal-schema-canonicalization`)
//! needs every record to carry a non-empty purpose statement so the
//! similarity matcher has something to compare against. This binary
//! is the one-shot backfill the operator runs once per environment.
//!
//! ## Invocation
//!
//! ```bash
//! # Dev (Sled) — always dry-run first.
//! cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
//!     --backend local --db-path ~/.folddb/schema_registry --dry-run
//! cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
//!     --backend local --db-path ~/.folddb/schema_registry
//!
//! # Prod/R2 (S3-compatible) — requires SCHEMA_STORE_BUCKET,
//! # SCHEMA_EMBEDDINGS_TABLE, SCHEMA_STORE_ENDPOINT_URL, and
//! # SCHEMA_STORE_ACCESS_KEY_ID / SCHEMA_STORE_SECRET_ACCESS_KEY for R2.
//! cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
//!     --backend s3 --dry-run
//! cargo run -p schema_service_worker --bin schema_service_backfill_purpose -- \
//!     --backend s3
//! ```
//!
//! Idempotent — a second run is a no-op. See `schema_service/README.md`
//! "Operations — backfill purpose_statement" for the full runbook.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, ValueEnum};
use schema_service_core::{Embedder, SchemaServiceState};

const SCHEMA_STORE_R2_ENDPOINT_ENV: &str = "SCHEMA_STORE_R2_ENDPOINT";
const SCHEMA_STORE_ACCESS_KEY_ID_ENV: &str = "SCHEMA_STORE_ACCESS_KEY_ID";
const SCHEMA_STORE_SECRET_ACCESS_KEY_ENV: &str = "SCHEMA_STORE_SECRET_ACCESS_KEY";
const SCHEMA_STORE_REGION_ENV: &str = "SCHEMA_STORE_REGION";
const SCHEMA_STORE_DEFAULT_REGION: &str = "auto";

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Backend {
    /// Local Last Store — used by the dev binary and `./run.sh` workflows.
    #[value(alias = "sled")]
    Local,
    /// S3-backed external persistence — what the schema-infra Lambda
    /// runs against in production.
    S3,
}

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Backfill purpose_statement on existing canonical schemas (Phase D)."
)]
struct Cli {
    /// Which schema-service backend to target.
    #[arg(long, value_enum)]
    backend: Backend,

    /// Path to the Last Store home. Required for `--backend local` (alias: `sled`).
    #[arg(long, default_value = "schema_registry")]
    db_path: String,

    /// S3 bucket holding `schemas.json`. Required for `--backend s3`.
    /// Falls back to the `SCHEMA_STORE_BUCKET` env var if unset.
    #[arg(long, env = "SCHEMA_STORE_BUCKET")]
    s3_bucket: Option<String>,

    /// DynamoDB table holding persisted fastembed vectors. Required
    /// for `--backend s3`. Falls back to the `SCHEMA_EMBEDDINGS_TABLE`
    /// env var if unset.
    #[arg(long, env = "SCHEMA_EMBEDDINGS_TABLE")]
    embeddings_table: Option<String>,

    /// Optional S3-compatible endpoint URL for schema blobs. Set this
    /// to the Cloudflare R2 account endpoint when migrating registry
    /// state to R2. Falls back to `SCHEMA_STORE_ENDPOINT_URL`; if that
    /// is absent, the legacy alias `SCHEMA_STORE_R2_ENDPOINT` is read.
    #[arg(long, env = "SCHEMA_STORE_ENDPOINT_URL")]
    s3_endpoint_url: Option<String>,

    /// Optional JSON file:
    /// `{ "<descriptive_name>": "<purpose_statement>", ... }`.
    /// Records whose `descriptive_name` matches a key get the mapped
    /// value; unmatched records fall back to `descriptive_name`. If
    /// omitted, uses the built-in fbrain six-kinds placeholder
    /// mapping (Concept / Preference / Reference / Agent / Project /
    /// Spike).
    #[arg(long)]
    mapping_file: Option<PathBuf>,

    /// Print every decision without writing.
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    // Install the standard observability stack so every backfill
    // decision (per-schema info logs + the final summary) flows
    // through the same JSON sink the dev binary and Lambda use.
    let _obs_guard = match observability::init_node(
        "schema_service_backfill_purpose",
        env!("CARGO_PKG_VERSION"),
    ) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("failed to install observability: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mapping = match load_mapping(cli.mapping_file.as_deref()) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to load mapping file");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(
        mapping_entries = mapping.len(),
        dry_run = cli.dry_run,
        backend = ?cli.backend,
        "backfill: starting",
    );

    let state = match build_state(&cli).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to build SchemaServiceState");
            return ExitCode::FAILURE;
        }
    };

    let report = match state
        .backfill_purpose_statements(&mapping, cli.dry_run)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "backfill failed");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        considered = report.considered,
        updated = report.updated,
        would_update = report.would_update,
        skipped_already_set = report.skipped_already_set,
        skipped_no_descriptive_name = report.skipped_no_descriptive_name,
        dry_run = cli.dry_run,
        "backfill: done",
    );

    ExitCode::SUCCESS
}

async fn build_state(cli: &Cli) -> Result<SchemaServiceState, String> {
    let embedder: Arc<dyn Embedder> =
        Arc::new(schema_service_server_shared::FoldDbFastEmbedder::new());

    match cli.backend {
        Backend::Local => SchemaServiceState::new(&cli.db_path, embedder).map_err(|e| {
            format!(
                "Last Store state init failed (db_path={}): {e}",
                cli.db_path
            )
        }),
        Backend::S3 => {
            let bucket = cli
                .s3_bucket
                .clone()
                .ok_or("--backend s3 requires --s3-bucket or SCHEMA_STORE_BUCKET")?;
            let embeddings_table = cli
                .embeddings_table
                .clone()
                .ok_or("--backend s3 requires --embeddings-table or SCHEMA_EMBEDDINGS_TABLE")?;

            let endpoint_url = schema_store_endpoint_url(cli.s3_endpoint_url.as_deref());
            let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let schema_store_credentials = if endpoint_url.is_some() {
                schema_store_s3_credentials_from_env()?
            } else {
                None
            };
            let s3_config = schema_store_s3_config_builder(
                aws_sdk_s3::config::Builder::from(&aws_config),
                endpoint_url.as_deref(),
                schema_store_credentials.as_ref(),
            );
            let s3_client = aws_sdk_s3::Client::from_conf(s3_config.build());
            let ddb_client = aws_sdk_dynamodb::Client::new(&aws_config);
            let backend: Arc<dyn schema_service_core::ExternalSchemaPersistence> =
                Arc::new(schema_service_s3::S3BlobPersistence::new(
                    s3_client,
                    bucket.clone(),
                    ddb_client,
                    embeddings_table,
                ));
            tracing::info!(
                bucket = %bucket,
                endpoint_url = endpoint_url.as_deref().unwrap_or("aws-default"),
                "backfill: initializing schema service against S3-compatible storage",
            );
            SchemaServiceState::new_with_external(backend, embedder)
                .await
                .map_err(|e| format!("S3-compatible state init failed (bucket={bucket}): {e}"))
        }
    }
}

fn schema_store_endpoint_url(cli_value: Option<&str>) -> Option<String> {
    cli_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| env_var_trimmed(SCHEMA_STORE_R2_ENDPOINT_ENV))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaStoreS3Credentials {
    access_key_id: String,
    secret_access_key: String,
    region: String,
}

fn schema_store_s3_credentials_from_env() -> Result<Option<SchemaStoreS3Credentials>, String> {
    let access_key_id = env_var_trimmed(SCHEMA_STORE_ACCESS_KEY_ID_ENV);
    let secret_access_key = env_var_trimmed(SCHEMA_STORE_SECRET_ACCESS_KEY_ENV);
    match (access_key_id, secret_access_key) {
        (None, None) => Ok(None),
        (Some(access_key_id), Some(secret_access_key)) => Ok(Some(SchemaStoreS3Credentials {
            access_key_id,
            secret_access_key,
            region: env_var_trimmed(SCHEMA_STORE_REGION_ENV)
                .unwrap_or_else(|| SCHEMA_STORE_DEFAULT_REGION.to_string()),
        })),
        _ => Err(format!(
            "{SCHEMA_STORE_ACCESS_KEY_ID_ENV} and {SCHEMA_STORE_SECRET_ACCESS_KEY_ENV} must both be set for S3-compatible schema-store credentials"
        )),
    }
}

fn schema_store_s3_config_builder(
    mut builder: aws_sdk_s3::config::Builder,
    endpoint_url: Option<&str>,
    credentials: Option<&SchemaStoreS3Credentials>,
) -> aws_sdk_s3::config::Builder {
    if let Some(endpoint_url) = endpoint_url {
        builder = builder.endpoint_url(endpoint_url).force_path_style(true);
        if let Some(credentials) = credentials {
            let s3_credentials = aws_sdk_s3::config::Credentials::new(
                &credentials.access_key_id,
                &credentials.secret_access_key,
                None,
                None,
                "schema-store-s3-compatible",
            );
            builder = builder
                .region(aws_sdk_s3::config::Region::new(credentials.region.clone()))
                .credentials_provider(s3_credentials);
        }
    }
    builder
}

fn env_var_trimmed(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn load_mapping(path: Option<&std::path::Path>) -> Result<HashMap<String, String>, String> {
    match path {
        None => Ok(built_in_fbrain_six_kinds_mapping()),
        Some(p) => {
            let raw = fs::read_to_string(p).map_err(|e| format!("read {}: {e}", p.display()))?;
            serde_json::from_str(&raw).map_err(|e| format!("parse {}: {e}", p.display()))
        }
    }
}

/// Placeholder purpose statements for fbrain's six kinds, drafted to
/// match the `dual-signal-schema-canonicalization` design doc's
/// terminology. Operators with environment-specific copy can override
/// via `--mapping-file`.
fn built_in_fbrain_six_kinds_mapping() -> HashMap<String, String> {
    [
        (
            "Concept",
            "A reusable knowledge atom — a definition, idea, or fact \
             that other artifacts can reference.",
        ),
        (
            "Preference",
            "A user-stated choice or constraint — a default or decision \
             the user wants honored across work.",
        ),
        (
            "Reference",
            "An external pointer — a link, citation, or source the user \
             wants to remember and return to.",
        ),
        (
            "Agent",
            "An autonomous task runner — a named role or process that \
             performs work without continuous user input.",
        ),
        (
            "Project",
            "A multi-step goal — an initiative with scope, status, and \
             stakeholders the user is actively driving.",
        ),
        (
            "Spike",
            "An exploratory investigation — a short, time-boxed probe \
             to answer a specific open question.",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}
