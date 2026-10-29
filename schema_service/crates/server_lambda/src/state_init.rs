//! Cold-start initialization of the shared `SchemaServiceState`, plus the
//! environment-driven wiring for the S3-compatible blob store, the DynamoDB
//! mutation-gate quota table, and the embedder.

use lambda_http::Error;
use schema_service_core::{ExternalSchemaPersistence, SchemaConfig};
use schema_service_s3 as s3_persistence;
use schema_service_s3::store_config::{
    env_var_trimmed, schema_store_s3_config_builder, schema_store_s3_credentials_from_env,
    SCHEMA_STORE_R2_ENDPOINT_ENV,
};
use schema_service_server_shared::state::SchemaServiceState;
use schema_service_server_shared::SchemaConfigEnv;
use std::env;
use std::sync::Arc;
use tokio::sync::OnceCell;

const SCHEMA_MUTATION_GATE_QUOTA_TABLE_ENV: &str = "SCHEMA_MUTATION_GATE_QUOTA_TABLE";
const SCHEMA_STORE_ENDPOINT_URL_ENV: &str = "SCHEMA_STORE_ENDPOINT_URL";

// Global singleton for Lambda warm starts
static SCHEMA_STATE: OnceCell<Arc<SchemaServiceState>> = OnceCell::const_new();

/// Initialize the schema service state (once per cold start)
pub(crate) async fn get_or_init_state() -> Result<Arc<SchemaServiceState>, Error> {
    // S3 bucket for the four domain blobs + wasm prefix. Set by the
    // CDK stack; the Lambda refuses to start without it. We build the
    // S3 client and the persistence backend outside OnceCell so any
    // failure surfaces as a cold-start error rather than a swallowed
    // panic from inside the init closure.
    let bucket = env::var("SCHEMA_STORE_BUCKET").map_err(|_| {
        Error::from(
            "SCHEMA_STORE_BUCKET env var is not set. The schema service Lambda requires \
             an S3 bucket to persist schemas, canonical fields, apps, and near-misses. \
             See CDK: schema-stack.ts",
        )
    })?;
    let embeddings_table = env::var("SCHEMA_EMBEDDINGS_TABLE").map_err(|_| {
        Error::from(
            "SCHEMA_EMBEDDINGS_TABLE env var is not set. The schema service Lambda \
             persists fastembed vectors in DynamoDB so cold start doesn't recompute \
             them on every invocation. See CDK: schema-stack.ts (SchemaEmbeddingsTable).",
        )
    })?;
    let schema_store_endpoint_url = schema_store_endpoint_url_from_env();
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let schema_store_credentials = if schema_store_endpoint_url.is_some() {
        schema_store_s3_credentials_from_env().map_err(Error::from)?
    } else {
        None
    };
    let s3_config = schema_store_s3_config_builder(
        aws_sdk_s3::config::Builder::from(&aws_config),
        schema_store_endpoint_url.as_deref(),
        schema_store_credentials.as_ref(),
    );
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config.build());
    let ddb_client = aws_sdk_dynamodb::Client::new(&aws_config);
    let schema_mutation_gate_store = schema_mutation_gate_quota_store_from_env(&ddb_client);
    let backend: Arc<dyn ExternalSchemaPersistence> = Arc::new(
        s3_persistence::S3BlobPersistence::new(
            s3_client,
            bucket.clone(),
            ddb_client,
            embeddings_table,
        )
        .with_definition_namespace_from_env()?,
    );
    let embedder = default_embedder();

    SCHEMA_STATE
        .get_or_try_init(|| async move {
            tracing::info!(
                bucket = %bucket,
                endpoint_url = schema_store_endpoint_url.as_deref().unwrap_or("aws-default"),
                "Initializing schema service with S3-compatible blob storage",
            );

            let state = SchemaServiceState::new_with_external_and_schema_mutation_gate_store(
                backend,
                embedder,
                schema_mutation_gate_store,
                SchemaConfig::from_env(),
            )
            .await
            .map_err(|e| {
                Error::from(format!(
                    "Failed to initialize schema service against S3-compatible bucket '{bucket}': {e}. \
                         Check that the bucket exists, the Lambda's IAM role has \
                         s3:GetObject and s3:PutObject on it (or the configured \
                         R2 token can read/write the bucket), and the domain blobs \
                         (definitions.json, apps.json, near_misses.json) \
                         are either absent or valid JSON."
                ))
            })?;

            configure_and_seed(&state).await?;
            Ok(Arc::new(state))
        })
        .await
        .cloned()
}

/// Apply the env-driven gates to a freshly loaded state, then seed the
/// service-owned canonical fields and built-in schemas.
async fn configure_and_seed(state: &SchemaServiceState) -> Result<(), Error> {
    // App identity, the mutation gate, and the canonicalization gates were
    // loaded once in `SchemaConfig::from_env` before state construction.
    // With no trusted roots, /v1/apps rejects every cert (401) and the
    // /v1/schemas owner_app_id gate is a passthrough.

    // Seed the curated canonical field registry BEFORE seeding
    // schemas, so that schema field classification hits the
    // pre-populated entries on the first pass and skips the
    // Anthropic LLM round-trip per field.
    schema_service_core::builtin_canonical_fields::seed(state)
        .await
        .map_err(|e| {
            Error::from(format!(
                "Failed to seed pre-populated canonical fields: {e}"
            ))
        })?;

    // Schema.org types and property rows are not live language
    // (preference-schema-org-not-live-language). Do not call
    // schema_org_seeds::seed here.

    // Seed service-owned built-in schemas. Idempotent:
    // already-present schemas are skipped by identity_hash match.
    schema_service_server_shared::builtin_schemas::seed(state)
        .await
        .map_err(|e| Error::from(format!("Failed to seed built-in schemas: {e}.")))?;

    tracing::info!(
        "Schema service initialized and built-ins seeded ({} descriptive names)",
        schema_service_server_shared::builtin_schemas::PHASE_1_DESCRIPTIVE_NAMES.len()
    );
    Ok(())
}

fn schema_mutation_gate_quota_table_from_env() -> Option<String> {
    env_var_trimmed(SCHEMA_MUTATION_GATE_QUOTA_TABLE_ENV)
}

fn schema_store_endpoint_url_from_env() -> Option<String> {
    env_var_trimmed(SCHEMA_STORE_ENDPOINT_URL_ENV)
        .or_else(|| env_var_trimmed(SCHEMA_STORE_R2_ENDPOINT_ENV))
}

fn schema_mutation_gate_quota_store_from_env(
    ddb_client: &aws_sdk_dynamodb::Client,
) -> schema_service_core::SchemaMutationGateStore {
    schema_mutation_gate_quota_table_from_env().map_or_else(
        schema_service_core::SchemaMutationGateStore::default,
        |table| {
            s3_persistence::DynamoDbSchemaMutationGateStore::new(ddb_client.clone(), table)
                .into_schema_mutation_gate_store()
        },
    )
}

#[cfg(feature = "fastembed")]
fn default_embedder() -> Arc<dyn schema_service_server_shared::Embedder> {
    Arc::new(schema_service_server_shared::FoldDbFastEmbedder::from_lambda_layer())
}

#[cfg(not(feature = "fastembed"))]
fn default_embedder() -> Arc<dyn schema_service_server_shared::Embedder> {
    Arc::new(schema_service_core::DisabledEmbeddingModel)
}
