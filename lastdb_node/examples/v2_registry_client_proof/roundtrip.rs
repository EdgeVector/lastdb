//! Server setup and HTTP round-trip operations for the proof.

use actix_web::{web, App, HttpServer};
use app_identity_crypto::{canonicalize, compute_payload_hash, Env, Purpose, SignatureEnvelope};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use ed25519_dalek::Signer;
use schema_service_core::app_identity::AppIdentityConfig;
use schema_service_core::{ExternalSchemaPersistence, SchemaConfig, SchemaServiceState};
use schema_service_s3::S3BlobPersistence;
use schema_service_server_shared::{SchemaConfigEnv, SystemClock};
use std::collections::HashMap;
use std::sync::Arc;

/// Creates an S3-backed SchemaServiceState for the embedded server.
pub async fn create_s3_state(
    verifier_pubkey: &str,
) -> Result<Arc<SchemaServiceState>, Box<dyn std::error::Error>> {
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "proof-v2".to_string());
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(ep) = endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(ep).force_path_style(true);
    }
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());
    let ddb_client = aws_sdk_dynamodb::Client::new(&aws_config);

    let _ = s3_client.create_bucket().bucket(&bucket).send().await;

    let backend = Arc::new(S3BlobPersistence::new(
        s3_client,
        bucket,
        ddb_client,
        "proof-embeddings".to_string(),
    ));
    let embedder = Arc::new(schema_service_core::DisabledEmbeddingModel);
    let state = SchemaServiceState::new_with_external(
        backend.clone(),
        embedder,
        SchemaConfig::from_env(),
        Arc::new(SystemClock),
    )
    .await?;
    state.configure_app_identity(AppIdentityConfig {
        deployment_env: Env::Dev,
        trusted_roots: HashMap::new(),
        revoked_dev_pubkeys: Default::default(),
        verifier_pubkey: Some(verifier_pubkey.to_string()),
    });

    let loaded_apps = backend.load_all_apps().await.unwrap_or_default();
    {
        let mut apps = state.apps.write().unwrap();
        apps.extend(loaded_apps);
    }
    let loaded_releases = backend.load_all_releases().await.unwrap_or_default();
    state.install_loaded_releases(loaded_releases, HashMap::new())?;

    Ok(Arc::new(state))
}

/// Starts the embedded actix-web server in a background thread.
pub fn start_server(state: Arc<SchemaServiceState>, port: u16) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(state.clone()))
                    .configure(schema_service_server_http::configure_routes)
            })
            .bind(("127.0.0.1", port))
            .unwrap();

            let _ = server.run().await;
        });
    });
}

/// Signs a JSON body with an Ed25519 key and returns the signature envelope.
pub fn sign_body(
    signing_key: &ed25519_dalek::SigningKey,
    body: &serde_json::Value,
) -> Result<String, Box<dyn std::error::Error>> {
    let payload_hash = compute_payload_hash(body)?;
    let body_canonical = canonicalize(body)?;
    let signature = signing_key.sign(&body_canonical);
    let envelope = SignatureEnvelope {
        version: 1,
        purpose: Purpose::AppReleasePublish,
        alg: "ed25519".to_string(),
        key_id: "verifier".to_string(),
        issued_at: chrono::Utc::now(),
        expires_at: None,
        env: Env::Dev,
        payload_hash,
        sig: Some(BASE64.encode(signature.to_bytes())),
    };
    Ok(serde_json::to_string(&envelope)?)
}

/// Extracts the base64-encoded signature from a signature envelope.
pub fn extract_signature(sig_header: &str) -> Result<String, Box<dyn std::error::Error>> {
    let envelope: SignatureEnvelope = serde_json::from_str(sig_header)?;
    envelope
        .sig
        .ok_or_else(|| "No signature in envelope".into())
}

/// POSTs a signed compat row to the server and returns the HTTP status code.
pub async fn post_compat_row(
    base_url: &str,
    release_id: &str,
    sig_header: &str,
    body: &serde_json::Value,
) -> Result<u16, Box<dyn std::error::Error>> {
    let url = format!("{}/v2/releases/{}/compat", base_url, release_id);
    let response = reqwest::Client::new()
        .post(&url)
        .header("X-Signature", sig_header)
        .json(body)
        .send()
        .await?;
    let status = response.status().as_u16();
    if !response.status().is_success() {
        let error_body = response.text().await?;
        return Err(format!("POST failed ({}): {}", status, error_body).into());
    }
    Ok(status)
}
