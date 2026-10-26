//! End-to-end proof for /v2 registry client signature verification.
// lint:file-size-ok proof example with embedded test server setup
//!
//! This proof:
//! 1. Seeds app and release data directly into moto S3
//! 2. Starts an embedded actix server with S3-backed persistence
//! 3. Performs health check (GET /v2/apps)
//! 4. POSTs two verifier-signed compat rows via HTTP
//! 5. Reads them back via the lastdb client and verifies signatures
//! 6. Tampers with stored S3 data and shows verification failing
//! 7. Tests wrong-key rejection
//! 8. Tests connection failure to unused port
//!
//! Run against moto:
//! ```bash
//! # Terminal 1: Start moto S3
//! moto_server -p 5000
//!
//! # Terminal 2: Run proof
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//! S3_ENDPOINT=http://localhost:5000 S3_BUCKET=proof-v2 \
//! AWS_REGION=us-east-1 \
//! cargo run --manifest-path lastdb_node/Cargo.toml --example v2_registry_client_proof
//! ```

use actix_web::{web, App, HttpServer};
use app_identity_crypto::{canonicalize, compute_payload_hash, Env, Purpose, SignatureEnvelope};
use aws_sdk_s3::primitives::ByteStream;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use ed25519_dalek::Signer;
use lastdb_node::app_release_host::registry::{
    verify_compat_row, CompatRowRead, ReleaseRegistryClient,
};
use schema_service_core::app_identity::AppIdentityConfig;
use schema_service_core::{ExternalSchemaPersistence, SchemaServiceState};
use schema_service_s3::S3BlobPersistence;
use schema_service_server_shared::handlers;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== /v2 Registry Client End-to-End Proof ===\n");

    let verifier_key_bytes: [u8; 32] = [
        0xbf, 0x2d, 0xee, 0xa3, 0xbc, 0x4a, 0x38, 0x63, 0x72, 0x0b, 0x9a, 0xdc, 0x12, 0x68, 0xa5,
        0xec, 0x21, 0xf9, 0x52, 0xbc, 0x89, 0xdf, 0x75, 0x20, 0x8c, 0xe5, 0x06, 0x10, 0x26, 0x97,
        0x3a, 0xeb,
    ];
    let verifier_key = ed25519_dalek::SigningKey::from_bytes(&verifier_key_bytes);
    let verifier_pubkey_b64 = BASE64.encode(verifier_key.verifying_key().as_bytes());

    let wrong_key_bytes: [u8; 32] = [
        0x3f, 0xc3, 0x82, 0xae, 0x3a, 0xfd, 0xba, 0x7d, 0x58, 0x39, 0xd3, 0x27, 0x1d, 0xe8, 0x50,
        0x9f, 0xa5, 0x08, 0x45, 0xe9, 0x66, 0x47, 0xf1, 0xe5, 0x9e, 0xed, 0x1c, 0xde, 0x33, 0x39,
        0x6e, 0x71,
    ];
    let wrong_key = ed25519_dalek::SigningKey::from_bytes(&wrong_key_bytes);

    println!(
        "Verifier key fingerprint: {}...\n",
        &verifier_pubkey_b64[..12]
    );

    // Seed S3 data
    println!("--- Seeding S3 ---");
    let release_id = seed_s3_data().await?;
    println!("✓ Seeded app and release in S3");
    println!("  Release ID: {}\n", release_id);

    // Create S3-backed state
    let state = create_s3_state(&verifier_pubkey_b64).await?;

    // Start server in background
    let port = 19200u16;
    let service_url = format!("http://localhost:{}", port);
    println!("--- Starting Schema Service ---");
    println!("Server command:");
    println!(
        "  cargo run -p schema_service_server_http --bin schema_service -- --port {}",
        port
    );
    println!("  (Embedded S3-backed server with verifier key configured)");

    let state_clone = state.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(state_clone.clone()))
                    .configure(configure_routes)
            })
            .bind(("127.0.0.1", port))
            .unwrap();

            let _ = server.run().await;
        });
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("✓ Server listening on {}\n", service_url);

    // (a) Health check: GET /v2/apps
    println!("--- Health Check: GET /v2/apps ---");
    let client = ReleaseRegistryClient::new(&service_url);
    match client.list_apps().await {
        Ok(_) => println!("✓ Service is reachable\n"),
        Err(e) => {
            println!("✗ Service unreachable: {}", e);
            std::process::exit(1);
        }
    }

    // (c) Sign and POST two compat rows
    println!("--- POST Compat Rows ---");

    let body_with_url = json!({
        "release_id": release_id,
        "contract_version": "1.2.3",
        "verification_url": "https://github.com/EdgeVector/lastdb/actions/runs/456"
    });
    let sig_with_url = sign_body(&verifier_key, &body_with_url)?;
    let status1 = post_compat_row(&service_url, &release_id, &sig_with_url, &body_with_url).await?;
    println!("✓ POST with verification_url: HTTP {}", status1);

    let body_without_url = json!({
        "release_id": release_id,
        "contract_version": "1.2.4",
    });
    let sig_without_url = sign_body(&verifier_key, &body_without_url)?;
    let status2 = post_compat_row(
        &service_url,
        &release_id,
        &sig_without_url,
        &body_without_url,
    )
    .await?;
    println!("✓ POST without verification_url: HTTP {}\n", status2);

    // (d) Read back via client and verify signatures
    println!("--- Read Back and Verify Signatures ---");
    let rows = client.list_compat_rows(&release_id).await?;
    println!("Retrieved {} compat rows", rows.len());

    for row in &rows {
        match verify_compat_row(row, &verifier_pubkey_b64) {
            Ok(()) => {
                println!(
                    "✓ Row verified: contract_version={}, verification_url={:?}",
                    row.contract_version, row.verification_url
                );
                println!("  payload_hash: {}", row.payload_hash);
            }
            Err(e) => {
                return Err(format!("Verification failed: {}", e).into());
            }
        }
    }
    println!();

    // (e) Tamper with stored data and show verification failing
    println!("--- Tamper Test: Alter Stored Row ---");
    tamper_stored_row(&release_id, "1.2.3").await?;
    let tampered_rows = client.list_compat_rows(&release_id).await?;
    let tampered = tampered_rows
        .iter()
        .find(|r| r.contract_version == "1.2.3")
        .ok_or("Tampered row not found")?;

    match verify_compat_row(tampered, &verifier_pubkey_b64) {
        Ok(()) => return Err("Tampered row should have failed verification".into()),
        Err(e) => {
            println!("✓ Tampered row rejected");
            println!("  error: {}\n", e);
        }
    }

    // Wrong-key rejection
    println!("--- Wrong-Key Test ---");
    let wrong_key_body = json!({
        "release_id": release_id,
        "contract_version": "9.9.9",
    });
    let wrong_sig = sign_body(&wrong_key, &wrong_key_body)?;
    let wrong_key_pubkey = BASE64.encode(wrong_key.verifying_key().as_bytes());

    let wrong_row = CompatRowRead {
        release_id: release_id.clone(),
        contract_version: "9.9.9".to_string(),
        verifier_pubkey: wrong_key_pubkey.clone(),
        verified_at: chrono::Utc::now().to_rfc3339(),
        verification_url: None,
        payload_hash: compute_payload_hash(&wrong_key_body)?,
        signature: extract_signature(&wrong_sig)?,
    };

    match verify_compat_row(&wrong_row, &verifier_pubkey_b64) {
        Ok(()) => return Err("Wrong-key row should have been rejected".into()),
        Err(e) => {
            println!("✓ Wrong-key row rejected");
            println!("  error: {}\n", e);
        }
    }

    // (f) Connection failure to unused port
    println!("--- Unreachable Service Test ---");
    let unreachable_client = ReleaseRegistryClient::new("http://127.0.0.1:1");
    match unreachable_client.list_apps().await {
        Ok(_) => return Err("Should have failed to connect to unused port".into()),
        Err(e) => {
            println!("✓ Connection failure detected (exits 1)");
            println!("  error: {}\n", e);
        }
    }

    println!("=== ✅ All end-to-end tests passed ===");
    Ok(())
}

fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/v2")
            .route("/apps", web::get().to(handlers::list_apps))
            .service(
                web::resource("/releases/{release_id}/compat")
                    .route(web::post().to(handlers::write_compat_row))
                    .route(web::get().to(handlers::list_compat_rows)),
            ),
    );
}

async fn create_s3_state(
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
    let state = SchemaServiceState::new_with_external(backend.clone(), embedder).await?;
    state.configure_app_identity(AppIdentityConfig {
        deployment_env: Env::Dev,
        trusted_roots: HashMap::new(),
        revoked_dev_pubkeys: Default::default(),
        verifier_pubkey: Some(verifier_pubkey.to_string()),
    });

    // Load existing apps and releases from S3
    let loaded_apps = backend.load_all_apps().await.unwrap_or_default();
    {
        let mut apps = state.apps.write().unwrap();
        apps.extend(loaded_apps);
    }
    let loaded_releases = backend.load_all_releases().await.unwrap_or_default();
    state.install_loaded_releases(loaded_releases, HashMap::new())?;

    Ok(Arc::new(state))
}

fn sign_body(
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

fn extract_signature(sig_header: &str) -> Result<String, Box<dyn std::error::Error>> {
    let envelope: SignatureEnvelope = serde_json::from_str(sig_header)?;
    envelope
        .sig
        .ok_or_else(|| "No signature in envelope".into())
}

async fn post_compat_row(
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

async fn seed_s3_data() -> Result<String, Box<dyn std::error::Error>> {
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

    let _ = s3_client.create_bucket().bucket(&bucket).send().await;

    let app = json!({
        "app_id": "proof-app",
        "owner_dev_pubkey": "owner_key",
        "metadata": {
            "display_name": "Proof App",
            "description": "Test",
            "homepage_url": "https://example.com",
            "icon_url": null
        },
        "version": "1.0.0",
        "registered_at": "2026-10-26T21:00:00Z",
        "tier": "Live"
    });

    s3_client
        .put_object()
        .bucket(&bucket)
        .key("apps/proof-app.json")
        .body(ByteStream::from(serde_json::to_vec(&app)?))
        .content_type("application/json")
        .send()
        .await?;

    let release_id = "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee";
    let release = json!({
        "release_id": release_id,
        "manifest": {
            "app_id": "proof-app",
            "app_uuid": "proof-uuid",
            "schemas": {},
            "source_commit": "abc123",
            "artifact_digest": "digest123",
            "artifact_url": "https://example.com/artifact",
            "artifact_signature": "sig123"
        },
        "publisher_dev_pubkey": "owner_key",
        "published_at": "2026-10-26T21:00:00Z"
    });

    s3_client
        .put_object()
        .bucket(&bucket)
        .key(format!("releases/{}.json", release_id))
        .body(ByteStream::from(serde_json::to_vec(&release)?))
        .content_type("application/json")
        .send()
        .await?;

    Ok(release_id.to_string())
}

async fn tamper_stored_row(
    release_id: &str,
    contract_version: &str,
) -> Result<(), Box<dyn std::error::Error>> {
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

    let key = format!("compat/{}/{}.json", release_id, contract_version);

    let obj = s3_client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;

    let body = obj.body.collect().await?;
    let mut row: serde_json::Value = serde_json::from_slice(&body.into_bytes())?;

    row["contract_version"] = json!("99.99.99");

    s3_client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(ByteStream::from(serde_json::to_vec(&row)?))
        .content_type("application/json")
        .send()
        .await?;

    Ok(())
}
