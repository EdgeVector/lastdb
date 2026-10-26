//! Proof for /v2 compat-row write with signature verification.
//!
//! Demonstrates against moto S3:
//! 1. Correctly signed row → 201, persists, readable via load
//! 2. Row signed by wrong key → 401 signature_invalid
//! 3. Unsigned row (empty signature) → 401 signature_invalid
//! 4. Malformed signature → 401 signature_invalid
//! 5. Body altered after signing → 401 signature_invalid
//! 6. Duplicate row → 409 already_exists
//!
//! Run against moto:
//! ```bash
//! # Terminal 1:
//! moto_server -p 5000
//!
//! # Terminal 2:
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//! S3_ENDPOINT=http://localhost:5000 S3_BUCKET=proof-v2 \
//! AWS_REGION=us-east-1 \
//! cargo run -p schema_service_s3 --example v2_read_routes_proof
//! ```

use app_identity_crypto::{compute_payload_hash, Env, Purpose, SignatureEnvelope};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use schema_service_core::app_identity::AppIdentityConfig;
use schema_service_core::app_release::{ReleaseManifest, ReleaseRecord};
use schema_service_core::external_persistence::ExternalSchemaPersistence;
use schema_service_core::snapshot::{AppMetadata, AppRecord, AppTier};
use schema_service_core::SchemaServiceState;
use schema_service_s3::S3BlobPersistence;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== /v2 Compat-Row Proof (Storage + Signature) ===\n");
    let verifier_key_bytes: [u8; 32] = [
        0x9d, 0x2f, 0x1f, 0x4b, 0x8c, 0x8e, 0x9a, 0x0c, 0xfe, 0x3d, 0x1a, 0x8f, 0x6b, 0x5c, 0x7e,
        0x2d, 0xa1, 0xb3, 0xc4, 0xd5, 0xe6, 0xf7, 0x08, 0x19, 0x2a, 0x3b, 0x4c, 0x5d, 0x6e, 0x7f,
        0x80, 0x91,
    ];
    let verifier_key = ed25519_dalek::SigningKey::from_bytes(&verifier_key_bytes);
    let verifier_pubkey_b64 = BASE64.encode(verifier_key.verifying_key().as_bytes());
    let wrong_key_bytes: [u8; 32] = [
        0x1d, 0x2f, 0x1f, 0x4b, 0x8c, 0x8e, 0x9a, 0x0c, 0xfe, 0x3d, 0x1a, 0x8f, 0x6b, 0x5c, 0x7e,
        0x2d, 0xa1, 0xb3, 0xc4, 0xd5, 0xe6, 0xf7, 0x08, 0x19, 0x2a, 0x3b, 0x4c, 0x5d, 0x6e, 0x7f,
        0x80, 0x91,
    ];
    let wrong_key = ed25519_dalek::SigningKey::from_bytes(&wrong_key_bytes);

    println!("Verifier key: {}", verifier_pubkey_b64);
    println!(
        "Wrong key: {}\n",
        BASE64.encode(wrong_key.verifying_key().as_bytes())
    );

    let (state, backend) = create_state(&verifier_pubkey_b64).await?;
    let release_id = seed_data(&state, &backend).await?;

    test_case_1_correct_signature(&state, &backend, &verifier_key, &release_id).await?;
    test_case_2_wrong_key(&state, &backend, &wrong_key, &release_id).await?;
    test_case_3_unsigned(&state, &backend, &release_id).await?;
    test_case_4_malformed_signature(&state, &backend, &release_id).await?;
    test_case_5_body_altered(&state, &backend, &verifier_key, &release_id).await?;
    test_case_6_duplicate(&state, &verifier_key, &release_id).await?;

    println!("\n=== ✅ All 6 cases verified ===");
    Ok(())
}

async fn create_state(
    verifier_pubkey: &str,
) -> Result<(Arc<SchemaServiceState>, Arc<S3BlobPersistence>), Box<dyn std::error::Error>> {
    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| "proof-v2".to_string());
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let aws_config = aws_config::from_env().load().await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Some(ep) = endpoint {
        s3_config_builder = s3_config_builder.endpoint_url(ep).force_path_style(true);
    }
    let s3_config = s3_config_builder.build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
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
    Ok((Arc::new(state), backend))
}

async fn seed_data(
    state: &Arc<SchemaServiceState>,
    backend: &Arc<S3BlobPersistence>,
) -> Result<String, Box<dyn std::error::Error>> {
    let app = AppRecord {
        app_id: "proof-app".to_string(),
        owner_dev_pubkey: "owner_key".to_string(),
        metadata: AppMetadata {
            display_name: "Proof App".to_string(),
            description: "Test".to_string(),
            homepage_url: "https://example.com".to_string(),
            icon_url: None,
        },
        version: "1.0.0".to_string(),
        registered_at: "2026-10-26T20:00:00Z".to_string(),
        tier: AppTier::Sandbox,
        code_signature: None,
        source: None,
        artifact: None,
        uses: vec![],
    };
    backend.save_app(&app).await?;
    let release_id = "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee";
    let release = ReleaseRecord {
        release_id: release_id.to_string(),
        manifest: ReleaseManifest {
            app_id: "proof-app".to_string(),
            app_uuid: "proof-uuid".to_string(),
            schemas: BTreeMap::new(),
            source_commit: "abc123".to_string(),
            artifact_digest: "digest123".to_string(),
            artifact_url: "https://example.com/artifact".to_string(),
            artifact_signature: "sig123".to_string(),
        },
        publisher_dev_pubkey: "owner_key".to_string(),
        published_at: "2026-10-26T20:00:00Z".to_string(),
        revoked_at: None,
        revocation_reason: None,
    };
    backend.save_release(&release).await?;
    state.install_loaded_releases(
        [(release_id.to_string(), release)].into_iter().collect(),
        HashMap::new(),
    )?;
    println!("✓ Seeded release {}\n", release_id);
    Ok(release_id.to_string())
}

fn sign_body(
    signing_key: &ed25519_dalek::SigningKey,
    body: &Value,
) -> Result<String, Box<dyn std::error::Error>> {
    use app_identity_crypto::canonicalize;
    use ed25519_dalek::Signer;
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

async fn test_case_1_correct_signature(
    state: &Arc<SchemaServiceState>,
    backend: &Arc<S3BlobPersistence>,
    verifier_key: &ed25519_dalek::SigningKey,
    release_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 1: Correctly signed row ---");

    let body = json!({
        "release_id": release_id,
        "contract_version": "1.2.3",
        "verification_url": "https://github.com/org/verifier/runs/123"
    });

    let sig_header = sign_body(verifier_key, &body)?;

    match state.write_compat_row(release_id, &sig_header, &body).await {
        Ok(record) => {
            println!("✓ Status: 201 Created");
            println!("  release_id: {}", record.release_id);
            println!("  contract_version: {}", record.contract_version);
            println!("  verifier_pubkey: {}", record.verifier_pubkey);
            println!("  payload_hash: {}", record.payload_hash);
            println!("  signature: {}...", &record.signature[..20]);
            assert!(
                !record.payload_hash.is_empty(),
                "payload_hash must be present"
            );
            assert!(!record.signature.is_empty(), "signature must be present");
        }
        Err(e) => {
            panic!("Expected 201, got error: {:?}", e);
        }
    }

    let rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(rows.len(), 1);
    println!("✓ Row persisted and readable via storage load");
    println!("  Stored payload_hash: {}", rows[0].payload_hash);
    println!("  Stored signature: {}...", &rows[0].signature[..20]);

    Ok(())
}

async fn test_case_2_wrong_key(
    state: &Arc<SchemaServiceState>,
    backend: &Arc<S3BlobPersistence>,
    wrong_key: &ed25519_dalek::SigningKey,
    release_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 2: Row signed by wrong key ---");

    let body = json!({
        "release_id": release_id,
        "contract_version": "1.3.0",
    });

    let sig_header = sign_body(wrong_key, &body)?;

    match state.write_compat_row(release_id, &sig_header, &body).await {
        Ok(_) => panic!("Expected 401, got 201"),
        Err(e) => {
            let (status, body) = e.to_http();
            println!(
                "✓ Status: {} {}",
                status,
                body["reason"].as_str().unwrap_or("")
            );
            assert_eq!(status, 401);
            assert_eq!(body["reason"], "signature_invalid");
        }
    }

    let rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(rows.len(), 1, "Storage unchanged: still 1 row");
    println!("✓ Storage unchanged (still 1 row from case 1)");

    Ok(())
}

async fn test_case_3_unsigned(
    state: &Arc<SchemaServiceState>,
    backend: &Arc<S3BlobPersistence>,
    release_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 3: Unsigned row (no signature) ---");

    let body = json!({
        "release_id": release_id,
        "contract_version": "1.4.0",
    });

    match state.write_compat_row(release_id, "", &body).await {
        Ok(_) => panic!("Expected 401, got 201"),
        Err(e) => {
            let (status, body) = e.to_http();
            println!(
                "✓ Status: {} {}",
                status,
                body["reason"].as_str().unwrap_or("")
            );
            assert_eq!(status, 401);
            assert_eq!(body["reason"], "signature_invalid");
        }
    }

    let rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(rows.len(), 1, "Storage unchanged: still 1 row");
    println!("✓ Storage unchanged (still 1 row from case 1)");

    Ok(())
}

async fn test_case_4_malformed_signature(
    state: &Arc<SchemaServiceState>,
    backend: &Arc<S3BlobPersistence>,
    release_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 4: Malformed signature ---");

    let body = json!({
        "release_id": release_id,
        "contract_version": "1.4.0",
    });

    match state
        .write_compat_row(release_id, "not-valid-json", &body)
        .await
    {
        Ok(_) => panic!("Expected 401, got 201"),
        Err(e) => {
            let (status, body_val) = e.to_http();
            println!(
                "✓ Status: {} {}",
                status,
                body_val["reason"].as_str().unwrap_or("")
            );
            assert_eq!(status, 401);
            assert_eq!(body_val["reason"], "signature_invalid");
        }
    }

    let rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(rows.len(), 1, "Storage unchanged: still 1 row");
    println!("✓ Storage unchanged (still 1 row from case 1)");

    Ok(())
}

async fn test_case_5_body_altered(
    state: &Arc<SchemaServiceState>,
    backend: &Arc<S3BlobPersistence>,
    verifier_key: &ed25519_dalek::SigningKey,
    release_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 5: Body altered after signing ---");

    let original_body = json!({
        "release_id": release_id,
        "contract_version": "1.5.0",
    });

    let sig_header = sign_body(verifier_key, &original_body)?;
    let tampered_body = json!({
        "release_id": release_id,
        "contract_version": "99.99.99",
    });

    match state
        .write_compat_row(release_id, &sig_header, &tampered_body)
        .await
    {
        Ok(_) => panic!("Expected 401, got 201"),
        Err(e) => {
            let (status, body_val) = e.to_http();
            println!(
                "✓ Status: {} {}",
                status,
                body_val["reason"].as_str().unwrap_or("")
            );
            assert_eq!(status, 401);
            assert_eq!(body_val["reason"], "signature_invalid");
        }
    }

    let rows = backend.load_compat_rows_for_release(release_id).await?;
    assert_eq!(rows.len(), 1, "Storage unchanged: still 1 row");
    println!("✓ Storage unchanged (still 1 row from case 1)");

    Ok(())
}

async fn test_case_6_duplicate(
    state: &Arc<SchemaServiceState>,
    verifier_key: &ed25519_dalek::SigningKey,
    release_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 6: Duplicate row (same release_id + contract_version) ---");

    let body = json!({
        "release_id": release_id,
        "contract_version": "1.2.3",
        "verification_url": "https://github.com/org/verifier/runs/456"
    });

    let sig_header = sign_body(verifier_key, &body)?;

    match state.write_compat_row(release_id, &sig_header, &body).await {
        Ok(_) => panic!("Expected 409, got 201"),
        Err(e) => {
            let (status, body_val) = e.to_http();
            println!(
                "✓ Status: {} {}",
                status,
                body_val["reason"].as_str().unwrap_or("")
            );
            assert_eq!(status, 409);
            assert_eq!(body_val["reason"], "already_exists");
        }
    }

    println!("✓ Duplicate rejected with 409");

    Ok(())
}
