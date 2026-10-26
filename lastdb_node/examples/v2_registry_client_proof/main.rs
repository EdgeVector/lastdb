//! End-to-end proof for /v2 registry client signature verification.
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

mod roundtrip;
mod seed;

use app_identity_crypto::compute_payload_hash;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use lastdb_node::app_release_host::{verify_compat_row, CompatRowRead, ReleaseRegistryClient};
use serde_json::json;

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

    println!("--- Seeding S3 ---");
    let release_id = seed::seed_s3_data().await?;
    println!("✓ Seeded app and release in S3");
    println!("  Release ID: {}\n", release_id);

    let state = roundtrip::create_s3_state(&verifier_pubkey_b64).await?;

    let port = 19200u16;
    let service_url = format!("http://localhost:{}", port);
    println!("--- Starting Schema Service ---");
    println!("Server command:");
    println!(
        "  cargo run -p schema_service_server_http --bin schema_service -- --port {}",
        port
    );
    println!("  (Embedded S3-backed server with verifier key configured)");

    roundtrip::start_server(state, port);

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("✓ Server listening on {}\n", service_url);

    health_check(&service_url).await?;
    post_compat_rows(&service_url, &release_id, &verifier_key).await?;

    let client = ReleaseRegistryClient::new(&service_url);
    verify_all_rows(&client, &release_id, &verifier_pubkey_b64).await?;
    test_tampered_row(&client, &release_id, &verifier_pubkey_b64).await?;
    test_wrong_key(&release_id, &wrong_key, &verifier_pubkey_b64).await?;
    test_unreachable_service().await?;

    println!("=== ✅ All end-to-end tests passed ===");
    Ok(())
}

async fn health_check(service_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Health Check: GET /v2/apps ---");
    let client = ReleaseRegistryClient::new(service_url);
    match client.list_apps().await {
        Ok(_) => println!("✓ Service is reachable\n"),
        Err(e) => {
            println!("✗ Service unreachable: {}", e);
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn post_compat_rows(
    service_url: &str,
    release_id: &str,
    verifier_key: &ed25519_dalek::SigningKey,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- POST Compat Rows ---");

    let body_with_url = json!({
        "release_id": release_id,
        "contract_version": "1.2.3",
        "verification_url": "https://github.com/EdgeVector/lastdb/actions/runs/456"
    });
    let sig_with_url = roundtrip::sign_body(verifier_key, &body_with_url)?;
    let status1 =
        roundtrip::post_compat_row(service_url, release_id, &sig_with_url, &body_with_url).await?;
    println!("✓ POST with verification_url: HTTP {}", status1);

    let body_without_url = json!({
        "release_id": release_id,
        "contract_version": "1.2.4",
    });
    let sig_without_url = roundtrip::sign_body(verifier_key, &body_without_url)?;
    let status2 =
        roundtrip::post_compat_row(service_url, release_id, &sig_without_url, &body_without_url)
            .await?;
    println!("✓ POST without verification_url: HTTP {}\n", status2);

    Ok(())
}

async fn verify_all_rows(
    client: &ReleaseRegistryClient,
    release_id: &str,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Read Back and Verify Signatures ---");
    let rows = client.list_compat_rows(release_id).await?;
    println!("Retrieved {} compat rows", rows.len());

    for row in &rows {
        match verify_compat_row(row, verifier_pubkey) {
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
    Ok(())
}

async fn test_tampered_row(
    client: &ReleaseRegistryClient,
    release_id: &str,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Tamper Test: Alter Stored Row ---");
    seed::tamper_stored_row(release_id, "1.2.3").await?;
    let tampered_rows = client.list_compat_rows(release_id).await?;
    let tampered = tampered_rows
        .iter()
        .find(|r| r.contract_version == "1.2.3")
        .ok_or("Tampered row not found")?;

    match verify_compat_row(tampered, verifier_pubkey) {
        Ok(()) => return Err("Tampered row should have failed verification".into()),
        Err(e) => {
            println!("✓ Tampered row rejected");
            println!("  error: {}\n", e);
        }
    }
    Ok(())
}

async fn test_wrong_key(
    release_id: &str,
    wrong_key: &ed25519_dalek::SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Wrong-Key Test ---");
    let wrong_key_body = json!({
        "release_id": release_id,
        "contract_version": "9.9.9",
    });
    let wrong_sig = roundtrip::sign_body(wrong_key, &wrong_key_body)?;
    let wrong_key_pubkey = BASE64.encode(wrong_key.verifying_key().as_bytes());

    let wrong_row = CompatRowRead {
        release_id: release_id.to_string(),
        contract_version: "9.9.9".to_string(),
        verifier_pubkey: wrong_key_pubkey.clone(),
        verified_at: chrono::Utc::now().to_rfc3339(),
        verification_url: None,
        payload_hash: compute_payload_hash(&wrong_key_body)?,
        signature: roundtrip::extract_signature(&wrong_sig)?,
    };

    match verify_compat_row(&wrong_row, verifier_pubkey) {
        Ok(()) => return Err("Wrong-key row should have been rejected".into()),
        Err(e) => {
            println!("✓ Wrong-key row rejected");
            println!("  error: {}\n", e);
        }
    }
    Ok(())
}

async fn test_unreachable_service() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Unreachable Service Test ---");
    let unreachable_client = ReleaseRegistryClient::new("http://127.0.0.1:1");
    match unreachable_client.list_apps().await {
        Ok(_) => return Err("Should have failed to connect to unused port".into()),
        Err(e) => {
            println!("✓ Connection failure detected (exits 1)");
            println!("  error: {}\n", e);
        }
    }
    Ok(())
}
