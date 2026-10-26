//! Round-trip proof for /v2 registry client + compat row verification.
//!
//! Posts verifier-signed compat rows (with and without verification_url),
//! reads them back, verifies signatures, and shows a tampered row rejected.
//!
//! Run against local schema service + moto S3:
//! ```bash
//! # Terminal 1:
//! moto_server -p 5000
//!
//! # Terminal 2 (in schema_service directory):
//! AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
//! S3_ENDPOINT=http://localhost:5000 S3_BUCKET=proof-v2 \
//! AWS_REGION=us-east-1 PORT=8080 \
//! APP_REGISTRY_VERIFIER_PUBKEY=KoZY/woGIalthwnBDLFyEeAwwHpOJuHreViBA5eA2nw= \
//! cargo run -p server_http
//!
//! # Terminal 3:
//! cargo run --manifest-path lastdb_node/Cargo.toml \
//!   --example v2_registry_client_proof
//! ```

use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, Env, Purpose, SignatureEnvelope, SigningKey,
    ALG_ED25519, ENVELOPE_VERSION,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use lastdb_node::app_release_host::{verify_compat_row, ReleaseRegistryClient};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== /v2 Registry Client Round-Trip Proof ===\n");

    let verifier_key_bytes: [u8; 32] = [
        0x2a, 0x86, 0x58, 0xff, 0x0a, 0x06, 0x21, 0xa9, 0x6d, 0x87, 0x09, 0xc1, 0x0c, 0xb1, 0x72,
        0x11, 0xe0, 0x30, 0xc0, 0x7a, 0x4e, 0x26, 0xe1, 0xeb, 0x79, 0x58, 0x81, 0x03, 0x97, 0x80,
        0xda, 0x7c,
    ];
    let verifier_key = SigningKey::from_bytes(&verifier_key_bytes);
    let verifier_pubkey_b64 = BASE64.encode(verifier_key.verifying_key().as_bytes());

    println!("Verifier key: {verifier_pubkey_b64}\n");

    let base_url =
        std::env::var("SCHEMA_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let client = ReleaseRegistryClient::new(&base_url);

    println!("Testing /v2 client against: {base_url}\n");

    ensure_service_reachable(&base_url).await?;

    seed_test_data(&base_url).await?;

    round_trip_with_url(&client, &verifier_key, &verifier_pubkey_b64).await?;
    round_trip_without_url(&client, &verifier_key, &verifier_pubkey_b64).await?;
    tampered_row_rejected(&client, &verifier_pubkey_b64).await?;

    println!("\n=== ✅ All round-trip cases verified ===");
    Ok(())
}

async fn ensure_service_reachable(base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Checking service reachability ---");
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    match http_client
        .get(&format!("{base_url}/v1/health"))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                println!("✓ Service reachable\n");
                Ok(())
            } else {
                Err(format!(
                    "Service unreachable at {base_url}: health check returned {}",
                    response.status()
                )
                .into())
            }
        }
        Err(e) => Err(format!("Service unreachable at {base_url}: {e}").into()),
    }
}

async fn seed_test_data(base_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Seeding test data ---");

    let app_payload = json!({
        "app_id": "proof-v2-app",
        "app_uuid": "00000000-0000-0000-0000-000000000001",
        "source_repo": "https://github.com/EdgeVector/lastdb-proof",
    });

    let dev_key_bytes: [u8; 32] = [
        0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];
    let dev_key = SigningKey::from_bytes(&dev_key_bytes);

    let app_sig = post_signed(
        &app_payload,
        &dev_key,
        Purpose::AppRegister,
        &format!("{base_url}/v2/apps"),
    )
    .await?;
    println!("✓ Created app: {app_sig}");

    let release_1_id = "1111aaaa2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000";
    let release_1 = json!({
        "release_id": release_1_id,
        "app_id": "proof-v2-app",
        "app_uuid": "00000000-0000-0000-0000-000000000001",
        "source_commit": "abc123def456",
        "artifact_digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "artifact_url": "file:///dev/null",
        "manifest": {
            "lastdb_contract_version": "1.0.0",
            "platform": "darwin-arm64",
        }
    });

    let rel_1_sig = post_signed(
        &release_1,
        &dev_key,
        Purpose::AppReleasePublish,
        &format!("{base_url}/v2/releases"),
    )
    .await?;
    println!("✓ Created release 1: {rel_1_sig}");

    let release_2_id = "2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000aaaabbbb";
    let release_2 = json!({
        "release_id": release_2_id,
        "app_id": "proof-v2-app",
        "app_uuid": "00000000-0000-0000-0000-000000000001",
        "source_commit": "def789abc012",
        "artifact_digest": "sha256:fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        "artifact_url": "file:///dev/null",
        "manifest": {
            "lastdb_contract_version": "1.0.0",
            "platform": "darwin-arm64",
        }
    });

    let rel_2_sig = post_signed(
        &release_2,
        &dev_key,
        Purpose::AppReleasePublish,
        &format!("{base_url}/v2/releases"),
    )
    .await?;
    println!("✓ Created release 2: {rel_2_sig}\n");

    Ok(())
}

async fn round_trip_with_url(
    client: &ReleaseRegistryClient,
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Round-trip: compat row WITH verification_url ---");

    let release_id = "1111aaaa2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000";
    let contract_version = "1.2.3";
    let verification_url = "https://github.com/EdgeVector/lastdb/actions/runs/456";

    let payload = json!({
        "release_id": release_id,
        "contract_version": contract_version,
        "verification_url": verification_url,
    });

    let base_url =
        std::env::var("SCHEMA_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let compat_sig = post_signed(
        &payload,
        verifier_key,
        Purpose::DevCert,
        &format!("{base_url}/v2/releases/{release_id}/compat"),
    )
    .await?;
    println!("✓ Posted compat row: {compat_sig}");

    let rows = client.list_compat_rows(release_id).await?;
    let row = rows
        .iter()
        .find(|r| r.contract_version == contract_version)
        .ok_or("Posted row not found in listing")?;

    println!("✓ Read back row:");
    let contract_version = &row.contract_version;
    let verification_url = &row.verification_url;
    let payload_hash = &row.payload_hash;
    println!("  contract_version: {contract_version}");
    println!("  verification_url: {verification_url:?}");
    println!("  payload_hash: {payload_hash}");

    verify_compat_row(row, verifier_pubkey)?;
    println!("✓ Signature verified\n");

    Ok(())
}

async fn round_trip_without_url(
    client: &ReleaseRegistryClient,
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Round-trip: compat row WITHOUT verification_url ---");

    let release_id = "2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000aaaabbbb";
    let contract_version = "1.3.0";

    let payload = json!({
        "release_id": release_id,
        "contract_version": contract_version,
    });

    let base_url =
        std::env::var("SCHEMA_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let compat_sig = post_signed(
        &payload,
        verifier_key,
        Purpose::DevCert,
        &format!("{base_url}/v2/releases/{release_id}/compat"),
    )
    .await?;
    println!("✓ Posted compat row: {compat_sig}");

    let rows = client.list_compat_rows(release_id).await?;
    let row = rows
        .iter()
        .find(|r| r.contract_version == contract_version)
        .ok_or("Posted row not found in listing")?;

    println!("✓ Read back row:");
    let contract_version = &row.contract_version;
    let verification_url = &row.verification_url;
    let payload_hash = &row.payload_hash;
    println!("  contract_version: {contract_version}");
    println!("  verification_url: {verification_url:?}");
    println!("  payload_hash: {payload_hash}");

    verify_compat_row(row, verifier_pubkey)?;
    println!("✓ Signature verified\n");

    Ok(())
}

async fn tampered_row_rejected(
    client: &ReleaseRegistryClient,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Tampered stored row rejected ---");

    let rows = client
        .list_compat_rows("1111aaaa2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000")
        .await?;
    let mut tampered = rows.first().ok_or("No rows to tamper")?.clone();

    tampered.contract_version = "99.99.99".to_string();

    match verify_compat_row(&tampered, verifier_pubkey) {
        Ok(()) => Err("Expected tampered row to fail verification".into()),
        Err(e) => {
            println!("✓ Tampered row rejected");
            println!("  error: {e}\n");
            Ok(())
        }
    }
}

async fn post_signed(
    payload: &Value,
    signing_key: &SigningKey,
    purpose: Purpose,
    url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let payload_hash = compute_payload_hash(payload)
        .map_err(|e| format!("Failed to compute payload hash: {e}"))?;

    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose,
        alg: ALG_ED25519.to_string(),
        key_id: key_id(&signing_key.verifying_key()),
        issued_at: chrono::Utc::now(),
        expires_at: None,
        env: Env::Dev,
        payload_hash: payload_hash.clone(),
        sig: None,
    };

    let signed = sign_envelope(signing_key, unsigned)
        .map_err(|e| format!("Envelope signing failed: {e}"))?;

    let sig_header =
        serde_json::to_string(&signed).map_err(|e| format!("Failed to encode envelope: {e}"))?;

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let response = http_client
        .post(url)
        .header("x-signature", sig_header)
        .json(payload)
        .send()
        .await?;

    let status = response.status().as_u16();
    let body = response.text().await?;

    if status != 200 && status != 201 {
        return Err(format!("POST {url} returned {status}: {body}").into());
    }

    Ok(payload_hash)
}
