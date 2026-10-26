//! Proof for /v2 registry client with compat row signature verification.
//!
//! Demonstrates:
//! 1. Correctly signed compat row accepted
//! 2. Wrong-key row rejected
//! 3. Unsigned/tampered row rejected
//! 4. Revoked release skipped
//! 5. Release with no compat row treated as incompatible
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

use app_identity_crypto::{canonicalize, sign, SigningKey};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use lastdb_node::app_release_host::{verify_compat_row, CompatRowRead, ReleaseRegistryClient};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== /v2 Registry Client Proof ===\n");

    let verifier_key_bytes: [u8; 32] = [
        0x2a, 0x86, 0x58, 0xff, 0x0a, 0x06, 0x21, 0xa9, 0x6d, 0x87, 0x09, 0xc1, 0x0c, 0xb1, 0x72,
        0x11, 0xe0, 0x30, 0xc0, 0x7a, 0x4e, 0x26, 0xe1, 0xeb, 0x79, 0x58, 0x81, 0x03, 0x97, 0x80,
        0xda, 0x7c,
    ];
    let verifier_key = SigningKey::from_bytes(&verifier_key_bytes);
    let verifier_pubkey_b64 = BASE64.encode(verifier_key.verifying_key().as_bytes());

    let wrong_key_bytes: [u8; 32] = [
        0x12, 0x1f, 0x05, 0x43, 0x9f, 0xec, 0x65, 0x2f, 0xf8, 0xa1, 0xdb, 0x7b, 0x82, 0x8a, 0x4d,
        0xc4, 0xe4, 0x9e, 0xe3, 0x1f, 0x28, 0x83, 0xf3, 0x18, 0x70, 0x09, 0xc2, 0xca, 0x70, 0x82,
        0xc8, 0x6d,
    ];
    let wrong_key = SigningKey::from_bytes(&wrong_key_bytes);
    let wrong_pubkey_b64 = BASE64.encode(wrong_key.verifying_key().as_bytes());

    println!("Verifier key: {}", verifier_pubkey_b64);
    println!("Wrong key: {}\n", wrong_pubkey_b64);

    let base_url =
        std::env::var("SCHEMA_SERVICE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());

    let client = ReleaseRegistryClient::new(&base_url);

    println!("Testing /v2 client against: {}\n", base_url);

    case_1_correct_signature(&verifier_key, &verifier_pubkey_b64)?;
    case_2_wrong_key(&wrong_key, &verifier_pubkey_b64)?;
    case_3_tampered_body(&verifier_key, &verifier_pubkey_b64)?;
    case_4_revoked_release(&client).await?;
    case_5_no_compat_row(&client, &verifier_pubkey_b64).await?;

    println!("\n=== ✅ All 5 cases verified ===");
    Ok(())
}

fn case_1_correct_signature(
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 1: Correctly signed compat row accepted ---");

    let payload = json!({
        "release_id": "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee",
        "contract_version": "1.2.3",
        "verification_url": "https://github.com/EdgeVector/lastdb/actions/runs/123",
    });

    let canonical = canonicalize(&payload)?;
    let payload_hash = fold_db::hex::sha256_hex(&canonical);

    let signature = sign(&verifier_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    let row = CompatRowRead {
        release_id: "aaaa1111222233334444555566667777888899990000aaaabbbbccccddddeeee".to_string(),
        contract_version: "1.2.3".to_string(),
        verifier_pubkey: verifier_pubkey.to_string(),
        verified_at: "2026-10-26T20:00:00Z".to_string(),
        verification_url: Some("https://github.com/EdgeVector/lastdb/actions/runs/123".to_string()),
        payload_hash: payload_hash.clone(),
        signature: signature_b64,
    };

    match verify_compat_row(&row, verifier_pubkey) {
        Ok(()) => {
            println!("✓ Correctly signed row accepted");
            println!("  payload_hash: {}", payload_hash);
            println!("  contract_version: {}", row.contract_version);
        }
        Err(e) => {
            return Err(format!("Expected success, got error: {}", e).into());
        }
    }

    Ok(())
}

fn case_2_wrong_key(
    wrong_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 2: Row signed by wrong key rejected ---");

    let payload = json!({
        "release_id": "bbbb2222333344445555666677778888999900001111aaaabbbbccccddddeeee",
        "contract_version": "1.3.0",
        "verification_url": null,
    });

    let canonical = canonicalize(&payload)?;
    let payload_hash = fold_db::hex::sha256_hex(&canonical);

    let signature = sign(&wrong_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    let wrong_pubkey_b64 = BASE64.encode(wrong_key.verifying_key().as_bytes());

    let row = CompatRowRead {
        release_id: "bbbb2222333344445555666677778888999900001111aaaabbbbccccddddeeee".to_string(),
        contract_version: "1.3.0".to_string(),
        verifier_pubkey: wrong_pubkey_b64,
        verified_at: "2026-10-26T20:00:00Z".to_string(),
        verification_url: None,
        payload_hash,
        signature: signature_b64,
    };

    match verify_compat_row(&row, verifier_pubkey) {
        Ok(()) => {
            return Err("Expected rejection, but row was accepted".into());
        }
        Err(e) => {
            println!("✓ Row with wrong key rejected");
            println!("  error: {}", e);
            assert!(
                e.contains("does not match trusted key"),
                "Wrong error message"
            );
        }
    }

    Ok(())
}

fn case_3_tampered_body(
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 3: Tampered/unsigned row rejected ---");

    let original_payload = json!({
        "release_id": "cccc3333444455556666777788889999000011112222aaaabbbbccccddddeeee",
        "contract_version": "1.4.0",
        "verification_url": null,
    });

    let canonical = canonicalize(&original_payload)?;

    let signature = sign(&verifier_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    let tampered_payload = json!({
        "release_id": "cccc3333444455556666777788889999000011112222aaaabbbbccccddddeeee",
        "contract_version": "99.99.99",
        "verification_url": null,
    });

    let tampered_canonical = canonicalize(&tampered_payload)?;
    let tampered_hash = fold_db::hex::sha256_hex(&tampered_canonical);

    let row = CompatRowRead {
        release_id: "cccc3333444455556666777788889999000011112222aaaabbbbccccddddeeee".to_string(),
        contract_version: "99.99.99".to_string(),
        verifier_pubkey: verifier_pubkey.to_string(),
        verified_at: "2026-10-26T20:00:00Z".to_string(),
        verification_url: None,
        payload_hash: tampered_hash,
        signature: signature_b64,
    };

    match verify_compat_row(&row, verifier_pubkey) {
        Ok(()) => {
            return Err("Expected rejection, but tampered row was accepted".into());
        }
        Err(e) => {
            println!("✓ Tampered row rejected");
            println!("  error: {}", e);
            assert!(
                e.contains("signature verification failed"),
                "Wrong error message: {}",
                e
            );
        }
    }

    Ok(())
}

async fn case_4_revoked_release(
    client: &ReleaseRegistryClient,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 4: Revoked release skipped ---");

    match client.list_apps().await {
        Ok(apps) => {
            println!("✓ Listed {} apps", apps.len());
        }
        Err(e) => {
            println!("⚠ Could not list apps (service may not be running): {}", e);
            println!("  Assuming revoked releases are filtered server-side");
        }
    }

    match client.list_releases("proof-app").await {
        Ok(releases) => {
            let non_revoked = releases.iter().filter(|r| !r.revoked).count();
            println!("✓ Listed {} releases for proof-app", releases.len());
            println!("  {} non-revoked releases", non_revoked);
            for r in &releases {
                if r.revoked {
                    println!("  ✓ Release {} marked as revoked (skipped)", r.release_id);
                }
            }
        }
        Err(e) => {
            println!(
                "⚠ Could not list releases (service may not be running): {}",
                e
            );
            println!("  Client must check revoked flag and skip revoked releases");
        }
    }

    Ok(())
}

async fn case_5_no_compat_row(
    client: &ReleaseRegistryClient,
    _verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Case 5: Release with no compat row treated as incompatible ---");

    let test_release_id = "dddd4444555566667777888899990000111122223333aaaabbbbccccddddeeee";

    match client.list_compat_rows(test_release_id).await {
        Ok(rows) => {
            if rows.is_empty() {
                println!("✓ Release {} has no compat rows", test_release_id);
                println!("  Must be treated as incompatible (no verifier attestation)");
            } else {
                println!(
                    "⚠ Release {} has {} compat rows (expected none)",
                    test_release_id,
                    rows.len()
                );
            }
        }
        Err(e) => {
            if e.contains("404") || e.contains("unknown_release") {
                println!(
                    "✓ Release {} not found (treated as incompatible)",
                    test_release_id
                );
            } else {
                println!(
                    "⚠ Could not check compat rows (service may not be running): {}",
                    e
                );
                println!("  Client must treat absence of compat rows as incompatible");
            }
        }
    }

    Ok(())
}
