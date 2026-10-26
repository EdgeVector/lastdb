//! Proof for /v2 registry client + compat row signature verification.
//!
//! Demonstrates:
//! 1. Verifier-signed compat rows (with and without verification_url) verify correctly
//! 2. Tampered rows are rejected  
//! 3. Wrong-key rows are rejected
//! 4. Client fails hard when service is unreachable
//!
//! Run: cargo run --manifest-path lastdb_node/Cargo.toml --example v2_registry_client_proof

use app_identity_crypto::{canonicalize, sign, SigningKey};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use lastdb_node::app_release_host::{verify_compat_row, CompatRowRead, ReleaseRegistryClient};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== /v2 Registry Client Signature Verification Proof ===\n");

    let verifier_key_bytes: [u8; 32] = [
        0x2a, 0x86, 0x58, 0xff, 0x0a, 0x06, 0x21, 0xa9, 0x6d, 0x87, 0x09, 0xc1, 0x0c, 0xb1, 0x72,
        0x11, 0xe0, 0x30, 0xc0, 0x7a, 0x4e, 0x26, 0xe1, 0xeb, 0x79, 0x58, 0x81, 0x03, 0x97, 0x80,
        0xda, 0x7c,
    ];
    let verifier_key = SigningKey::from_bytes(&verifier_key_bytes);
    let verifier_pubkey_b64 = BASE64.encode(verifier_key.verifying_key().as_bytes());
    println!("Verifier key: {verifier_pubkey_b64}\n");

    // Test 1: Compat row WITH verification_url
    verify_row_with_url(&verifier_key, &verifier_pubkey_b64)?;

    // Test 2: Compat row WITHOUT verification_url
    verify_row_without_url(&verifier_key, &verifier_pubkey_b64)?;

    // Test 3: Tampered row rejected
    tampered_row_rejected(&verifier_key, &verifier_pubkey_b64)?;

    // Test 4: Wrong-key row rejected
    wrong_key_rejected(&verifier_key, &verifier_pubkey_b64)?;

    // Test 5: Service unreachable fails hard
    service_unreachable_fails().await?;

    println!("\n=== ✅ All signature verification cases passed ===");
    Ok(())
}

fn verify_row_with_url(
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 1: Compat row WITH verification_url ---");

    let release_id = "1111aaaa2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000";
    let contract_version = "1.2.3";
    let verification_url = "https://github.com/EdgeVector/lastdb/actions/runs/456";

    let payload = json!({
        "release_id": release_id,
        "contract_version": contract_version,
        "verification_url": verification_url,
    });

    let canonical = canonicalize(&payload)?;
    let payload_hash = fold_db::hex::sha256_hex(&canonical);
    let signature = sign(verifier_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    let row = CompatRowRead {
        release_id: release_id.to_string(),
        contract_version: contract_version.to_string(),
        verifier_pubkey: verifier_pubkey.to_string(),
        verified_at: "2026-10-26T20:00:00Z".to_string(),
        verification_url: Some(verification_url.to_string()),
        payload_hash: payload_hash.clone(),
        signature: signature_b64,
    };

    verify_compat_row(&row, verifier_pubkey)?;
    println!("✓ Row with verification_url verified");
    println!("  payload_hash: {payload_hash}");
    println!("  verification_url: Some(\"{verification_url}\")\n");

    Ok(())
}

fn verify_row_without_url(
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 2: Compat row WITHOUT verification_url ---");

    let release_id = "2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000aaaabbbb";
    let contract_version = "1.3.0";

    let payload = json!({
        "release_id": release_id,
        "contract_version": contract_version,
    });

    let canonical = canonicalize(&payload)?;
    let payload_hash = fold_db::hex::sha256_hex(&canonical);
    let signature = sign(verifier_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    let row = CompatRowRead {
        release_id: release_id.to_string(),
        contract_version: contract_version.to_string(),
        verifier_pubkey: verifier_pubkey.to_string(),
        verified_at: "2026-10-26T20:00:00Z".to_string(),
        verification_url: None,
        payload_hash: payload_hash.clone(),
        signature: signature_b64,
    };

    verify_compat_row(&row, verifier_pubkey)?;
    println!("✓ Row without verification_url verified");
    println!("  payload_hash: {payload_hash}");
    println!("  verification_url: None\n");

    Ok(())
}

fn tampered_row_rejected(
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 3: Tampered row rejected ---");

    let payload = json!({
        "release_id": "cccc3333444455556666777788889999000011112222aaaabbbbccccddddeeee",
        "contract_version": "1.4.0",
    });

    let canonical = canonicalize(&payload)?;
    let signature = sign(verifier_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    // Create row with tampered contract_version
    let tampered_payload = json!({
        "release_id": "cccc3333444455556666777788889999000011112222aaaabbbbccccddddeeee",
        "contract_version": "99.99.99",
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
        Ok(()) => Err("Expected tampered row to fail verification".into()),
        Err(e) => {
            println!("✓ Tampered row rejected");
            println!("  error: {e}\n");
            Ok(())
        }
    }
}

fn wrong_key_rejected(
    _verifier_key: &SigningKey,
    correct_verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 4: Row signed by wrong key rejected ---");

    let wrong_key_bytes: [u8; 32] = [
        0x12, 0x1f, 0x05, 0x43, 0x9f, 0xec, 0x65, 0x2f, 0xf8, 0xa1, 0xdb, 0x7b, 0x82, 0x8a, 0x4d,
        0xc4, 0xe4, 0x9e, 0xe3, 0x1f, 0x28, 0x83, 0xf3, 0x18, 0x70, 0x09, 0xc2, 0xca, 0x70, 0x82,
        0xc8, 0x6d,
    ];
    let wrong_key = SigningKey::from_bytes(&wrong_key_bytes);
    let wrong_pubkey_b64 = BASE64.encode(wrong_key.verifying_key().as_bytes());

    let payload = json!({
        "release_id": "bbbb2222333344445555666677778888999900001111aaaabbbbccccddddeeee",
        "contract_version": "1.5.0",
    });

    let canonical = canonicalize(&payload)?;
    let payload_hash = fold_db::hex::sha256_hex(&canonical);
    let signature = sign(&wrong_key, &canonical);
    let signature_b64 = BASE64.encode(signature);

    let row = CompatRowRead {
        release_id: "bbbb2222333344445555666677778888999900001111aaaabbbbccccddddeeee".to_string(),
        contract_version: "1.5.0".to_string(),
        verifier_pubkey: wrong_pubkey_b64,
        verified_at: "2026-10-26T20:00:00Z".to_string(),
        verification_url: None,
        payload_hash,
        signature: signature_b64,
    };

    match verify_compat_row(&row, correct_verifier_pubkey) {
        Ok(()) => Err("Expected wrong-key row to fail verification".into()),
        Err(e) => {
            println!("✓ Row with wrong key rejected");
            println!("  error: {e}\n");
            Ok(())
        }
    }
}

async fn service_unreachable_fails() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Case 5: Service unreachable exits nonzero ---");

    let client = ReleaseRegistryClient::new("http://localhost:99999");

    match client.list_apps().await {
        Ok(_) => Err("Expected service unreachable error".into()),
        Err(e) => {
            println!("✓ Service unreachable detected (would exit nonzero)");
            println!("  error: {e}\n");
            Ok(())
        }
    }
}
