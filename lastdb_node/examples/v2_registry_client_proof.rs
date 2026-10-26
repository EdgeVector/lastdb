//! Round-trip proof for /v2 registry client + compat row verification.
//!
//! Creates a test trust chain, posts verifier-signed compat rows (with and
//! without verification_url), reads them back, verifies signatures, and shows
//! tampered/wrong-key rows rejected.
//!
//! Run with local schema service + moto (both auto-started by this proof):
//! ```bash
//! cargo run --manifest-path lastdb_node/Cargo.toml --example v2_registry_client_proof
//! ```

use app_identity_crypto::{
    compute_payload_hash, key_id, sign_envelope, DevCert, Env, Purpose, SignatureEnvelope,
    SigningKey, ALG_ED25519, ALG_ES256, DEV_CERT_VERSION, ENVELOPE_VERSION, PURPOSE_DEV_CERT,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use lastdb_node::app_release_host::{verify_compat_row, ReleaseRegistryClient};
use p256::ecdsa::{signature::Signer, Signature, SigningKey as P256SigningKey};
use serde_json::{json, Value};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const SERVICE_PORT: u16 = 19102;
const SERVICE_URL: &str = "http://localhost:19102";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== /v2 Registry Client Round-Trip Proof ===\n");

    let (root_key, root_pubkey_b64) = generate_root_key();
    println!("Generated test root key: {root_pubkey_b64}");

    let verifier_key_bytes: [u8; 32] = [
        0x2a, 0x86, 0x58, 0xff, 0x0a, 0x06, 0x21, 0xa9, 0x6d, 0x87, 0x09, 0xc1, 0x0c, 0xb1, 0x72,
        0x11, 0xe0, 0x30, 0xc0, 0x7a, 0x4e, 0x26, 0xe1, 0xeb, 0x79, 0x58, 0x81, 0x03, 0x97, 0x80,
        0xda, 0x7c,
    ];
    let verifier_key = SigningKey::from_bytes(&verifier_key_bytes);
    let verifier_pubkey_b64 = BASE64.encode(verifier_key.verifying_key().as_bytes());
    println!("Verifier key: {verifier_pubkey_b64}\n");

    let dev_key_bytes: [u8; 32] = [
        0xff, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];
    let dev_key = SigningKey::from_bytes(&dev_key_bytes);
    let dev_cert = create_dev_cert(&root_key, &dev_key)?;
    let dev_cert_b64 = BASE64.encode(serde_json::to_vec(&dev_cert)?);

    println!("Starting schema service on port {SERVICE_PORT}...");
    let mut service_process = start_service(&root_pubkey_b64, &verifier_pubkey_b64)?;
    std::thread::sleep(Duration::from_secs(3));

    let result = run_proof(&dev_cert_b64, &dev_key, &verifier_key, &verifier_pubkey_b64).await;

    service_process.kill().ok();
    result
}

fn generate_root_key() -> (P256SigningKey, String) {
    use p256::pkcs8::EncodePublicKey;
    let mut rng = rand::thread_rng();
    let signing_key = P256SigningKey::random(&mut rng);
    let verifying_key = signing_key.verifying_key();
    let spki_der = verifying_key.to_public_key_der().unwrap();
    let spki_b64 = BASE64.encode(spki_der.as_bytes());
    (signing_key, spki_b64)
}

fn create_dev_cert(
    root_key: &P256SigningKey,
    dev_key: &SigningKey,
) -> Result<DevCert, Box<dyn std::error::Error>> {
    use p256::pkcs8::EncodePublicKey;
    let dev_pubkey = BASE64.encode(dev_key.verifying_key().as_bytes());
    let root_verifying_key = root_key.verifying_key();
    let root_spki_der = root_verifying_key.to_public_key_der().unwrap();
    let root_key_id = app_identity_crypto::root_key_id(root_spki_der.as_bytes());

    let now = chrono::Utc::now();
    let expires_at = now + chrono::Duration::hours(1);

    let mut cert = DevCert {
        version: DEV_CERT_VERSION,
        purpose: PURPOSE_DEV_CERT.to_string(),
        alg: ALG_ES256.to_string(),
        key_id: root_key_id,
        dev_pubkey,
        user_hash: "test_user_hash".to_string(),
        issued_at: now.to_rfc3339(),
        expires_at: expires_at.to_rfc3339(),
        env: "dev".to_string(),
        authorized_publisher: true,
        sig: String::new(),
    };

    let mut cert_value = serde_json::to_value(&cert)?;
    cert_value.as_object_mut().unwrap().remove("sig");
    let canonical = app_identity_crypto::canonicalize(&cert_value)?;
    let signature: Signature = root_key.sign(&canonical);
    cert.sig = BASE64.encode(signature.to_der().as_bytes());

    Ok(cert)
}

fn start_service(
    root_pubkey: &str,
    verifier_pubkey: &str,
) -> Result<Child, Box<dyn std::error::Error>> {
    Command::new("cargo")
        .args([
            "run",
            "-p",
            "schema_service_server_http",
            "--release",
            "--",
            "--port",
        ])
        .arg(SERVICE_PORT.to_string())
        .current_dir("/workspace/schema_service")
        .env("APP_IDENTITY_ROOT_PUBKEYS", root_pubkey)
        .env("APP_REGISTRY_VERIFIER_PUBKEY", verifier_pubkey)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("Failed to start schema service: {e}").into())
}

async fn run_proof(
    dev_cert_b64: &str,
    dev_key: &SigningKey,
    verifier_key: &SigningKey,
    verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing /v2 client against: {SERVICE_URL}\n");

    let client = ReleaseRegistryClient::new(SERVICE_URL);
    ensure_service_reachable().await?;

    seed_test_data(dev_cert_b64, dev_key).await?;

    round_trip_with_url(&client, dev_cert_b64, verifier_key, verifier_pubkey).await?;
    round_trip_without_url(&client, dev_cert_b64, verifier_key, verifier_pubkey).await?;
    tampered_row_rejected(&client, verifier_pubkey).await?;
    wrong_key_rejected(&client, verifier_pubkey).await?;

    println!("\n=== ✅ All round-trip cases verified ===");
    Ok(())
}

async fn ensure_service_reachable() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Checking service reachability ---");
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    for attempt in 1..=10 {
        match http_client
            .get(&format!("{SERVICE_URL}/v1/health"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                println!("✓ Service reachable\n");
                return Ok(());
            }
            _ if attempt < 10 => std::thread::sleep(Duration::from_millis(500)),
            _ => {
                return Err(
                    format!("Service unreachable at {SERVICE_URL} after 10 attempts").into(),
                )
            }
        }
    }
    unreachable!()
}

async fn seed_test_data(
    dev_cert_b64: &str,
    dev_key: &SigningKey,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Seeding test data ---");

    let app_payload = json!({
        "app_id": "proof-v2-app",
        "app_uuid": "00000000-0000-0000-0000-000000000001",
        "source_repo": "https://github.com/EdgeVector/lastdb-proof",
        "metadata": {
            "display_name": "Proof App",
            "description": "Test app for v2 registry client proof",
            "homepage_url": "https://github.com/EdgeVector/lastdb-proof"
        }
    });

    post_with_dev_cert(
        &app_payload,
        dev_cert_b64,
        dev_key,
        Purpose::AppRegister,
        "/v2/apps",
    )
    .await?;
    println!("✓ Created app");

    let release_1 = json!({
        "release_id": "1111aaaa2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000",
        "app_id": "proof-v2-app",
        "app_uuid": "00000000-0000-0000-0000-000000000001",
        "source_commit": "abc123",
        "artifact_digest": "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "artifact_url": "file:///dev/null",
        "manifest": {
            "lastdb_contract_version": "1.0.0",
            "platform": "darwin-arm64",
        }
    });

    post_with_dev_cert(
        &release_1,
        dev_cert_b64,
        dev_key,
        Purpose::AppReleasePublish,
        "/v2/apps/proof-v2-app/releases",
    )
    .await?;
    println!("✓ Created release 1");

    let release_2 = json!({
        "release_id": "2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000aaaabbbb",
        "app_id": "proof-v2-app",
        "app_uuid": "00000000-0000-0000-0000-000000000001",
        "source_commit": "def789",
        "artifact_digest": "sha256:fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        "artifact_url": "file:///dev/null",
        "manifest": {
            "lastdb_contract_version": "1.0.0",
            "platform": "darwin-arm64",
        }
    });

    post_with_dev_cert(
        &release_2,
        dev_cert_b64,
        dev_key,
        Purpose::AppReleasePublish,
        "/v2/apps/proof-v2-app/releases",
    )
    .await?;
    println!("✓ Created release 2\n");

    Ok(())
}

async fn round_trip_with_url(
    client: &ReleaseRegistryClient,
    _dev_cert_b64: &str,
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

    post_verifier_signed(
        &payload,
        verifier_key,
        &format!("/v2/releases/{release_id}/compat"),
    )
    .await?;
    println!("✓ Posted compat row");

    let rows = client.list_compat_rows(release_id).await?;
    let row = rows
        .iter()
        .find(|r| r.contract_version == contract_version)
        .ok_or("Posted row not found in listing")?;

    let contract_version = &row.contract_version;
    let verification_url = &row.verification_url;
    let payload_hash = &row.payload_hash;
    println!("✓ Read back row:");
    println!("  contract_version: {contract_version}");
    println!("  verification_url: {verification_url:?}");
    println!("  payload_hash: {payload_hash}");

    verify_compat_row(row, verifier_pubkey)?;
    println!("✓ Signature verified\n");

    Ok(())
}

async fn round_trip_without_url(
    client: &ReleaseRegistryClient,
    _dev_cert_b64: &str,
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

    post_verifier_signed(
        &payload,
        verifier_key,
        &format!("/v2/releases/{release_id}/compat"),
    )
    .await?;
    println!("✓ Posted compat row");

    let rows = client.list_compat_rows(release_id).await?;
    let row = rows
        .iter()
        .find(|r| r.contract_version == contract_version)
        .ok_or("Posted row not found in listing")?;

    let contract_version = &row.contract_version;
    let verification_url = &row.verification_url;
    let payload_hash = &row.payload_hash;
    println!("✓ Read back row:");
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

async fn wrong_key_rejected(
    client: &ReleaseRegistryClient,
    _correct_verifier_pubkey: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Row signed by wrong key rejected ---");

    let wrong_key_bytes: [u8; 32] = [
        0x12, 0x1f, 0x05, 0x43, 0x9f, 0xec, 0x65, 0x2f, 0xf8, 0xa1, 0xdb, 0x7b, 0x82, 0x8a, 0x4d,
        0xc4, 0xe4, 0x9e, 0xe3, 0x1f, 0x28, 0x83, 0xf3, 0x18, 0x70, 0x09, 0xc2, 0xca, 0x70, 0x82,
        0xc8, 0x6d,
    ];
    let wrong_key = SigningKey::from_bytes(&wrong_key_bytes);
    let wrong_pubkey_b64 = BASE64.encode(wrong_key.verifying_key().as_bytes());

    let rows = client
        .list_compat_rows("1111aaaa2222bbbb3333cccc4444dddd5555eeee6666ffff7777888899990000")
        .await?;
    let row = rows.first().ok_or("No rows to check")?;

    match verify_compat_row(row, &wrong_pubkey_b64) {
        Ok(()) => Err("Expected wrong-key verification to fail".into()),
        Err(e) => {
            println!("✓ Row rejected when verified against wrong key");
            println!("  error: {e}\n");
            Ok(())
        }
    }
}

async fn post_with_dev_cert(
    payload: &Value,
    dev_cert_b64: &str,
    signing_key: &SigningKey,
    purpose: Purpose,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let sig_envelope = create_signature_envelope(payload, signing_key, purpose)?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let url = format!("{SERVICE_URL}{path}");
    let response = http_client
        .post(&url)
        .header("X-Exemem-Dev-Cert", dev_cert_b64)
        .header("X-Signature", sig_envelope)
        .json(payload)
        .send()
        .await?;

    let status = response.status().as_u16();
    if status != 200 && status != 201 {
        let body = response.text().await?;
        return Err(format!("POST {url} returned {status}: {body}").into());
    }

    Ok(())
}

async fn post_verifier_signed(
    payload: &Value,
    signing_key: &SigningKey,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let sig_envelope = create_signature_envelope(payload, signing_key, Purpose::DevCert)?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let url = format!("{SERVICE_URL}{path}");
    let response = http_client
        .post(&url)
        .header("X-Signature", sig_envelope)
        .json(payload)
        .send()
        .await?;

    let status = response.status().as_u16();
    if status != 200 && status != 201 {
        let body = response.text().await?;
        return Err(format!("POST {url} returned {status}: {body}").into());
    }

    Ok(())
}

fn create_signature_envelope(
    payload: &Value,
    signing_key: &SigningKey,
    purpose: Purpose,
) -> Result<String, Box<dyn std::error::Error>> {
    let payload_hash = compute_payload_hash(payload)?;

    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose,
        alg: ALG_ED25519.to_string(),
        key_id: key_id(&signing_key.verifying_key()),
        issued_at: chrono::Utc::now(),
        expires_at: None,
        env: Env::Dev,
        payload_hash,
        sig: None,
    };

    let signed = sign_envelope(signing_key, unsigned)?;
    Ok(serde_json::to_string(&signed)?)
}
