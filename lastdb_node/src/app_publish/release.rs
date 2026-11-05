//! Dev cert minting and envelope signing for registry writes.

use super::*;

/// Mint a short-TTL DevCert from the exemem auth service.
pub(super) async fn mint_dev_cert(
    exemem_api_url: &str,
    api_key: &str,
    dev_key: &SigningKey,
) -> Result<DevCert, String> {
    let dev_pubkey = BASE64.encode(dev_key.verifying_key().to_bytes());
    let url = format!("{}/v1/dev-cert", exemem_api_url.trim_end_matches('/'));
    let client = http_client()?;
    // trace-egress: propagate (exemem auth_service /v1/dev-cert mint)
    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .json(&json!({ "dev_pubkey": dev_pubkey }))
        .send()
        .await
        .map_err(|e| format!("POST {url} failed: {e}"))?;
    let status = response.status().as_u16();
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("POST {url}: invalid JSON response: {e}"))?;
    if status != 200 {
        return Err(format!(
            "dev-cert mint rejected ({status}): {} — is this developer enrolled \
             (paid plan or developer_access grant)?",
            serde_json::to_string(&payload).unwrap_or_default()
        ));
    }
    let cert = payload
        .get("cert")
        .cloned()
        .ok_or_else(|| "dev-cert response missing 'cert'".to_string())?;
    serde_json::from_value(cert).map_err(|e| format!("dev-cert response does not parse: {e}"))
}

/// Base64 `SignatureEnvelope` for an `X-Signature` header (the production
/// counterpart of the test-utils helper of the same name).
pub(super) fn sign_envelope_b64(
    dev_key: &SigningKey,
    purpose: AppIdentityPurpose,
    env: Env,
    payload: &Value,
) -> Result<String, String> {
    let unsigned = SignatureEnvelope {
        version: ENVELOPE_VERSION,
        purpose,
        alg: ALG_ED25519.to_string(),
        key_id: key_id(&dev_key.verifying_key()),
        issued_at: chrono::Utc::now(),
        expires_at: None,
        env,
        payload_hash: compute_payload_hash(payload)
            .map_err(|e| format!("payload hash failed: {e}"))?,
        sig: None,
    };
    let signed =
        sign_envelope(dev_key, unsigned).map_err(|e| format!("envelope signing failed: {e}"))?;
    Ok(BASE64.encode(
        serde_json::to_vec(&signed).map_err(|e| format!("failed to encode envelope: {e}"))?,
    ))
}
