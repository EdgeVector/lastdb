//! Signed device registration.

use std::time::Duration;

use crate::profile::endpoints::{exemem_api_url_for, Environment};

use super::*;

const RESPONSE_LIMIT: usize = 65_536;

/// Parse a 24-word BIP39 recovery phrase into the account's 32-byte Ed25519 seed.
pub fn seed_from_phrase(words: &str) -> Result<[u8; 32], String> {
    let mnemonic =
        bip39::Mnemonic::parse(words.trim()).map_err(|_| "Invalid recovery phrase".to_string())?;
    let entropy = mnemonic.to_entropy();
    let seed: [u8; 32] = entropy.as_slice().try_into().map_err(|_| {
        format!(
            "Recovery phrase must encode 32 bytes, got {}",
            entropy.len()
        )
    })?;
    Ok(seed)
}

/// Credentials minted by a successful signed register.
pub struct Registered {
    pub api_key: String,
    pub user_hash: String,
}

/// An owned register request. Do not derive `Debug`: the JSON body contains
/// the one-use invite supplied on stdin.
struct SignedRegisterRequest {
    url: reqwest::Url,
    body: serde_json::Value,
    expected_user_hash: String,
}

/// Sign `{public_key_hex}:{timestamp}` with the account key for the Exemem CLI
/// register endpoint (`POST {api_url}/api/auth/cli/register`).
fn signed_register_request(
    api_url: &str,
    keypair: &Ed25519KeyPair,
    invite_code: Option<&str>,
    device_id: Option<&str>,
) -> Result<SignedRegisterRequest, String> {
    let public_key_hex = fold_db::hex::hex_lower(keypair.public_key_bytes());
    let timestamp = chrono::Utc::now().timestamp();
    let payload = format!("{public_key_hex}:{timestamp}");
    let signature = keypair.sign(payload.as_bytes());
    let signature_b64 = fold_db::security::KeyUtils::signature_to_base64(&signature);

    let mut body = serde_json::json!({
        "public_key": public_key_hex,
        "timestamp": timestamp,
        "signature": signature_b64,
    });
    if let Some(code) = invite_code {
        body["invite_code"] = serde_json::Value::String(code.to_string());
    }
    if let Some(device) = device_id {
        body["device_id"] = serde_json::Value::String(device.to_string());
    }

    let url = reqwest::Url::parse(&format!("{api_url}/api/auth/cli/register"))
        .map_err(|_| "Invalid Exemem API URL for register".to_string())?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err("Register requires an HTTPS endpoint without embedded credentials".into());
    }
    Ok(SignedRegisterRequest {
        url,
        body,
        expected_user_hash: crate::identity::user_hash_from_pubkey(&keypair.public_key_base64())
            .map_err(|_| "Cannot derive native account identity".to_string())?,
    })
}

fn parse_registered_response(text: &str, expected_user_hash: &str) -> Result<Registered, String> {
    let json: serde_json::Value = serde_json::from_str(text)
        .map_err(|_| "Register response was not valid JSON".to_string())?;

    if !json
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Err("Register request was rejected".to_string());
    }

    let field = |name: &str| -> Result<String, String> {
        json.get(name)
            .and_then(|v| v.as_str())
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("Register response missing nonempty {name}"))
    };
    let api_key = field("api_key")?;
    let registered = Registered {
        api_key,
        user_hash: field("user_hash")?,
    };
    if registered.user_hash != expected_user_hash {
        return Err("Register returned credentials for another signed account".into());
    }
    Ok(registered)
}

/// Register one fresh DEV device without an invite or caller endpoint.
pub async fn register_dev_recovery_device(
    keypair: &Ed25519KeyPair,
    device_id: &str,
) -> Result<Registered, String> {
    register_dev_native_device(keypair, device_id, None).await
}

/// Register one native-backup DEV device through the signed auth API.
pub async fn register_dev_native_device(
    keypair: &Ed25519KeyPair,
    device_id: &str,
    invite_code: Option<&str>,
) -> Result<Registered, String> {
    register_native_device(Environment::Dev, keypair, device_id, invite_code).await
}

/// Explicit compiled-environment registration without a local store or recipient.
pub async fn register_native_device(
    environment: Environment,
    keypair: &Ed25519KeyPair,
    device_id: &str,
    invite_code: Option<&str>,
) -> Result<Registered, String> {
    let parsed = uuid::Uuid::parse_str(device_id).map_err(|_| "invalid DEV recovery device ID")?;
    if parsed.get_version() != Some(uuid::Version::Random) {
        return Err("DEV recovery requires a random device ID".into());
    }
    let request = signed_register_request(
        exemem_api_url_for(environment),
        keypair,
        invite_code,
        Some(device_id),
    )?;
    let client = crate::cloud_backup_client::build_shared_http_client()
        .map_err(|_| "cannot construct DEV recovery register client")?;
    let mut response = client
        .post(request.url.clone())
        .timeout(Duration::from_secs(30))
        .json(&request.body)
        .send()
        .await
        .map_err(|_| "DEV recovery register request failed")?;
    if response.status().is_redirection() || response.url() != &request.url {
        return Err("DEV recovery register redirected".into());
    }
    if !response.status().is_success() {
        return Err(format!(
            "DEV recovery register HTTP status {}",
            response.status().as_u16()
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "cannot read DEV recovery response")?
    {
        if chunk.len() > RESPONSE_LIMIT.saturating_sub(bytes.len()) {
            return Err("DEV recovery response exceeds supported size".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| "DEV recovery response is not UTF-8")?;
    parse_registered_response(text, &request.expected_user_hash)
}
