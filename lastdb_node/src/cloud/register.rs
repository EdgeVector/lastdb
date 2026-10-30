//! Signed device registration.

use super::*;

#[path = "register/recovery.rs"]
mod recovery;
pub use recovery::{
    register_dev_native_device, register_dev_recovery_device, register_native_device,
};

/// Parse a 24-word BIP39 recovery phrase into the account's 32-byte Ed25519
/// seed — the same derivation as the full node's `identity_from_phrase`.
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
pub(super) struct SignedRegisterRequest {
    pub(super) url: reqwest::Url,
    pub(super) body: serde_json::Value,
    pub(super) expected_user_hash: String,
}

/// Sign `{public_key_hex}:{timestamp}` with the account key for the Exemem CLI
/// register endpoint (`POST {api_url}/api/auth/cli/register`).
pub(super) fn signed_register_request(
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
    // Multi-device: scope key rotation to THIS device (401-window fix);
    // same `.device_id` the sync engine uses.
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
        expected_user_hash: host::user_hash_from_pubkey(&keypair.public_key_base64())
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
    // The response contract requires a token. The sync engine mints its own
    // sessions, so the daemon has no reason to retain this one.
    let api_key = field("api_key")?;
    field("session_token")?;
    let registered = Registered {
        api_key,
        user_hash: field("user_hash")?,
    };
    if registered.user_hash != expected_user_hash {
        return Err("Register returned credentials for another signed account".into());
    }
    Ok(registered)
}
