//! Signing-key secret locators: resolve `lastsecrets://`, `keychain://`,
//! `envelope-file:`, `file:` and `env:` locators into Ed25519 seed material.

use std::fs;
use std::process::Command;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use app_identity_crypto::SigningKey;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use schema_service_core::resolver_pack::TrustedResolverPackKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SecretEnvelope {
    pub(super) version: u32,
    pub(super) provider: SecretEnvelopeProvider,
    pub(super) keychain_service: String,
    pub(super) keychain_account: String,
    pub(super) nonce_b64: String,
    pub(super) ciphertext_b64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) hint: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum SecretEnvelopeProvider {
    MacosKeychainAes256Gcm,
}

pub(super) fn load_signing_key(locator: &str) -> Result<SigningKey, String> {
    let secret = resolve_secret_locator(locator)?;
    parse_ed25519_seed(&secret)
}

pub(super) fn resolve_secret_locator(locator: &str) -> Result<String, String> {
    if let Some(slug) = locator.strip_prefix("lastsecrets://") {
        if slug.is_empty() {
            return Err("empty lastsecrets locator".to_string());
        }
        let output = Command::new("lastsecrets")
            .arg("get")
            .arg(slug)
            .output()
            .map_err(|e| format!("failed to run lastsecrets get: {e}"))?;
        if !output.status.success() {
            return Err("lastsecrets get failed".to_string());
        }
        return String::from_utf8(output.stdout)
            .map_err(|_| "lastsecrets returned non-utf8 secret".to_string());
    }
    if let Some(rest) = locator.strip_prefix("keychain://") {
        let (service, account) = rest
            .split_once('/')
            .ok_or("keychain locator must be keychain://service/account")?;
        return keychain_secret(service, account);
    }
    if let Some(path) = locator.strip_prefix("envelope-file:") {
        let envelope_json =
            fs::read_to_string(path).map_err(|e| format!("read envelope file: {e}"))?;
        reject_raw_key_material_in_brain_payload(&envelope_json)?;
        let envelope: SecretEnvelope =
            serde_json::from_str(&envelope_json).map_err(|e| format!("parse envelope: {e}"))?;
        let root = keychain_secret(&envelope.keychain_service, &envelope.keychain_account)?;
        return decrypt_secret_envelope(&envelope, root.trim().as_bytes());
    }
    if let Some(path) = locator.strip_prefix("file:") {
        return fs::read_to_string(path).map_err(|e| format!("read signing key file: {e}"));
    }
    if let Some(var) = locator.strip_prefix("env:") {
        return std::env::var(var).map_err(|_| format!("environment variable {var} is not set"));
    }
    Err("unsupported signing key locator".to_string())
}

pub(super) fn keychain_secret(service: &str, account: &str) -> Result<String, String> {
    let output = Command::new("/usr/bin/security")
        .arg("find-generic-password")
        .arg("-s")
        .arg(service)
        .arg("-a")
        .arg(account)
        .arg("-w")
        .output()
        .map_err(|e| format!("failed to run security: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "missing decrypt root or keychain secret for service={service} account={account}"
        ));
    }
    String::from_utf8(output.stdout).map_err(|_| "keychain secret was not utf8".to_string())
}

pub(super) fn parse_ed25519_seed(secret: &str) -> Result<SigningKey, String> {
    let trimmed = secret.trim();
    let bytes = if let Some(hex) = trimmed.strip_prefix("hex:") {
        parse_hex_32(hex)?
    } else {
        let b64 = trimmed.strip_prefix("base64:").unwrap_or(trimmed);
        BASE64.decode(b64.as_bytes()).map_err(|_| {
            "signing key must be base64 or hex: encoded 32-byte Ed25519 seed".to_string()
        })?
    };
    let seed: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "signing key must be exactly 32 bytes".to_string())?;
    Ok(SigningKey::from_bytes(&seed))
}

pub(super) fn parse_hex_32(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("hex signing key must be 64 hex characters".to_string());
    }
    let mut out = Vec::with_capacity(32);
    for pair in hex.as_bytes().chunks_exact(2) {
        let s =
            std::str::from_utf8(pair).map_err(|_| "hex signing key was not utf8".to_string())?;
        out.push(u8::from_str_radix(s, 16).map_err(|_| "bad hex signing key".to_string())?);
    }
    Ok(out)
}

pub(super) fn parse_trusted_keys(values: &[String]) -> Result<Vec<TrustedResolverPackKey>, String> {
    if values.is_empty() {
        return Err("at least one --trusted-key is required".to_string());
    }
    values
        .iter()
        .map(|value| {
            let (key_id, public_key_b64) = value
                .split_once('=')
                .ok_or("--trusted-key must be key_id=base64_public_key")?;
            if key_id.is_empty() || public_key_b64.is_empty() {
                return Err("--trusted-key must not contain empty key_id or public key".to_string());
            }
            Ok(TrustedResolverPackKey {
                key_id: key_id.to_string(),
                public_key_b64: public_key_b64.to_string(),
            })
        })
        .collect()
}

pub(super) fn decrypt_secret_envelope(
    envelope: &SecretEnvelope,
    root: &[u8],
) -> Result<String, String> {
    if envelope.version != 1 {
        return Err(format!(
            "unsupported secret envelope version {}",
            envelope.version
        ));
    }
    if envelope.provider != SecretEnvelopeProvider::MacosKeychainAes256Gcm {
        return Err("unsupported secret envelope provider".to_string());
    }
    let root = normalize_aes_root(root)?;
    let nonce_bytes = BASE64
        .decode(envelope.nonce_b64.as_bytes())
        .map_err(|_| "envelope nonce is not base64".to_string())?;
    let nonce: [u8; 12] = nonce_bytes
        .as_slice()
        .try_into()
        .map_err(|_| "envelope nonce must be 12 bytes".to_string())?;
    let ciphertext = BASE64
        .decode(envelope.ciphertext_b64.as_bytes())
        .map_err(|_| "envelope ciphertext is not base64".to_string())?;
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&root));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|_| "secret envelope decrypt failed".to_string())?;
    String::from_utf8(plaintext).map_err(|_| "decrypted secret was not utf8".to_string())
}

pub(super) fn normalize_aes_root(root: &[u8]) -> Result<[u8; 32], String> {
    if let Ok(arr) = <[u8; 32]>::try_from(root) {
        return Ok(arr);
    }
    let trimmed = std::str::from_utf8(root)
        .map_err(|_| "decrypt root must be 32 raw bytes or base64-encoded 32 bytes".to_string())?
        .trim()
        .as_bytes()
        .to_vec();
    if let Ok(decoded) = BASE64.decode(&trimmed) {
        if let Ok(arr) = <[u8; 32]>::try_from(decoded.as_slice()) {
            return Ok(arr);
        }
    }
    <[u8; 32]>::try_from(trimmed.as_slice())
        .map_err(|_| "decrypt root must be 32 raw bytes or base64-encoded 32 bytes".to_string())
}

pub(super) fn reject_raw_key_material_in_brain_payload(payload: &str) -> Result<(), String> {
    let lower = payload.to_ascii_lowercase();
    for forbidden in [
        "-----begin private key-----",
        "-----begin openssh private key-----",
        "-----begin encrypted private key-----",
        "\"private_key\"",
        "\"ed25519_seed\"",
    ] {
        if lower.contains(forbidden) {
            return Err(
                "Brain-backed secret envelope contains raw private-key-shaped material".to_string(),
            );
        }
    }
    Ok(())
}
