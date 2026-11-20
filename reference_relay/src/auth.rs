//! HTTP `nodeSignature` verification. Grant-blind: no grant or AcceptEntry checks.

use std::time::{SystemTime, UNIX_EPOCH};

use app_identity_crypto::{hex_lower, verify, VerifyingKey};
use axum::http::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use sha2::{Digest, Sha256};

use crate::error::{ApiError, ApiResult};

const SKEW_MS: u64 = 60_000;
const PROTOCOL: &str = "1";

/// Verify `X-LastDB-*` headers over the raw request body for `path`.
pub(crate) fn verify_request(headers: &HeaderMap, path: &str, body: &[u8]) -> ApiResult<()> {
    let protocol = header(headers, "x-lastdb-relay-protocol")?;
    if protocol != PROTOCOL {
        return Err(ApiError::upgrade_required("request.protocol"));
    }
    let node_key = header(headers, "x-lastdb-node-key")?;
    let device_id = header(headers, "x-lastdb-device-id")?;
    let issued_at = header(headers, "x-lastdb-issued-at")?;
    let nonce = header(headers, "x-lastdb-nonce")?;
    let signature = header(headers, "x-lastdb-signature")?;
    validate_formats(&node_key, &device_id, &issued_at, &nonce)?;
    check_skew(&issued_at)?;
    let body_hash = hex_lower(&Sha256::digest(body));
    let message = format!(
        "POST\n{path}\n{PROTOCOL}\n{node_key}\n{device_id}\n{issued_at}\n{nonce}\n{body_hash}"
    );
    let vk = verifying_key_hex(&node_key)?;
    let sig = decode_signature(&signature)?;
    verify(&vk, &sig, message.as_bytes()).map_err(|_| ApiError::unauthorized("signature"))?;
    Ok(())
}

fn header(headers: &HeaderMap, name: &str) -> ApiResult<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| ApiError::unauthorized(name.trim_start_matches("x-lastdb-")))
}

fn validate_formats(
    node_key: &str,
    device_id: &str,
    issued_at: &str,
    nonce: &str,
) -> ApiResult<()> {
    if !is_hex64(node_key) {
        return Err(ApiError::unauthorized("node_key"));
    }
    if device_id.is_empty() || device_id.len() > 64 {
        return Err(ApiError::unauthorized("device_id"));
    }
    if issued_at.is_empty()
        || issued_at.len() > 20
        || !issued_at.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ApiError::unauthorized("issued_at"));
    }
    if !is_hex64(nonce) {
        return Err(ApiError::unauthorized("nonce"));
    }
    Ok(())
}

fn check_skew(issued_at: &str) -> ApiResult<()> {
    let issued: u64 = issued_at
        .parse()
        .map_err(|_| ApiError::unauthorized("issued_at"))?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // Accept either unix-ms or unix-seconds (legacy clients).
    let issued_ms = if issued > 10_000_000_000 {
        issued
    } else {
        issued.saturating_mul(1000)
    };
    let earliest = now_ms.saturating_sub(SKEW_MS);
    let latest = now_ms.saturating_add(SKEW_MS);
    if issued_ms < earliest || issued_ms > latest {
        return Err(ApiError::unauthorized("issued_at"));
    }
    Ok(())
}

fn verifying_key_hex(hex: &str) -> ApiResult<VerifyingKey> {
    let bytes = decode_hex32(hex).ok_or_else(|| ApiError::unauthorized("node_key"))?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| ApiError::unauthorized("node_key"))
}

fn decode_signature(sig: &str) -> ApiResult<[u8; 64]> {
    let raw = URL_SAFE_NO_PAD
        .decode(sig.as_bytes())
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(sig.as_bytes()))
        .map_err(|_| ApiError::unauthorized("signature"))?;
    raw.try_into()
        .map_err(|_| ApiError::unauthorized("signature"))
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn decode_hex32(s: &str) -> Option<[u8; 32]> {
    if !is_hex64(s) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = from_hex(chunk[0])?;
        let lo = from_hex(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
