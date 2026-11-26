//! HTTP `nodeSignature` verification. Returns the verified caller node key;
//! grant gating lives in `gate.rs` and never runs AcceptEntry.

use std::time::{SystemTime, UNIX_EPOCH};

use app_identity_crypto::{hex_lower, verify, VerifyingKey};
use axum::http::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use mutation_log::NodeKey;
use relay_protocol::{headers, signed_message, PROTOCOL};
use sha2::{Digest, Sha256};

use crate::error::{ApiError, ApiResult};

const SKEW_SECS: u64 = 60;

/// Verified caller of one relay request.
pub(crate) struct Caller {
    pub(crate) node_key: NodeKey,
    database_id: String,
}

impl Caller {
    /// The signed `X-LastDB-Database-Id` must name the database in the body.
    pub(crate) fn check_database(&self, body_database_id: &str) -> ApiResult<()> {
        if self.database_id == body_database_id {
            Ok(())
        } else {
            Err(ApiError::invalid("database_id"))
        }
    }
}

/// Verify the protocol's request headers ([`relay_protocol::headers`]) over the raw request body
/// for `path`. The signature covers [`signed_message`].
pub(crate) fn verify_request(map: &HeaderMap, path: &str, body: &[u8]) -> ApiResult<Caller> {
    let protocol = header(map, headers::PROTOCOL)?;
    if protocol != PROTOCOL {
        return Err(ApiError::upgrade_required("request.protocol"));
    }
    let node_key = header(map, headers::NODE_KEY)?;
    let database_id = header(map, headers::DATABASE_ID)?;
    let device_id = header(map, headers::DEVICE_ID)?;
    let issued_at = header(map, headers::ISSUED_AT)?;
    let nonce = header(map, headers::NONCE)?;
    let signature = header(map, headers::SIGNATURE)?;
    validate_formats(&node_key, &database_id, &device_id, &issued_at, &nonce)?;
    check_skew(&issued_at)?;
    let body_hash = hex_lower(&Sha256::digest(body));
    let message = signed_message(
        path,
        &node_key,
        &database_id,
        &body_hash,
        &issued_at,
        &nonce,
    );
    let vk = verifying_key_hex(&node_key)?;
    let sig = decode_signature(&signature)?;
    verify(&vk, &sig, message.as_bytes()).map_err(|_| ApiError::unauthorized("signature"))?;
    Ok(Caller {
        node_key: NodeKey::from_bytes(
            decode_hex32(&node_key).ok_or_else(|| ApiError::unauthorized("node_key"))?,
        ),
        database_id,
    })
}

/// The value of header `name`. A missing one is 401 with the name in snake case after
/// `x-lastdb-` (`X-LastDB-Node-Key` is field `node_key`).
fn header(map: &HeaderMap, name: &str) -> ApiResult<String> {
    map.get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| {
            let name = name.to_ascii_lowercase();
            ApiError::unauthorized(name.trim_start_matches("x-lastdb-").replace('-', "_"))
        })
}

fn validate_formats(
    node_key: &str,
    database_id: &str,
    device_id: &str,
    issued_at: &str,
    nonce: &str,
) -> ApiResult<()> {
    if !is_hex64(node_key) {
        return Err(ApiError::unauthorized("node_key"));
    }
    if !is_hex64(database_id) {
        return Err(ApiError::unauthorized("database_id"));
    }
    if !is_hex64(device_id) {
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let earliest = now.saturating_sub(SKEW_SECS);
    let latest = now.saturating_add(SKEW_SECS);
    if issued < earliest || issued > latest {
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
    crate::store::is_hex64(s)
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
        _ => None,
    }
}
