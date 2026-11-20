//! HMAC-signed download URLs served by this same process.

use app_identity_crypto::hex_lower;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::{ApiError, ApiResult};

type HmacSha256 = Hmac<Sha256>;

const TTL_MS: u64 = 300_000;

/// Build a short-lived download URL for `sha256` under `public_base`.
pub(crate) fn make_get_url(
    public_base: &str,
    secret: &[u8],
    sha256: &str,
    now_ms: u64,
) -> (String, u64) {
    let expires_at_ms = now_ms.saturating_add(TTL_MS);
    let sig = sign(secret, sha256, expires_at_ms);
    let url = format!("{public_base}/relay/v1/blob/bytes/{sha256}?e={expires_at_ms}&s={sig}");
    (url, expires_at_ms)
}

/// Build a one-shot upload URL for a staging token.
pub(crate) fn make_put_url(public_base: &str, token: &str, now_ms: u64) -> (String, u64) {
    let expires_at_ms = now_ms.saturating_add(TTL_MS);
    let url = format!("{public_base}/relay/v1/blob/upload/{token}");
    (url, expires_at_ms)
}

/// Reject an expired or forged download signature.
pub(crate) fn check_get(
    secret: &[u8],
    sha256: &str,
    expires_at_ms: u64,
    sig: &str,
    now_ms: u64,
) -> ApiResult<()> {
    if now_ms > expires_at_ms {
        return Err(ApiError::unauthorized("expires_at_ms"));
    }
    let expected = sign(secret, sha256, expires_at_ms);
    if expected != sig {
        return Err(ApiError::unauthorized("signature"));
    }
    Ok(())
}

fn sign(secret: &[u8], sha256: &str, expires_at_ms: u64) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC key length");
    mac.update(sha256.as_bytes());
    mac.update(b"|");
    mac.update(expires_at_ms.to_string().as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

/// Random 32-byte secret as lowercase hex for durable storage.
pub(crate) fn new_secret_hex() -> String {
    let mut bytes = [0u8; 32];
    // Prefer OS randomness; fall back to time mix for CI-only relay.
    if fill_os(&mut bytes).is_err() {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = ((t >> ((i % 16) * 4)) as u8).wrapping_add(i as u8);
        }
    }
    hex_lower(&bytes)
}

fn fill_os(buf: &mut [u8]) -> Result<(), ()> {
    use std::fs::File;
    use std::io::Read;
    let mut f = File::open("/dev/urandom").map_err(|_| ())?;
    f.read_exact(buf).map_err(|_| ())
}

/// Decode a stored hex secret.
pub(crate) fn secret_from_hex(hex: &str) -> ApiResult<Vec<u8>> {
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ApiError::invalid("presign.secret"));
    }
    let mut out = Vec::with_capacity(32);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = from_hex(chunk[0]).ok_or_else(|| ApiError::invalid("presign.secret"))?;
        let lo = from_hex(chunk[1]).ok_or_else(|| ApiError::invalid("presign.secret"))?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
