//! Lowercase hex encoding and SHA-256 hex digests.
//!
//! One definition replaces the per-module `hex`, `hex_lower`, `hex_encode`,
//! `sha256_hex` and `sha256_hex_bytes` helpers. Every output is lowercase with
//! two characters per byte, so digests stay byte-identical to the old copies.

use sha2::{Digest, Sha256};

const DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Lowercase hex of `bytes`, two characters per byte.
pub fn hex_lower(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Decode hex text (either case) into bytes. `None` for an odd length or any
/// character outside `[0-9a-fA-F]`; a sign such as `+f` is not a hex digit.
pub fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks_exact(2)
        .map(|pair| Some((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?))
        .collect()
}

/// Decode exactly `2 * N` hex characters (either case) into `N` bytes.
pub fn hex_decode_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2 {
        return None;
    }
    hex_decode(text)?.try_into().ok()
}

/// Lowercase hex of the SHA-256 digest of `bytes` (64 characters).
pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    hex_lower(Sha256::digest(bytes.as_ref()))
}

/// True when `value` is exactly 64 lowercase hex digits, the shape of
/// [`sha256_hex`] output.
pub fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
