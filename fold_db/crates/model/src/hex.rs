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

/// Decode exactly `2 * N` hex characters (either case) into `N` bytes.
pub fn hex_decode_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    let bytes = text.as_bytes();
    if bytes.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (slot, pair) in out.iter_mut().zip(bytes.chunks_exact(2)) {
        *slot = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(out)
}

/// Lowercase hex of the SHA-256 digest of `bytes` (64 characters).
pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    hex_lower(Sha256::digest(bytes.as_ref()))
}
