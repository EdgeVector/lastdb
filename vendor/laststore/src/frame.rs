//! AEAD frame primitives for Last Store encrypted chunk files.
//!
//! The default keyless store continues to write raw segment bytes. When a store
//! is opened with a data key, group-commit batches are persisted as these
//! authenticated frames.

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use uuid::Uuid;

/// Magic prefix for encrypted Last Store frames.
pub const MAGIC: [u8; 4] = *b"LSF1";
/// Frame format version written by this build.
pub const VERSION: u8 = 2;
const LEGACY_VERSION: u8 = 1;
const FLAG_ZSTD: u8 = 1;
const KNOWN_FLAGS: u8 = FLAG_ZSTD;
const ZSTD_LEVEL: i32 = 3;
const UNCOMPRESSED_LEN_SIZE: usize = 8;
/// Absolute ceiling for one decompressed group-commit frame.
pub const MAX_DECOMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;

const HEADER_SIZE: usize = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 16;
const TAG_SIZE: usize = 16;
const HKDF_INFO: &[u8] = b"laststore/frame/aes-256-gcm/v1";

/// Immutable metadata authenticated with every frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Unique chunk identifier; paired with the counter for nonce uniqueness.
    pub chunk_uuid: Uuid,
    /// Shard number for the chunk.
    pub shard: u16,
    /// Starting commit sequence number for this frame.
    pub start_csn: u64,
    /// Monotonic frame counter within the chunk.
    pub counter: u64,
}

/// A decrypted frame and the authenticated metadata it carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFrame {
    /// Authenticated immutable frame metadata.
    pub header: FrameHeader,
    /// Decrypted frame payload.
    pub payload: Vec<u8>,
}

/// Compression accounting for one encoded frame payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameCompression {
    /// Plaintext bytes presented by the group-commit path.
    pub input_bytes: u64,
    /// Bytes encrypted and stored, excluding the fixed frame header and tag.
    pub stored_bytes: u64,
    /// Whether the frame used zstd compression.
    pub compressed: bool,
}

/// An encoded frame plus its compression accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedFrame {
    /// Complete authenticated frame bytes.
    pub bytes: Vec<u8>,
    /// Compression accounting for the encrypted payload.
    pub compression: FrameCompression,
}

/// Total encoded length of one frame, read from its clear authenticated header.
pub fn encoded_len(frame: &[u8]) -> Result<usize> {
    if frame.len() < HEADER_SIZE {
        return Err(Error::Corrupt("bad frame header length".into()));
    }
    let (_, payload_len, _) = decode_header_prefix(&frame[..HEADER_SIZE])?;
    HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|n| n.checked_add(TAG_SIZE))
        .ok_or_else(|| Error::Corrupt("frame length overflow".into()))
}

/// Cleartext header size for frame scanners.
pub fn header_size() -> usize {
    HEADER_SIZE
}

/// Minimum valid encoded frame length.
pub fn min_encoded_len() -> usize {
    HEADER_SIZE + TAG_SIZE
}

/// Encode one AES-256-GCM frame.
///
/// The header is used as associated data and is included verbatim before the
/// ciphertext. The GCM tag is appended to the ciphertext.
pub fn encode_frame(data_key: &[u8; 32], header: FrameHeader, payload: &[u8]) -> Result<Vec<u8>> {
    Ok(encode_frame_with_stats(data_key, header, payload)?.bytes)
}

/// Encode one frame and report whether compression reduced its payload.
pub fn encode_frame_with_stats(
    data_key: &[u8; 32],
    header: FrameHeader,
    payload: &[u8],
) -> Result<EncodedFrame> {
    let compressed = (payload.len() <= MAX_DECOMPRESSED_FRAME_BYTES)
        .then(|| zstd::bulk::compress(payload, ZSTD_LEVEL))
        .transpose()
        .map_err(|e| Error::Config(format!("frame zstd compression failed: {e}")))?;
    let use_compressed = compressed
        .as_ref()
        .is_some_and(|bytes| bytes.len().saturating_add(UNCOMPRESSED_LEN_SIZE) < payload.len());
    let mut stored = if use_compressed {
        let compressed = compressed.expect("compression selected");
        let mut bytes = Vec::with_capacity(UNCOMPRESSED_LEN_SIZE + compressed.len());
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&compressed);
        bytes
    } else {
        payload.to_vec()
    };
    let flags = if use_compressed { FLAG_ZSTD } else { 0 };
    let header_bytes = encode_header(header, stored.len() as u64, flags);
    let cipher = Aes256Gcm::new_from_slice(&chunk_key(data_key, header.chunk_uuid)?)
        .map_err(|_| Error::Config("invalid aead key length".into()))?;
    let nonce_bytes = counter_nonce(header.counter);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let tag = cipher
        .encrypt_in_place_detached(nonce, &header_bytes, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;

    let mut out = Vec::with_capacity(HEADER_SIZE + stored.len() + TAG_SIZE);
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(&stored);
    out.extend_from_slice(&tag);
    Ok(EncodedFrame {
        bytes: out,
        compression: FrameCompression {
            input_bytes: payload.len() as u64,
            stored_bytes: stored.len() as u64,
            compressed: use_compressed,
        },
    })
}

/// Decode and authenticate one AES-256-GCM frame.
///
/// Any authentication failure returns [`Error::AeadAuthFail`]; plaintext is
/// returned only after the tag verifies.
pub fn decode_frame(data_key: &[u8; 32], frame: &[u8]) -> Result<DecodedFrame> {
    decode_frame_inner(data_key, frame, None)
}

/// A block reader supplies its physical format's tighter allocation limit.
/// Legacy frame callers retain their existing uncompressed-record contract.
pub(crate) fn decode_frame_bounded(
    data_key: &[u8; 32],
    frame: &[u8],
    max_payload: usize,
) -> Result<DecodedFrame> {
    decode_frame_inner(data_key, frame, Some(max_payload))
}

fn decode_frame_inner(
    data_key: &[u8; 32],
    frame: &[u8],
    max_payload: Option<usize>,
) -> Result<DecodedFrame> {
    if frame.len() < HEADER_SIZE + TAG_SIZE {
        return Err(Error::Corrupt("frame too short".into()));
    }
    let header_bytes = &frame[..HEADER_SIZE];
    let (header, payload_len, flags) = decode_header(header_bytes)?;
    let expected_len = HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|n| n.checked_add(TAG_SIZE))
        .ok_or_else(|| Error::Corrupt("frame length overflow".into()))?;
    if frame.len() != expected_len {
        return Err(Error::Corrupt("frame length mismatch".into()));
    }
    if max_payload.is_some_and(|limit| payload_len > limit.saturating_add(UNCOMPRESSED_LEN_SIZE)) {
        return Err(Error::Corrupt("stored frame exceeds block limit".into()));
    }
    let tag_offset = HEADER_SIZE + payload_len;
    let mut payload = frame[HEADER_SIZE..tag_offset].to_vec();
    let tag = Tag::from_slice(&frame[tag_offset..]);
    let cipher = Aes256Gcm::new_from_slice(&chunk_key(data_key, header.chunk_uuid)?)
        .map_err(|_| Error::Config("invalid aead key length".into()))?;
    let nonce_bytes = counter_nonce(header.counter);
    let nonce = Nonce::from_slice(&nonce_bytes);
    cipher
        .decrypt_in_place_detached(nonce, header_bytes, &mut payload, tag)
        .map_err(|_| Error::AeadAuthFail)?;
    if flags & FLAG_ZSTD != 0 {
        payload = decompress_bounded(
            &payload,
            max_payload.unwrap_or(MAX_DECOMPRESSED_FRAME_BYTES),
        )?;
    } else if max_payload.is_some_and(|limit| payload.len() > limit) {
        return Err(Error::Corrupt("decoded frame exceeds block limit".into()));
    }
    Ok(DecodedFrame { header, payload })
}

fn decompress_bounded(stored: &[u8], max_payload: usize) -> Result<Vec<u8>> {
    if stored.len() < UNCOMPRESSED_LEN_SIZE {
        return Err(Error::Corrupt("compressed frame missing length".into()));
    }
    let expected_u64 = u64::from_le_bytes(stored[..8].try_into().unwrap());
    let expected = usize::try_from(expected_u64)
        .map_err(|_| Error::Corrupt("decompressed frame length too large".into()))?;
    let limit = max_payload.min(MAX_DECOMPRESSED_FRAME_BYTES);
    if expected > limit {
        return Err(Error::Corrupt(format!(
            "decompressed frame exceeds {limit} bytes"
        )));
    }
    let decoded = zstd::bulk::decompress(&stored[8..], expected)
        .map_err(|e| Error::Corrupt(format!("frame zstd decompression failed: {e}")))?;
    if decoded.len() != expected {
        return Err(Error::Corrupt("decompressed frame length mismatch".into()));
    }
    Ok(decoded)
}

fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<[u8; 32]> {
    let hk = Hkdf::<Sha256>::new(Some(chunk_uuid.as_bytes()), data_key);
    let mut out = [0u8; 32];
    hk.expand(HKDF_INFO, &mut out)
        .map_err(|_| Error::Config("hkdf expand failed".into()))?;
    Ok(out)
}

fn encode_header(header: FrameHeader, payload_len: u64, flags: u8) -> [u8; HEADER_SIZE] {
    encode_header_version(header, payload_len, VERSION, flags)
}

fn encode_header_version(
    header: FrameHeader,
    payload_len: u64,
    version: u8,
    flags: u8,
) -> [u8; HEADER_SIZE] {
    let mut out = [0u8; HEADER_SIZE];
    out[..4].copy_from_slice(&MAGIC);
    out[4] = version;
    out[5] = flags;
    out[6..8].copy_from_slice(&header.shard.to_le_bytes());
    out[8..16].copy_from_slice(&header.start_csn.to_le_bytes());
    out[16..24].copy_from_slice(&header.counter.to_le_bytes());
    out[24..32].copy_from_slice(&payload_len.to_le_bytes());
    out[32..48].copy_from_slice(header.chunk_uuid.as_bytes());
    out
}

fn decode_header(bytes: &[u8]) -> Result<(FrameHeader, usize, u8)> {
    decode_header_prefix(bytes)
}

fn decode_header_prefix(bytes: &[u8]) -> Result<(FrameHeader, usize, u8)> {
    if bytes.len() != HEADER_SIZE {
        return Err(Error::Corrupt("bad frame header length".into()));
    }
    if bytes[..4] != MAGIC {
        return Err(Error::AeadAuthFail);
    }
    let version = bytes[4];
    if version != LEGACY_VERSION && version != VERSION {
        return Err(Error::AeadAuthFail);
    }
    let flags = bytes[5];
    if (version == LEGACY_VERSION && flags != 0)
        || (version == VERSION && flags & !KNOWN_FLAGS != 0)
    {
        return Err(Error::AeadAuthFail);
    }
    let shard = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    let start_csn = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let counter = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let payload_len_u64 = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let payload_len = usize::try_from(payload_len_u64)
        .map_err(|_| Error::Corrupt("frame payload length too large".into()))?;
    let chunk_uuid =
        Uuid::from_slice(&bytes[32..48]).map_err(|e| Error::Corrupt(format!("chunk uuid: {e}")))?;
    Ok((
        FrameHeader {
            chunk_uuid,
            shard,
            start_csn,
            counter,
        },
        payload_len,
        flags,
    ))
}

fn counter_nonce(counter: u64) -> [u8; 12] {
    let mut bytes = [0u8; 12];
    bytes[4..].copy_from_slice(&counter.to_be_bytes());
    bytes
}
