//! AEAD frame primitives. Every block of a disk format v2 run file and every
//! `CURRENT` list is one of these authenticated frames.

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use uuid::Uuid;

mod compression;
mod decode;
mod stream;
pub(crate) use stream::VerifiedFrameReader;

/// Magic prefix for encrypted Last Store frames.
pub const MAGIC: [u8; 4] = *b"LSF1";
/// Frame format version. Readers accept only this version.
pub const VERSION: u8 = 2;
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
    let compression_timing = crate::commit_timing::stage("frame_compression_us");
    let (mut stored, use_compressed) = compression::stored_payload(payload)?;
    drop(compression_timing);
    crate::commit_timing::count("frame_input_bytes", payload.len() as u64);
    crate::commit_timing::count("frame_stored_bytes", stored.len() as u64);
    crate::commit_timing::count("frame_compressed_frames", u64::from(use_compressed));
    let encryption_timing = crate::commit_timing::stage("frame_encryption_us");
    let flags = if use_compressed { FLAG_ZSTD } else { 0 };
    let header_bytes = encode_header(header, stored.len() as u64, flags);
    let cipher = Aes256Gcm::new_from_slice(&chunk_key(data_key, header.chunk_uuid)?)
        .map_err(|_| Error::Config("invalid aead key length".into()))?;
    let nonce_bytes = counter_nonce(header.counter);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let tag = cipher
        .encrypt_in_place_detached(nonce, &header_bytes, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;
    drop(encryption_timing);

    let _assembly_timing = crate::commit_timing::stage("frame_assembly_us");
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

pub use decode::decode_frame;
pub(crate) use decode::{decode_frame_bounded, DecodeScratch, PreparedFrameKey};

fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<[u8; 32]> {
    let hk = Hkdf::<Sha256>::new(Some(chunk_uuid.as_bytes()), data_key);
    let mut out = [0u8; 32];
    hk.expand(HKDF_INFO, &mut out)
        .map_err(|_| Error::Config("hkdf expand failed".into()))?;
    Ok(out)
}

fn encode_header(header: FrameHeader, payload_len: u64, flags: u8) -> [u8; HEADER_SIZE] {
    let mut out = [0u8; HEADER_SIZE];
    out[..4].copy_from_slice(&MAGIC);
    out[4] = VERSION;
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
    if bytes[4] != VERSION {
        return Err(Error::AeadAuthFail);
    }
    let flags = bytes[5];
    if flags & !KNOWN_FLAGS != 0 {
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
