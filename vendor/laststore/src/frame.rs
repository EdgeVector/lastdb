//! AEAD frame primitives for disk format v2 run blocks and `CURRENT` manifests.

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use openssl::symm::{Cipher, Crypter, Mode};
use sha2::Sha256;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

mod gcm;
mod stream;
pub(crate) use stream::VerifiedFrameReader;

const MAGIC: [u8; 4] = *b"LSF1";
const VERSION: u8 = 2;
const FLAG_ZSTD: u8 = 1;
const KNOWN_FLAGS: u8 = FLAG_ZSTD;
const ZSTD_LEVEL: i32 = 3;
const UNCOMPRESSED_LEN_SIZE: usize = 8;
pub(crate) const MAX_DECOMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;

const HEADER_SIZE: usize = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 16;
const TAG_SIZE: usize = 16;
const HKDF_INFO: &[u8] = b"laststore/frame/aes-256-gcm/v1";

const MIN_INPUT_BYTES: usize = 256;
const MIN_SAVED_BYTES: usize = 64;
const MIN_SAVED_PERCENT: usize = 10;
const NATIVE_MIN_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    pub chunk_uuid: Uuid,
    pub shard: u16,
    pub start_csn: u64,
    pub counter: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedFrame {
    pub header: FrameHeader,
    pub payload: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct DecodeScratch {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl DecodeScratch {
    fn decompress(&mut self, stored: &[u8], maximum: usize) -> std::io::Result<Vec<u8>> {
        if self.decompressor.is_none() {
            self.decompressor = Some(zstd::bulk::Decompressor::new()?);
        }
        self.decompressor
            .as_mut()
            .expect("decoder initialized")
            .decompress(stored, maximum)
    }
}

pub(crate) struct PreparedFrameKey {
    chunk: Uuid,
    cipher: Aes256Gcm,
    raw_key: Zeroizing<[u8; 32]>,
}

impl PreparedFrameKey {
    pub(crate) fn new(data_key: &[u8; 32], chunk: Uuid) -> Result<Self> {
        make_key(data_key, chunk)
    }

    pub(crate) fn decode(
        &self,
        data_key: &[u8; 32],
        frame: &[u8],
        maximum: usize,
        scratch: Option<&mut DecodeScratch>,
    ) -> Result<DecodedFrame> {
        decode_inner(data_key, frame, Some(maximum), Some(self), scratch)
    }
}

pub(crate) fn min_encoded_len() -> usize {
    HEADER_SIZE + TAG_SIZE
}

pub(crate) fn encode_frame(
    data_key: &[u8; 32],
    header: FrameHeader,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let (mut stored, use_compressed) = stored_payload(payload)?;
    let flags = if use_compressed { FLAG_ZSTD } else { 0 };
    let header_bytes = encode_header(header, stored.len() as u64, flags);
    let cipher = Aes256Gcm::new_from_slice(&chunk_key(data_key, header.chunk_uuid)?)
        .map_err(|_| Error::Config("invalid aead key length".into()))?;
    let nonce_bytes = counter_nonce(header.counter);
    let tag = cipher
        .encrypt_in_place_detached(Nonce::from_slice(&nonce_bytes), &header_bytes, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;

    let mut out = Vec::with_capacity(HEADER_SIZE + stored.len() + TAG_SIZE);
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(&stored);
    out.extend_from_slice(&tag);
    Ok(out)
}

pub(crate) fn decode_frame(data_key: &[u8; 32], frame: &[u8]) -> Result<DecodedFrame> {
    decode_inner(data_key, frame, None, None, None)
}

pub(crate) fn decode_frame_bounded(
    data_key: &[u8; 32],
    frame: &[u8],
    maximum: usize,
) -> Result<DecodedFrame> {
    decode_inner(data_key, frame, Some(maximum), None, None)
}

fn stored_payload(payload: &[u8]) -> Result<(Vec<u8>, bool)> {
    if payload.len() < MIN_INPUT_BYTES || payload.len() > MAX_DECOMPRESSED_FRAME_BYTES {
        return Ok((payload.to_vec(), false));
    }
    let compressed = zstd::bulk::compress(payload, ZSTD_LEVEL)
        .map_err(|e| Error::Config(format!("frame zstd compression failed: {e}")))?;
    let stored_len = compressed.len().saturating_add(UNCOMPRESSED_LEN_SIZE);
    let saved = payload.len().saturating_sub(stored_len);
    if saved < MIN_SAVED_BYTES || saved.saturating_mul(100) < payload.len() * MIN_SAVED_PERCENT {
        return Ok((payload.to_vec(), false));
    }
    let mut stored = Vec::with_capacity(stored_len);
    stored.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    stored.extend_from_slice(&compressed);
    Ok((stored, true))
}

fn make_key(data_key: &[u8; 32], chunk: Uuid) -> Result<PreparedFrameKey> {
    let raw_key = Zeroizing::new(chunk_key(data_key, chunk)?);
    let cipher = Aes256Gcm::new_from_slice(raw_key.as_ref())
        .map_err(|_| Error::Config("invalid aead key length".into()))?;
    Ok(PreparedFrameKey {
        chunk,
        cipher,
        raw_key,
    })
}

fn decode_inner(
    data_key: &[u8; 32],
    frame: &[u8],
    max_payload: Option<usize>,
    prepared: Option<&PreparedFrameKey>,
    scratch: Option<&mut DecodeScratch>,
) -> Result<DecodedFrame> {
    if frame.len() < HEADER_SIZE + TAG_SIZE {
        return Err(Error::Corrupt("frame too short".into()));
    }
    let header_bytes = &frame[..HEADER_SIZE];
    let (header, payload_len, flags) = decode_header_prefix(header_bytes)?;
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
    let generated;
    let key = if let Some(key) = prepared.filter(|key| key.chunk == header.chunk_uuid) {
        key
    } else {
        generated = make_key(data_key, header.chunk_uuid)?;
        &generated
    };
    let mut payload = open_payload(
        key,
        OpenInput {
            nonce: counter_nonce(header.counter),
            header: header_bytes,
            ciphertext: &frame[HEADER_SIZE..tag_offset],
            tag: &frame[tag_offset..],
        },
        prepared.is_some(),
    )?;
    if flags & FLAG_ZSTD != 0 {
        payload = decompress_bounded(
            &payload,
            max_payload.unwrap_or(MAX_DECOMPRESSED_FRAME_BYTES),
            scratch,
        )?;
    } else if max_payload.is_some_and(|limit| payload.len() > limit) {
        return Err(Error::Corrupt("decoded frame exceeds block limit".into()));
    }
    Ok(DecodedFrame { header, payload })
}

struct OpenInput<'a> {
    nonce: [u8; 12],
    header: &'a [u8],
    ciphertext: &'a [u8],
    tag: &'a [u8],
}

fn open_payload(key: &PreparedFrameKey, input: OpenInput<'_>, ordinary: bool) -> Result<Vec<u8>> {
    let native = ordinary && input.ciphertext.len() >= NATIVE_MIN_BYTES;
    let extra = if native {
        Cipher::aes_256_gcm().block_size()
    } else {
        0
    };
    let capacity = input
        .ciphertext
        .len()
        .checked_add(extra)
        .ok_or_else(|| Error::Corrupt("frame output length overflow".into()))?;
    let mut payload = vec![0u8; capacity];
    if !native {
        payload.copy_from_slice(input.ciphertext);
    }
    let opened = if native {
        open_native(key, &input, &mut payload)
    } else {
        key.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&input.nonce),
                input.header,
                &mut payload,
                Tag::from_slice(input.tag),
            )
            .map_err(|_| Error::AeadAuthFail)
    };
    if let Err(error) = opened {
        payload.zeroize();
        return Err(error);
    }
    Ok(payload)
}

fn open_native(key: &PreparedFrameKey, input: &OpenInput<'_>, output: &mut Vec<u8>) -> Result<()> {
    let mut cipher = Crypter::new(
        Cipher::aes_256_gcm(),
        Mode::Decrypt,
        key.raw_key.as_ref(),
        Some(&input.nonce),
    )
    .map_err(|_| Error::AeadAuthFail)?;
    cipher
        .aad_update(input.header)
        .map_err(|_| Error::AeadAuthFail)?;
    cipher.set_tag(input.tag).map_err(|_| Error::AeadAuthFail)?;
    let count = cipher
        .update(input.ciphertext, output)
        .map_err(|_| Error::AeadAuthFail)?;
    let rest = cipher
        .finalize(&mut output[count..])
        .map_err(|_| Error::AeadAuthFail)?;
    if count.checked_add(rest) != Some(input.ciphertext.len()) {
        return Err(Error::Corrupt("frame GCM output length differs".into()));
    }
    output.truncate(count + rest);
    Ok(())
}

fn decompress_bounded(
    stored: &[u8],
    max_payload: usize,
    scratch: Option<&mut DecodeScratch>,
) -> Result<Vec<u8>> {
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
    let decoded = match scratch {
        Some(scratch) => scratch.decompress(&stored[8..], expected),
        None => zstd::bulk::decompress(&stored[8..], expected),
    }
    .map_err(|e| Error::Corrupt(format!("frame zstd decompression failed: {e}")))?;
    if decoded.len() != expected {
        return Err(Error::Corrupt("decompressed frame length mismatch".into()));
    }
    Ok(decoded)
}

pub(super) fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<[u8; 32]> {
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

pub(super) fn decode_header_prefix(bytes: &[u8]) -> Result<(FrameHeader, usize, u8)> {
    if bytes.len() != HEADER_SIZE {
        return Err(Error::Corrupt("bad frame header length".into()));
    }
    if bytes[..4] != MAGIC || bytes[4] != VERSION || bytes[5] & !KNOWN_FLAGS != 0 {
        return Err(Error::AeadAuthFail);
    }
    let flags = bytes[5];
    let shard = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    let start_csn = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let counter = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let payload_len = usize::try_from(u64::from_le_bytes(bytes[24..32].try_into().unwrap()))
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

pub(super) fn counter_nonce(counter: u64) -> [u8; 12] {
    u128::from(counter).to_be_bytes()[4..16]
        .try_into()
        .expect("12-byte counter nonce")
}
