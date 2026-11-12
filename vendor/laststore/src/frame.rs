//! AEAD frame primitives for LastStore run and checkpoint files.

mod stream;
pub(crate) use stream::VerifiedFrameReader;

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use uuid::Uuid;
use zeroize::Zeroize;

const MAGIC: [u8; 4] = *b"LSF1";
const VERSION: u8 = 2;
pub(self) const FLAG_ZSTD: u8 = 1;
pub(self) const UNCOMPRESSED_LEN_SIZE: usize = 8;
pub(crate) const MAX_DECOMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub(self) const ZSTD_WINDOW_BYTES: usize = MAX_DECOMPRESSED_FRAME_BYTES.next_power_of_two();
pub(self) const HEADER_SIZE: usize = 48;
pub(self) const TAG_SIZE: usize = 16;
pub(crate) const MIN_ENCODED_LEN: usize = HEADER_SIZE + TAG_SIZE;
const HKDF_INFO: &[u8] = b"laststore/frame/aes-256-gcm/v1";

pub(self) fn corrupt(msg: &str) -> Error {
    Error::Corrupt(msg.into())
}

#[derive(Default)]
pub(crate) struct DecodeScratch {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

pub(crate) fn new_cipher(data_key: &[u8; 32], chunk: Uuid) -> Result<Aes256Gcm> {
    let mut k = chunk_key(data_key, chunk)?;
    let res = Aes256Gcm::new_from_slice(&k).map_err(|_| Error::Config("invalid aead key".into()));
    k.zeroize();
    res
}

pub(crate) fn encode_frame(
    key: &[u8; 32],
    uuid: Uuid,
    counter: u64,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let (mut stored, compressed) = stored_payload(payload)?;
    let mut hdr = [0u8; HEADER_SIZE];
    hdr[..4].copy_from_slice(&MAGIC);
    hdr[4] = VERSION;
    hdr[5] = u8::from(compressed) * FLAG_ZSTD;
    hdr[16..24].copy_from_slice(&counter.to_le_bytes());
    hdr[24..32].copy_from_slice(&(stored.len() as u64).to_le_bytes());
    hdr[32..48].copy_from_slice(uuid.as_bytes());
    let nonce = counter_nonce(counter);
    let tag = new_cipher(key, uuid)?
        .encrypt_in_place_detached(Nonce::from_slice(&nonce), &hdr, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;
    Ok([&hdr[..], &stored, &tag].concat())
}

pub(crate) fn decode_frame_bounded(key: &[u8; 32], frame: &[u8], max: usize) -> Result<Vec<u8>> {
    decode_frame(key, frame, max, None, None).map(|(_, _, p)| p)
}

pub(crate) fn decode_frame(
    key: &[u8; 32],
    frame: &[u8],
    max: usize,
    prep: Option<(Uuid, &Aes256Gcm)>,
    scratch: Option<&mut DecodeScratch>,
) -> Result<(Uuid, u64, Vec<u8>)> {
    if frame.len() < MIN_ENCODED_LEN {
        return Err(corrupt("frame too short"));
    }
    let hdr = &frame[..HEADER_SIZE];
    let (uuid, counter, len, flags) = decode_header_prefix(hdr)?;
    if len.checked_add(MIN_ENCODED_LEN) != Some(frame.len()) {
        return Err(corrupt("frame length mismatch"));
    }
    if len > max.saturating_add(UNCOMPRESSED_LEN_SIZE) {
        return Err(corrupt("stored frame exceeds block limit"));
    }
    let tag_at = HEADER_SIZE + len;
    let fresh;
    let cipher = match prep {
        Some((id, c)) if id == uuid => c,
        _ => {
            fresh = new_cipher(key, uuid)?;
            &fresh
        }
    };
    let (nonce, mut payload) = (counter_nonce(counter), frame[HEADER_SIZE..tag_at].to_vec());
    let tag = Tag::from_slice(&frame[tag_at..]);
    if cipher
        .decrypt_in_place_detached(Nonce::from_slice(&nonce), hdr, &mut payload, tag)
        .is_err()
    {
        payload.zeroize();
        return Err(Error::AeadAuthFail);
    }
    if flags & FLAG_ZSTD != 0 {
        payload = decompress_bounded(&payload, max, scratch)?;
    } else if payload.len() > max {
        return Err(corrupt("decoded frame exceeds block limit"));
    }
    Ok((uuid, counter, payload))
}

fn stored_payload(payload: &[u8]) -> Result<(Vec<u8>, bool)> {
    if !(256..=MAX_DECOMPRESSED_FRAME_BYTES).contains(&payload.len()) {
        return Ok((payload.to_vec(), false));
    }
    let comp = zstd::bulk::compress(payload, 3).map_err(|e| Error::Config(format!("zstd: {e}")))?;
    let saved = payload
        .len()
        .saturating_sub(comp.len().saturating_add(UNCOMPRESSED_LEN_SIZE));
    if saved < 64 || saved.saturating_mul(100) < payload.len() * 10 {
        return Ok((payload.to_vec(), false));
    }
    Ok((
        [&(payload.len() as u64).to_le_bytes()[..], &comp].concat(),
        true,
    ))
}

fn decompress_bounded(
    stored: &[u8],
    max: usize,
    scratch: Option<&mut DecodeScratch>,
) -> Result<Vec<u8>> {
    let hdr: [u8; 8] = stored
        .get(..8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| corrupt("missing zstd len"))?;
    let exp = usize::try_from(u64::from_le_bytes(hdr))
        .ok()
        .filter(|&n| n <= max.min(MAX_DECOMPRESSED_FRAME_BYTES))
        .ok_or_else(|| corrupt("decompressed frame exceeds limit"))?;
    let zerr = |e: std::io::Error| Error::Corrupt(format!("zstd: {e}"));
    let dec = match scratch {
        Some(s) => s
            .decompressor
            .get_or_insert(zstd::bulk::Decompressor::new().map_err(zerr)?)
            .decompress(&stored[8..], exp),
        None => zstd::bulk::decompress(&stored[8..], exp),
    }
    .map_err(zerr)?;
    (dec.len() == exp)
        .then_some(dec)
        .ok_or_else(|| corrupt("decompressed len mismatch"))
}

pub(self) fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    Hkdf::<Sha256>::new(Some(chunk_uuid.as_bytes()), data_key)
        .expand(HKDF_INFO, &mut out)
        .map_err(|_| Error::Config("hkdf expand failed".into()))?;
    Ok(out)
}

pub(self) fn decode_header_prefix(bytes: &[u8]) -> Result<(Uuid, u64, usize, u8)> {
    if bytes.len() != HEADER_SIZE {
        return Err(corrupt("bad frame header length"));
    }
    if bytes[..4] != MAGIC || bytes[4] != VERSION || bytes[5] & !FLAG_ZSTD != 0 {
        return Err(Error::AeadAuthFail);
    }
    let counter = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let len = usize::try_from(u64::from_le_bytes(bytes[24..32].try_into().unwrap()))
        .map_err(|_| corrupt("frame payload length too large"))?;
    let id = Uuid::from_slice(&bytes[32..48]).map_err(|e| Error::Corrupt(format!("uuid: {e}")))?;
    Ok((id, counter, len, bytes[5]))
}

pub(self) fn counter_nonce(counter: u64) -> [u8; 12] {
    u128::from(counter).to_be_bytes()[4..16].try_into().unwrap()
}
