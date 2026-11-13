//! AEAD frame codec for LastStore chunks and run blocks.

mod stream;

pub(crate) use stream::VerifiedFrameReader;

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use std::io;
use uuid::Uuid;
use zeroize::Zeroize;

const MAGIC: [u8; 4] = *b"LSF1";
const VERSION: u8 = 2;
pub(super) const FLAG_ZSTD: u8 = 1;
pub(super) const LEN_BYTES: usize = 8;
pub(crate) const MAX_DECOMPRESSED_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub(super) const HEADER_SIZE: usize = 48;
pub(super) const TAG_SIZE: usize = 16;
pub(crate) const MIN_ENCODED_LEN: usize = HEADER_SIZE + TAG_SIZE;
const HKDF_INFO: &[u8] = b"laststore/frame/aes-256-gcm/v1";

pub(super) fn corrupt(msg: &str) -> Error {
    Error::Corrupt(msg.into())
}

pub(super) fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    Hkdf::<Sha256>::new(Some(chunk_uuid.as_bytes()), data_key)
        .expand(HKDF_INFO, &mut out)
        .map_err(|_| Error::Config("hkdf expand failed".into()))?;
    Ok(out)
}

pub(super) fn counter_nonce(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

pub(super) fn decode_header(b: &[u8; HEADER_SIZE]) -> Result<(Uuid, u64, usize, u8)> {
    if b[..4] != MAGIC || b[4] != VERSION || b[5] & !FLAG_ZSTD != 0 {
        return Err(Error::AeadAuthFail);
    }
    let counter = u64::from_le_bytes(b[16..24].try_into().unwrap());
    let len = usize::try_from(u64::from_le_bytes(b[24..32].try_into().unwrap()))
        .map_err(|_| corrupt("frame payload length too large"))?;
    let id = Uuid::from_slice(&b[32..48]).map_err(|e| Error::Corrupt(format!("uuid: {e}")))?;
    Ok((id, counter, len, b[5]))
}

#[derive(Default)]
pub(crate) struct DecodeScratch(Option<zstd::bulk::Decompressor<'static>>);

pub(crate) fn new_cipher(data_key: &[u8; 32], chunk: Uuid) -> Result<Aes256Gcm> {
    let mut k = chunk_key(data_key, chunk)?;
    let res = Aes256Gcm::new_from_slice(&k).map_err(|_| Error::Config("invalid aead key".into()));
    k.zeroize();
    res
}

pub(crate) fn encode_frame(key: &[u8; 32], id: Uuid, ctr: u64, payload: &[u8]) -> Result<Vec<u8>> {
    let zstd = (256..=MAX_DECOMPRESSED_FRAME_BYTES)
        .contains(&payload.len())
        .then(|| zstd::bulk::compress(payload, 3).map_err(|e| Error::Config(format!("zstd: {e}"))))
        .transpose()?
        .filter(|c| {
            let saved = payload.len().saturating_sub(c.len() + LEN_BYTES);
            saved >= 64 && saved * 10 >= payload.len()
        });
    let (mut stored, flag) = if let Some(c) = zstd {
        (
            [&(payload.len() as u64).to_le_bytes()[..], &c].concat(),
            FLAG_ZSTD,
        )
    } else {
        (payload.to_vec(), 0)
    };
    let mut hdr = [0u8; HEADER_SIZE];
    hdr[..4].copy_from_slice(&MAGIC);
    (hdr[4], hdr[5]) = (VERSION, flag);
    hdr[16..24].copy_from_slice(&ctr.to_le_bytes());
    hdr[24..32].copy_from_slice(&(stored.len() as u64).to_le_bytes());
    hdr[32..48].copy_from_slice(id.as_bytes());
    let nonce = counter_nonce(ctr);
    let tag = new_cipher(key, id)?
        .encrypt_in_place_detached(Nonce::from_slice(&nonce), &hdr, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;
    Ok([&hdr[..], &stored, &tag].concat())
}

pub(crate) fn decode_frame(
    key: &[u8; 32],
    frame: &[u8],
    max: usize,
    prep: Option<(Uuid, &Aes256Gcm)>,
    scratch: Option<&mut DecodeScratch>,
) -> Result<(Uuid, u64, Vec<u8>)> {
    let hdr: &[u8; HEADER_SIZE] = frame
        .get(..HEADER_SIZE)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| corrupt("frame too short"))?;
    let (uuid, counter, len, flags) = decode_header(hdr)?;
    if len.checked_add(MIN_ENCODED_LEN) != Some(frame.len()) {
        return Err(corrupt("frame length mismatch"));
    }
    if len > max.saturating_add(LEN_BYTES) {
        return Err(corrupt("stored frame exceeds block limit"));
    }
    let tmp;
    let cipher = if let Some((_, c)) = prep.filter(|(id, _)| *id == uuid) {
        c
    } else {
        tmp = new_cipher(key, uuid)?;
        &tmp
    };
    let tag_at = HEADER_SIZE + len;
    let (mut payload, nonce) = (frame[HEADER_SIZE..tag_at].to_vec(), counter_nonce(counter));
    let tag = Tag::from_slice(&frame[tag_at..]);
    if cipher
        .decrypt_in_place_detached(Nonce::from_slice(&nonce), hdr, &mut payload, tag)
        .is_err()
    {
        payload.zeroize();
        return Err(Error::AeadAuthFail);
    }
    if flags & FLAG_ZSTD != 0 {
        let exp = payload
            .get(..LEN_BYTES)
            .and_then(|s| usize::try_from(u64::from_le_bytes(s.try_into().ok()?)).ok())
            .filter(|&n| n <= max.min(MAX_DECOMPRESSED_FRAME_BYTES))
            .ok_or_else(|| corrupt("decompressed frame exceeds limit"))?;
        let zerr = |e: io::Error| Error::Corrupt(format!("zstd: {e}"));
        let dec = if let Some(s) = scratch {
            if s.0.is_none() {
                s.0 = Some(zstd::bulk::Decompressor::new().map_err(zerr)?);
            }
            s.0.as_mut().unwrap().decompress(&payload[LEN_BYTES..], exp)
        } else {
            zstd::bulk::decompress(&payload[LEN_BYTES..], exp)
        }
        .map_err(zerr)?;
        return (dec.len() == exp)
            .then_some((uuid, counter, dec))
            .ok_or_else(|| corrupt("decompressed len mismatch"));
    }
    (payload.len() <= max)
        .then_some((uuid, counter, payload))
        .ok_or_else(|| corrupt("decoded frame exceeds block limit"))
}
