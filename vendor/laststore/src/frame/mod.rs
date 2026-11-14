//! AEAD frame codec for encrypted LastStore run blocks and manifests.

mod reader;

pub(crate) use reader::VerifiedFrameReader;

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use sha2::Sha256;
use std::io;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const MAGIC: [u8; 4] = *b"LSF1";
const VERSION: u8 = 2;
const FLAG_ZSTD: u8 = 1;
const LEN_BYTES: usize = 8;
const HEADER_SIZE: usize = 48;
const TAG_SIZE: usize = 16;
pub(crate) const MAX_DECOMPRESSED_FRAME_BYTES: usize = 64 << 20;
pub(crate) const MIN_ENCODED_LEN: usize = HEADER_SIZE + TAG_SIZE;

pub(crate) type DecodeCtx<'a> = (Option<(Uuid, &'a Aes256Gcm)>, Option<&'a mut DecodeScratch>);

fn corrupt(msg: &str) -> Error {
    Error::Corrupt(msg.into())
}

fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<Zeroizing<[u8; 32]>> {
    let mut out = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(chunk_uuid.as_bytes()), data_key)
        .expand(b"laststore/frame/aes-256-gcm/v1", out.as_mut())
        .map_err(|_| Error::Config("hkdf expand failed".into()))?;
    Ok(out)
}

fn nonce_for(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

fn decode_header(bytes: &[u8; HEADER_SIZE]) -> Result<(Uuid, u64, usize, u8)> {
    if bytes[..4] != MAGIC || bytes[4] != VERSION || bytes[5] & !FLAG_ZSTD != 0 {
        return Err(Error::AeadAuthFail);
    }
    let counter = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let len = usize::try_from(u64::from_le_bytes(bytes[24..32].try_into().unwrap()))
        .map_err(|_| corrupt("frame payload length too large"))?;
    let id = Uuid::from_slice(&bytes[32..48]).map_err(|e| Error::Corrupt(format!("uuid: {e}")))?;
    Ok((id, counter, len, bytes[5]))
}

#[derive(Default)]
pub(crate) struct DecodeScratch(Option<zstd::bulk::Decompressor<'static>>);

pub(crate) fn new_cipher(data_key: &[u8; 32], chunk: Uuid) -> Result<Aes256Gcm> {
    Aes256Gcm::new_from_slice(&*chunk_key(data_key, chunk)?)
        .map_err(|_| Error::Config("invalid aead key".into()))
}

pub(crate) fn encode_frame(key: &[u8; 32], id: Uuid, ctr: u64, payload: &[u8]) -> Result<Vec<u8>> {
    let (mut stored, mut flag) = (Vec::new(), 0);
    if (256..=MAX_DECOMPRESSED_FRAME_BYTES).contains(&payload.len()) {
        let compressed =
            zstd::bulk::compress(payload, 3).map_err(|e| Error::Config(format!("zstd: {e}")))?;
        let saved = payload.len().saturating_sub(compressed.len() + LEN_BYTES);
        if saved >= 64 && saved * 10 >= payload.len() {
            stored.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            stored.extend_from_slice(&compressed);
            flag = FLAG_ZSTD;
        }
    }
    if flag == 0 {
        stored.extend_from_slice(payload);
    }
    let (mut header, nonce) = ([0u8; HEADER_SIZE], nonce_for(ctr));
    header[..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[5] = flag;
    header[16..24].copy_from_slice(&ctr.to_le_bytes());
    header[24..32].copy_from_slice(&(stored.len() as u64).to_le_bytes());
    header[32..48].copy_from_slice(id.as_bytes());
    let tag = new_cipher(key, id)?
        .encrypt_in_place_detached(Nonce::from_slice(&nonce), &header, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;
    Ok([&header[..], &stored, &tag].concat())
}

pub(crate) fn decode_frame(
    key: &[u8; 32],
    frame: &[u8],
    max_plain_bytes: usize,
    (prepared, scratch): DecodeCtx<'_>,
) -> Result<(Uuid, u64, Vec<u8>)> {
    let Some((header, rest)) = frame.split_first_chunk::<HEADER_SIZE>() else {
        return Err(corrupt("frame too short"));
    };
    let (uuid, counter, len, flags) = decode_header(header)?;
    if len.checked_add(TAG_SIZE) != Some(rest.len())
        || len > max_plain_bytes.saturating_add(LEN_BYTES)
    {
        return Err(corrupt("frame length or block limit"));
    }
    let owned_cipher;
    let cipher = match prepared.filter(|(id, _)| *id == uuid) {
        Some((_, cipher)) => cipher,
        None => {
            owned_cipher = new_cipher(key, uuid)?;
            &owned_cipher
        }
    };
    let (mut payload, nonce, tag) = (
        rest[..len].to_vec(),
        nonce_for(counter),
        Tag::from_slice(&rest[len..]),
    );
    if cipher
        .decrypt_in_place_detached(Nonce::from_slice(&nonce), header, &mut payload, tag)
        .is_err()
    {
        payload.zeroize();
        return Err(Error::AeadAuthFail);
    }
    if flags & FLAG_ZSTD == 0 {
        return (payload.len() <= max_plain_bytes)
            .then_some((uuid, counter, payload))
            .ok_or_else(|| corrupt("decoded frame exceeds block limit"));
    }
    let raw_len = payload
        .first_chunk::<LEN_BYTES>()
        .map(|s| u64::from_le_bytes(*s));
    let expected = raw_len
        .and_then(|n| usize::try_from(n).ok())
        .filter(|&n| n <= max_plain_bytes.min(MAX_DECOMPRESSED_FRAME_BYTES))
        .ok_or_else(|| corrupt("decompressed frame exceeds limit"))?;
    let (body, zerr) = (&payload[LEN_BYTES..], |e: io::Error| {
        Error::Corrupt(format!("zstd: {e}"))
    });
    let decoded = match scratch {
        Some(sc) => {
            sc.0.get_or_insert(zstd::bulk::Decompressor::new().map_err(zerr)?)
                .decompress(body, expected)
        }
        None => zstd::bulk::decompress(body, expected),
    }
    .map_err(zerr)?;
    (decoded.len() == expected)
        .then_some((uuid, counter, decoded))
        .ok_or_else(|| corrupt("decompressed len mismatch"))
}
