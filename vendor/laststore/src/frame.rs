//! AEAD frame codec and two-pass verified streaming reader for LastStore frames.

use crate::{Error, Result};
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use hkdf::Hkdf;
use openssl::symm::{Cipher, Crypter, Mode};
use sha2::Sha256;
use std::fs::File;
use std::io::{self, BufRead, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
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

type FileId = (u64, u64, u64, i64, i64);
type Src = Arc<(File, FileId)>;
type ZBuf = Zeroizing<Vec<u8>>;
type GcmState = (u64, usize, usize, bool);
type FrameId = (Uuid, u64);
type FrameAuth<'a> = (Uuid, u64, &'a [u8; HEADER_SIZE], &'a [u8; TAG_SIZE]);
pub(crate) type DecodeCtx<'a> = (Option<(Uuid, &'a Aes256Gcm)>, Option<&'a mut DecodeScratch>);

fn corrupt(msg: &str) -> Error {
    Error::Corrupt(msg.into())
}
fn io_err(e: Error) -> io::Error {
    match e {
        Error::Io(io) => io,
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

fn chunk_key(data_key: &[u8; 32], chunk_uuid: Uuid) -> Result<Zeroizing<[u8; 32]>> {
    let mut out = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(chunk_uuid.as_bytes()), data_key)
        .expand(b"laststore/frame/aes-256-gcm/v1", out.as_mut())
        .map_err(|_| Error::Config("hkdf expand failed".into()))?;
    Ok(out)
}

fn nonce_for(counter: u64) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[4..].copy_from_slice(&counter.to_be_bytes());
    n
}

fn decode_header(b: &[u8; HEADER_SIZE]) -> Result<(Uuid, u64, usize, u8)> {
    if b[..4] != MAGIC || b[4] != VERSION || b[5] & !FLAG_ZSTD != 0 {
        return Err(Error::AeadAuthFail);
    }
    let ctr = u64::from_le_bytes(b[16..24].try_into().unwrap());
    let len = usize::try_from(u64::from_le_bytes(b[24..32].try_into().unwrap()))
        .map_err(|_| corrupt("frame payload length too large"))?;
    let id = Uuid::from_slice(&b[32..48]).map_err(|e| Error::Corrupt(format!("uuid: {e}")))?;
    Ok((id, ctr, len, b[5]))
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
        let c =
            zstd::bulk::compress(payload, 3).map_err(|e| Error::Config(format!("zstd: {e}")))?;
        let saved = payload.len().saturating_sub(c.len() + LEN_BYTES);
        if saved >= 64 && saved * 10 >= payload.len() {
            stored.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            stored.extend_from_slice(&c);
            flag = FLAG_ZSTD;
        }
    }
    if flag == 0 {
        stored.extend_from_slice(payload);
    }
    let (mut hdr, nonce) = ([0u8; HEADER_SIZE], nonce_for(ctr));
    hdr[..4].copy_from_slice(&MAGIC);
    hdr[4] = VERSION;
    hdr[5] = flag;
    hdr[16..24].copy_from_slice(&ctr.to_le_bytes());
    hdr[24..32].copy_from_slice(&(stored.len() as u64).to_le_bytes());
    hdr[32..48].copy_from_slice(id.as_bytes());
    let tag = new_cipher(key, id)?
        .encrypt_in_place_detached(Nonce::from_slice(&nonce), &hdr, &mut stored)
        .map_err(|_| Error::AeadAuthFail)?;
    Ok([&hdr[..], &stored, &tag].concat())
}

type DecodedFrame = Result<(Uuid, u64, Vec<u8>)>;

pub(crate) fn decode_frame(
    k: &[u8; 32],
    f: &[u8],
    max: usize,
    (prep, sc): DecodeCtx<'_>,
) -> DecodedFrame {
    let Some((hdr, rest)) = f.split_first_chunk::<HEADER_SIZE>() else {
        return Err(corrupt("frame too short"));
    };
    let (uuid, counter, len, flags) = decode_header(hdr)?;
    if len.checked_add(TAG_SIZE) != Some(rest.len()) || len > max.saturating_add(LEN_BYTES) {
        return Err(corrupt("frame length or block limit"));
    }
    let tmp;
    let cipher = match prep.filter(|(id, _)| *id == uuid) {
        Some((_, c)) => c,
        None => {
            tmp = new_cipher(k, uuid)?;
            &tmp
        }
    };
    let (mut payload, n, tag) = (
        rest[..len].to_vec(),
        nonce_for(counter),
        Tag::from_slice(&rest[len..]),
    );
    if cipher
        .decrypt_in_place_detached(Nonce::from_slice(&n), hdr, &mut payload, tag)
        .is_err()
    {
        payload.zeroize();
        return Err(Error::AeadAuthFail);
    }
    if flags & FLAG_ZSTD == 0 {
        return (payload.len() <= max)
            .then_some((uuid, counter, payload))
            .ok_or_else(|| corrupt("decoded frame exceeds block limit"));
    }
    let raw_len = payload
        .first_chunk::<LEN_BYTES>()
        .map(|s| u64::from_le_bytes(*s));
    let exp = raw_len
        .and_then(|n| usize::try_from(n).ok())
        .filter(|&n| n <= max.min(MAX_DECOMPRESSED_FRAME_BYTES))
        .ok_or_else(|| corrupt("decompressed frame exceeds limit"))?;
    let (body, zerr) = (&payload[LEN_BYTES..], |e: io::Error| {
        Error::Corrupt(format!("zstd: {e}"))
    });
    let dec = match sc {
        Some(s) => {
            s.0.get_or_insert(zstd::bulk::Decompressor::new().map_err(zerr)?)
                .decompress(body, exp)
        }
        None => zstd::bulk::decompress(body, exp),
    }
    .map_err(zerr)?;
    (dec.len() == exp)
        .then_some((uuid, counter, dec))
        .ok_or_else(|| corrupt("decompressed len mismatch"))
}

fn check_file((file, id): &(File, FileId)) -> Result<()> {
    let c = file.metadata()?;
    (c.is_file() && (c.dev(), c.ino(), c.len(), c.mtime(), c.mtime_nsec()) == *id)
        .then_some(())
        .ok_or_else(|| corrupt("streamed frame file changed"))
}

fn new_crypter(key: &[u8; 32], (id, ctr, hdr, tag): FrameAuth<'_>) -> Result<Crypter> {
    let (k, n) = (chunk_key(key, id)?, nonce_for(ctr));
    let mut c = Crypter::new(Cipher::aes_256_gcm(), Mode::Decrypt, k.as_ref(), Some(&n))
        .map_err(|_| Error::Config("streamed frame cipher init failed".into()))?;
    c.aad_update(hdr)
        .and_then(|()| c.set_tag(tag))
        .map_err(|_| Error::AeadAuthFail)?;
    Ok(c)
}

fn zbuf(n: usize) -> Result<ZBuf> {
    let mut b = Zeroizing::new(Vec::new());
    b.try_reserve_exact(n)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    b.resize(n, 0);
    Ok(b)
}

struct GcmReader(Src, (u64, u64), Crypter, ZBuf, ZBuf, GcmState);

impl GcmReader {
    fn new(src: Src, span: (u64, u64), k: &[u8; 32], a: FrameAuth<'_>) -> Result<(Self, u64)> {
        let lim = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        let in_len = usize::try_from(span.1).unwrap_or(usize::MAX).clamp(1, lim);
        let (in_b, out_b) = (zbuf(in_len)?, zbuf(in_len + 16)?);
        let mem = (in_b.capacity() + out_b.capacity()) as u64;
        let mut r = Self(src, span, new_crypter(k, a)?, in_b, out_b, (0, 0, 0, false));
        while !r.5 .3 {
            r.fill()?;
            r.5 .1 = r.5 .2;
        }
        (r.2, r.5) = (new_crypter(k, a)?, (0, 0, 0, false));
        Ok((r, mem))
    }

    fn fill(&mut self) -> Result<()> {
        let Self(src, (base, total), crypter, ibuf, obuf, st) = self;
        if st.1 < st.2 {
            return Ok(());
        }
        if st.0 == *total {
            if !st.3 {
                if crypter.finalize(obuf).map_err(|_| Error::AeadAuthFail)? != 0 {
                    return Err(corrupt("streamed GCM final length differs"));
                }
                *st = (st.0, 0, 0, true);
            }
            return check_file(src);
        }
        let n = (*total - st.0).min(ibuf.len() as u64) as usize;
        check_file(src)?;
        src.0.read_exact_at(&mut ibuf[..n], *base + st.0)?;
        check_file(src)?;
        let out = crypter
            .update(&ibuf[..n], obuf)
            .map_err(|_| Error::AeadAuthFail)?;
        (out == n)
            .then(|| *st = (st.0 + n as u64, 0, n, st.3))
            .ok_or_else(|| corrupt("GCM len"))
    }
}

impl Read for GcmReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let avail = self.fill_buf()?;
        let n = avail.len().min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for GcmReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.fill().map_err(io_err)?;
        Ok(&self.4[self.5 .1..self.5 .2])
    }
    fn consume(&mut self, count: usize) {
        self.5 .1 += count.min(self.5 .2 - self.5 .1);
    }
}

enum Payload {
    Raw(GcmReader),
    Pending(GcmReader),
    Zstd(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
    Done,
}

pub(crate) struct VerifiedFrameReader(Src, Payload, pub(crate) FrameId, pub(crate) (u64, u64), u64);

impl VerifiedFrameReader {
    pub(crate) fn open(file: &File, offset: u64, encoded: u64, key: &[u8; 32]) -> Result<Self> {
        let (m, end) = (file.metadata()?, offset.checked_add(encoded));
        if !m.is_file() || encoded < MIN_ENCODED_LEN as u64 || end.is_none_or(|e| e > m.len()) {
            return Err(corrupt("streamed frame file extent differs"));
        }
        let cloned = file.try_clone()?;
        let (mut hdr, mut tag) = ([0; HEADER_SIZE], [0; TAG_SIZE]);
        cloned.read_exact_at(&mut hdr, offset)?;
        let (uid, ctr, stored_sz, flags) = decode_header(&hdr)?;
        let (stored, data_at) = (stored_sz as u64, offset + HEADER_SIZE as u64);
        if stored.checked_add(MIN_ENCODED_LEN as u64) != Some(encoded) {
            return Err(corrupt("streamed frame length differs"));
        }
        cloned.read_exact_at(&mut tag, data_at + stored)?;
        let fid = (m.dev(), m.ino(), m.len(), m.mtime(), m.mtime_nsec());
        let src = Arc::new((cloned, fid));
        check_file(&src)?;
        let auth = (uid, ctr, &hdr, &tag);
        let (mut plain, buf_b) = GcmReader::new(Arc::clone(&src), (data_at, stored), key, auth)?;
        let (payload, len, extra) = if flags & FLAG_ZSTD == 0 {
            (Payload::Raw(plain), stored, 0)
        } else {
            let mut len_buf = [0; LEN_BYTES];
            plain.read_exact(&mut len_buf)?;
            let (exp, max_f) = (
                u64::from_le_bytes(len_buf),
                MAX_DECOMPRESSED_FRAME_BYTES as u64,
            );
            if exp > max_f {
                return Err(corrupt("streamed decompressed frame exceeds limit"));
            }
            let sbuf = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
            (Payload::Pending(plain), exp, max_f + 2 * sbuf)
        };
        Ok(Self(src, payload, (uid, ctr), (len, buf_b + extra), 0))
    }

    fn read_payload(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if matches!(self.1, Payload::Pending(_)) {
            if let Payload::Pending(plain) = std::mem::replace(&mut self.1, Payload::Done) {
                let mut dec = zstd::stream::read::Decoder::with_buffer(plain)?;
                dec.window_log_max(MAX_DECOMPRESSED_FRAME_BYTES.ilog2())?;
                self.1 = Payload::Zstd(Box::new(dec));
            }
        }
        match &mut self.1 {
            Payload::Raw(r) => r.read(out),
            Payload::Zstd(r) => r.read(out),
            _ => Ok(0),
        }
    }

    fn verify_end(&mut self) -> Result<()> {
        if !matches!(self.1, Payload::Done) && self.read_payload(&mut [0])? != 0 {
            return Err(corrupt("excess frame plaintext"));
        }
        let mut r = match std::mem::replace(&mut self.1, Payload::Done) {
            Payload::Raw(r) | Payload::Pending(r) => r,
            Payload::Zstd(r) => (*r).finish(),
            Payload::Done => return Ok(()),
        };
        (r.5 .0 == r.1 .1 && r.5 .1 == r.5 .2)
            .then(|| r.fill())
            .ok_or_else(|| corrupt("unconsumed encoded bytes"))?
    }
}

impl Read for VerifiedFrameReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.4 > self.3 .0 {
            return Err(io_err(corrupt("frame reader failed")));
        }
        let res = (|| -> Result<usize> {
            check_file(&self.0)?;
            if self.4 == self.3 .0 {
                return self.verify_end().map(|()| 0);
            }
            let cap = (self.3 .0 - self.4).min(out.len() as u64) as usize;
            let n = self.read_payload(&mut out[..cap])?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
            self.4 += n as u64;
            if self.4 == self.3 .0 {
                self.verify_end()?;
            }
            check_file(&self.0).map(|()| n)
        })();
        if res.is_err() {
            self.4 = u64::MAX;
        }
        res.map_err(io_err)
    }
}
