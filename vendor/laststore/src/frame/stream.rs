//! Two-pass verified AEAD streaming reader for backup-bounded frames.

use super::{
    chunk_key, corrupt, counter_nonce, decode_header, FLAG_ZSTD, HEADER_SIZE, LEN_BYTES,
    MAX_DECOMPRESSED_FRAME_BYTES, MIN_ENCODED_LEN, TAG_SIZE,
};
use crate::{Error, Result};
use openssl::symm::{Cipher, Crypter, Mode};
use std::fs::File;
use std::io::{self, BufRead, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
use uuid::Uuid;
use zeroize::Zeroizing;

const ZSTD_WINDOW_BYTES: usize = MAX_DECOMPRESSED_FRAME_BYTES.next_power_of_two();
type FileId = (u64, u64, u64, i64, i64);
type FrameAuth<'a> = (Uuid, u64, &'a [u8; HEADER_SIZE], &'a [u8; TAG_SIZE]);

fn io_err(e: Error) -> io::Error {
    if let Error::Io(io) = e {
        io
    } else {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}

fn inv(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn check_file((file, id): &(File, FileId)) -> Result<()> {
    let c = file.metadata()?;
    (c.is_file() && (c.dev(), c.ino(), c.len(), c.mtime(), c.mtime_nsec()) == *id)
        .then_some(())
        .ok_or_else(|| corrupt("streamed frame file changed"))
}

fn new_crypter(key: &[u8; 32], (id, ctr, hdr, tag): FrameAuth<'_>) -> Result<Crypter> {
    let (k, n) = (Zeroizing::new(chunk_key(key, id)?), counter_nonce(ctr));
    let mut c = Crypter::new(Cipher::aes_256_gcm(), Mode::Decrypt, k.as_ref(), Some(&n))
        .map_err(|_| Error::Config("streamed frame cipher init failed".into()))?;
    c.aad_update(hdr)
        .and_then(|()| c.set_tag(tag))
        .map_err(|_| Error::AeadAuthFail)?;
    Ok(c)
}

fn zero_buf(n: usize) -> Result<Zeroizing<Vec<u8>>> {
    let mut b = Zeroizing::new(Vec::new());
    b.try_reserve_exact(n)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    b.resize(n, 0);
    Ok(b)
}

struct GcmReader {
    src: Arc<(File, FileId)>,
    span: (u64, u64),
    cipher: Crypter,
    bufs: (Zeroizing<Vec<u8>>, Zeroizing<Vec<u8>>),
    cur: (u64, usize, usize, bool),
}

impl GcmReader {
    fn new(
        src: Arc<(File, FileId)>,
        span: (u64, u64),
        key: &[u8; 32],
        auth: FrameAuth<'_>,
    ) -> Result<(Self, u64)> {
        let lim = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        let in_len = usize::try_from(span.1).unwrap_or(usize::MAX).clamp(1, lim);
        let input = zero_buf(in_len)?;
        let plain = zero_buf(in_len.checked_add(16).ok_or(Error::ReadBudgetExceeded)?)?;
        let mem = input
            .capacity()
            .checked_add(plain.capacity())
            .ok_or(Error::ReadBudgetExceeded)? as u64;
        let mut r = Self {
            src,
            span,
            cipher: new_crypter(key, auth)?,
            bufs: (input, plain),
            cur: (0, 0, 0, false),
        };
        while r.cur.0 < span.1 {
            r.fill()?;
            r.cur.1 = r.cur.2;
        }
        r.finish()?;
        (r.cipher, r.cur) = (new_crypter(key, auth)?, (0, 0, 0, false));
        Ok((r, mem))
    }

    fn fill(&mut self) -> Result<()> {
        if self.cur.1 < self.cur.2 {
            return Ok(());
        }
        if self.cur.0 == self.span.1 {
            return self.finish();
        }
        let n = (self.span.1 - self.cur.0).min(self.bufs.0.len() as u64) as usize;
        check_file(&self.src)?;
        self.src
            .0
            .read_exact_at(&mut self.bufs.0[..n], self.span.0 + self.cur.0)?;
        check_file(&self.src)?;
        if self
            .cipher
            .update(&self.bufs.0[..n], &mut self.bufs.1)
            .map_err(|_| Error::AeadAuthFail)?
            != n
        {
            return Err(corrupt("streamed GCM output length differs"));
        }
        self.cur = (self.cur.0 + n as u64, 0, n, self.cur.3);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !self.cur.3 {
            if self
                .cipher
                .finalize(&mut self.bufs.1)
                .map_err(|_| Error::AeadAuthFail)?
                != 0
            {
                return Err(corrupt("streamed GCM final length differs"));
            }
            self.cur = (self.cur.0, 0, 0, true);
        }
        check_file(&self.src)
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
        Ok(&self.bufs.1[self.cur.1..self.cur.2])
    }
    fn consume(&mut self, count: usize) {
        self.cur.1 += count.min(self.cur.2 - self.cur.1);
    }
}

enum Payload {
    Raw(GcmReader),
    Pending(GcmReader, u32),
    Zstd(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
}

pub(crate) struct VerifiedFrameReader {
    src: Arc<(File, FileId)>,
    payload: Option<Payload>,
    pub(crate) chunk_uuid: Uuid,
    pub(crate) counter: u64,
    pub(crate) payload_len: u64,
    pub(crate) memory_bytes: u64,
    delivered: u64,
}

impl VerifiedFrameReader {
    pub(crate) fn open(file: &File, offset: u64, encoded: u64, key: &[u8; 32]) -> Result<Self> {
        let m = file.metadata()?;
        if !m.is_file()
            || encoded < MIN_ENCODED_LEN as u64
            || offset.checked_add(encoded).is_none_or(|e| e > m.len())
        {
            return Err(corrupt("streamed frame file extent differs"));
        }
        let (cloned, mut hdr, mut tag) = (file.try_clone()?, [0; HEADER_SIZE], [0; TAG_SIZE]);
        cloned.read_exact_at(&mut hdr, offset)?;
        let (chunk_uuid, counter, stored_sz, flags) = decode_header(&hdr)?;
        let (stored, data_at) = (stored_sz as u64, offset + HEADER_SIZE as u64);
        if stored.checked_add(MIN_ENCODED_LEN as u64) != Some(encoded) {
            return Err(corrupt("streamed frame length differs"));
        }
        cloned.read_exact_at(&mut tag, data_at + stored)?;
        let src = Arc::new((
            cloned,
            (m.dev(), m.ino(), m.len(), m.mtime(), m.mtime_nsec()),
        ));
        check_file(&src)?;
        let auth = (chunk_uuid, counter, &hdr, &tag);
        let (mut plain, buf_bytes) =
            GcmReader::new(Arc::clone(&src), (data_at, stored), key, auth)?;
        let sbuf = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
        let (payload, payload_len, extra) = if flags & FLAG_ZSTD == 0 {
            (Payload::Raw(plain), stored, 0)
        } else {
            let mut len_buf = [0; LEN_BYTES];
            plain.read_exact(&mut len_buf)?;
            let exp = u64::from_le_bytes(len_buf);
            if exp > MAX_DECOMPRESSED_FRAME_BYTES as u64 {
                return Err(corrupt("streamed decompressed frame exceeds limit"));
            }
            (
                Payload::Pending(plain, ZSTD_WINDOW_BYTES.ilog2()),
                exp,
                ZSTD_WINDOW_BYTES as u64 + 2 * sbuf,
            )
        };
        Ok(Self {
            src,
            payload: Some(payload),
            chunk_uuid,
            counter,
            payload_len,
            memory_bytes: buf_bytes + extra,
            delivered: 0,
        })
    }

    fn read_payload(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if matches!(self.payload, Some(Payload::Pending(..))) {
            let Some(Payload::Pending(plain, wlog)) = self.payload.take() else {
                unreachable!()
            };
            let mut dec = zstd::stream::read::Decoder::with_buffer(plain)?;
            dec.window_log_max(wlog)?;
            self.payload = Some(Payload::Zstd(Box::new(dec)));
        }
        match &mut self.payload {
            Some(Payload::Raw(r)) => r.read(out),
            Some(Payload::Zstd(r)) => r.read(out),
            _ => Ok(0),
        }
    }

    fn verify_end(&mut self) -> io::Result<()> {
        if self.payload.is_some() && self.read_payload(&mut [0])? != 0 {
            return Err(inv("excess frame plaintext"));
        }
        if let Some(p) = self.payload.take() {
            let mut r = match p {
                Payload::Raw(r) | Payload::Pending(r, _) => r,
                Payload::Zstd(r) => (*r).finish(),
            };
            if r.cur.0 != r.span.1 || r.cur.1 != r.cur.2 {
                return Err(inv("unconsumed encoded bytes"));
            }
            r.finish()
                .and_then(|()| check_file(&self.src))
                .map_err(io_err)?;
        }
        Ok(())
    }
}

impl Read for VerifiedFrameReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.delivered > self.payload_len {
            return Err(inv("frame reader failed"));
        }
        let res = (|| {
            check_file(&self.src).map_err(io_err)?;
            if self.delivered == self.payload_len {
                return self.verify_end().map(|()| 0);
            }
            let limit = (self.payload_len - self.delivered).min(output.len() as u64) as usize;
            let n = self.read_payload(&mut output[..limit])?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            self.delivered += n as u64;
            if self.delivered == self.payload_len {
                self.verify_end()?;
            }
            check_file(&self.src).map_err(io_err).map(|()| n)
        })();
        if res.is_err() {
            self.delivered = u64::MAX;
        }
        res
    }
}
