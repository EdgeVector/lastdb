//! Two-pass bounded streaming frame authentication and decompression.

use super::{
    chunk_key, corrupt, counter_nonce, decode_header_prefix, FLAG_ZSTD, HEADER_SIZE,
    MAX_DECOMPRESSED_FRAME_BYTES, MIN_ENCODED_LEN, TAG_SIZE, UNCOMPRESSED_LEN_SIZE,
    ZSTD_WINDOW_BYTES,
};
use crate::{Error, Result};
use openssl::symm::{Cipher, Crypter, Mode};
use std::fs::{File, Metadata};
use std::io::{self, BufRead, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
use uuid::Uuid;
use zeroize::Zeroizing;

fn io_err(error: Error) -> io::Error {
    match error {
        Error::Io(e) => e,
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

fn bad_data(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

struct Source {
    file: File,
    identity: Metadata,
    chunk_uuid: Uuid,
    counter: u64,
    header: [u8; HEADER_SIZE],
    tag: [u8; TAG_SIZE],
    flags: u8,
    offset: u64,
    stored_bytes: u64,
}

impl Source {
    fn open(file: &File, offset: u64, encoded: u64) -> Result<Self> {
        let identity = file.metadata()?;
        if !identity.is_file()
            || encoded < MIN_ENCODED_LEN as u64
            || offset
                .checked_add(encoded)
                .is_none_or(|e| e > identity.len())
        {
            return Err(corrupt("streamed frame file extent differs"));
        }
        let (file, mut header, mut tag) = (file.try_clone()?, [0; HEADER_SIZE], [0; TAG_SIZE]);
        file.read_exact_at(&mut header, offset)?;
        let (chunk_uuid, counter, stored, flags) = decode_header_prefix(&header)?;
        let stored_bytes = stored as u64;
        if stored_bytes.checked_add(MIN_ENCODED_LEN as u64) != Some(encoded) {
            return Err(corrupt("streamed frame length differs"));
        }
        file.read_exact_at(&mut tag, offset + HEADER_SIZE as u64 + stored_bytes)?;
        let src = Self {
            file,
            identity,
            chunk_uuid,
            counter,
            header,
            tag,
            flags,
            offset,
            stored_bytes,
        };
        src.check().map(|()| src)
    }

    fn check(&self) -> Result<()> {
        let (c, id) = (self.file.metadata()?, &self.identity);
        let same = (c.dev(), c.ino(), c.len(), c.mtime(), c.mtime_nsec())
            == (id.dev(), id.ino(), id.len(), id.mtime(), id.mtime_nsec());
        (c.is_file() && same)
            .then_some(())
            .ok_or_else(|| corrupt("streamed frame file changed"))
    }

    fn new_crypter(&self, data_key: &[u8; 32]) -> Result<Crypter> {
        let key = Zeroizing::new(chunk_key(data_key, self.chunk_uuid)?);
        let nonce = counter_nonce(self.counter);
        let mut c = Crypter::new(
            Cipher::aes_256_gcm(),
            Mode::Decrypt,
            key.as_ref(),
            Some(&nonce),
        )
        .map_err(|_| Error::Config("streamed frame cipher init failed".into()))?;
        c.aad_update(&self.header)
            .and_then(|()| c.set_tag(&self.tag))
            .map_err(|_| Error::AeadAuthFail)?;
        Ok(c)
    }
}

struct GcmReader {
    source: Arc<Source>,
    cipher: Crypter,
    input: Zeroizing<Vec<u8>>,
    plain: Zeroizing<Vec<u8>>,
    position: u64,
    begin: usize,
    end: usize,
    finalized: bool,
}

impl GcmReader {
    fn open_verified(source: Arc<Source>, data_key: &[u8; 32]) -> Result<Self> {
        let limit = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        let in_len = usize::try_from(source.stored_bytes)
            .unwrap_or(usize::MAX)
            .clamp(1, limit);
        let out_len = in_len.checked_add(16).ok_or(Error::ReadBudgetExceeded)?;
        let alloc = |n: usize| -> Result<Zeroizing<Vec<u8>>> {
            let mut b = Zeroizing::new(Vec::new());
            b.try_reserve_exact(n)
                .map_err(|_| Error::ReadBudgetExceeded)?;
            b.resize(n, 0);
            Ok(b)
        };
        let (input, plain) = (alloc(in_len)?, alloc(out_len)?);
        input
            .capacity()
            .checked_add(plain.capacity())
            .ok_or(Error::ReadBudgetExceeded)?;
        let mut r = Self {
            cipher: source.new_crypter(data_key)?,
            source,
            input,
            plain,
            position: 0,
            begin: 0,
            end: 0,
            finalized: false,
        };
        while r.position < r.source.stored_bytes {
            r.fill()?;
            r.begin = r.end;
        }
        r.finish()?;
        r.cipher = r.source.new_crypter(data_key)?;
        (r.position, r.finalized) = (0, false);
        Ok(r)
    }

    fn fill(&mut self) -> Result<()> {
        if self.begin < self.end {
            return Ok(());
        }
        if self.position == self.source.stored_bytes {
            return self.finish();
        }
        let n = (self.source.stored_bytes - self.position).min(self.input.len() as u64) as usize;
        self.source.check()?;
        let pos = self
            .source
            .offset
            .checked_add(HEADER_SIZE as u64)
            .and_then(|s| s.checked_add(self.position))
            .ok_or_else(|| corrupt("streamed frame offset overflow"))?;
        self.source.file.read_exact_at(&mut self.input[..n], pos)?;
        self.source.check()?;
        if self
            .cipher
            .update(&self.input[..n], &mut self.plain)
            .map_err(|_| Error::AeadAuthFail)?
            != n
        {
            return Err(corrupt("streamed GCM output length differs"));
        }
        (self.position, self.begin, self.end) = (self.position + n as u64, 0, n);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !self.finalized {
            if self
                .cipher
                .finalize(&mut self.plain)
                .map_err(|_| Error::AeadAuthFail)?
                != 0
            {
                return Err(corrupt("streamed GCM final length differs"));
            }
            (self.finalized, self.begin, self.end) = (true, 0, 0);
        }
        self.source.check()
    }

    fn verify_consumed(&mut self) -> io::Result<()> {
        if self.position != self.source.stored_bytes || self.begin != self.end {
            return Err(bad_data("unconsumed encoded bytes"));
        }
        self.finish().map_err(io_err)
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
        Ok(&self.plain[self.begin..self.end])
    }

    fn consume(&mut self, count: usize) {
        self.begin += count.min(self.end - self.begin);
    }
}

enum Payload {
    Raw(GcmReader),
    Pending(GcmReader, u32),
    Zstd(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
}

pub(crate) struct VerifiedFrameReader {
    source: Arc<Source>,
    payload: Option<Payload>,
    pub(crate) chunk_uuid: Uuid,
    pub(crate) counter: u64,
    pub(crate) payload_len: u64,
    pub(crate) memory_bytes: u64,
    delivered: u64,
    failed: bool,
}

impl VerifiedFrameReader {
    pub(crate) fn open(file: &File, offset: u64, encoded: u64, key: &[u8; 32]) -> Result<Self> {
        let source = Arc::new(Source::open(file, offset, encoded)?);
        let mut plain = GcmReader::open_verified(Arc::clone(&source), key)?;
        let buf_bytes = (plain.input.capacity() + plain.plain.capacity()) as u64;
        let stream_buf = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
        let (payload, payload_len, extra) = if source.flags & FLAG_ZSTD == 0 {
            (Payload::Raw(plain), source.stored_bytes, 0)
        } else {
            let mut len_buf = [0; UNCOMPRESSED_LEN_SIZE];
            plain.read_exact(&mut len_buf)?;
            let exp = u64::from_le_bytes(len_buf);
            if exp > MAX_DECOMPRESSED_FRAME_BYTES as u64 {
                return Err(corrupt("streamed decompressed frame exceeds limit"));
            }
            (
                Payload::Pending(plain, ZSTD_WINDOW_BYTES.ilog2()),
                exp,
                ZSTD_WINDOW_BYTES as u64 + 2 * stream_buf,
            )
        };
        Ok(Self {
            chunk_uuid: source.chunk_uuid,
            counter: source.counter,
            memory_bytes: buf_bytes + extra,
            source,
            payload: Some(payload),
            payload_len,
            delivered: 0,
            failed: false,
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
            return Err(bad_data("excess frame plaintext"));
        }
        if let Some(p) = self.payload.take() {
            let mut r = match p {
                Payload::Raw(r) | Payload::Pending(r, _) => r,
                Payload::Zstd(r) => (*r).finish(),
            };
            r.verify_consumed()?;
            self.source.check().map_err(io_err)?;
        }
        Ok(())
    }
}

impl Read for VerifiedFrameReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.failed {
            return Err(bad_data("frame reader failed"));
        }
        let res = (|| {
            self.source.check().map_err(io_err)?;
            if self.delivered == self.payload_len {
                return self.verify_end().map(|()| 0);
            }
            let limit = (self.payload_len - self.delivered).min(output.len() as u64) as usize;
            let n = self.read_payload(&mut output[..limit])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "frame plaintext short",
                ));
            }
            self.delivered += n as u64;
            if self.delivered == self.payload_len {
                self.verify_end()?;
            }
            self.source.check().map_err(io_err).map(|()| n)
        })();
        self.failed = res.is_err();
        res
    }
}
