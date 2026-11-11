//! Two-pass bounded GCM authentication for existing immutable frame extents.

use super::{
    chunk_key, counter_nonce, decode_header_prefix, FrameHeader, FLAG_ZSTD, HEADER_SIZE,
    MAX_DECOMPRESSED_FRAME_BYTES, TAG_SIZE, UNCOMPRESSED_LEN_SIZE,
};
use crate::{Error, Result};
use openssl::symm::{Cipher, Crypter, Mode};
use std::fs::{File, Metadata};
use std::io::{self, BufRead, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;
use zeroize::Zeroizing;

const ZSTD_WINDOW_BYTES: usize = MAX_DECOMPRESSED_FRAME_BYTES.next_power_of_two();

fn io_invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn as_io(error: Error) -> io::Error {
    match error {
        Error::Io(e) => e,
        e => io::Error::new(io::ErrorKind::InvalidData, e),
    }
}

struct Source {
    file: File,
    identity: Metadata,
    header: FrameHeader,
    header_bytes: [u8; HEADER_SIZE],
    tag: [u8; TAG_SIZE],
    flags: u8,
    offset: u64,
    stored_bytes: u64,
}

impl Source {
    fn open(file: &File, offset: u64, encoded_bytes: u64) -> Result<Self> {
        let identity = file.metadata()?;
        let min_len = (HEADER_SIZE + TAG_SIZE) as u64;
        let end = offset.checked_add(encoded_bytes);
        if !identity.is_file() || encoded_bytes < min_len || end.is_none_or(|e| e > identity.len())
        {
            return Err(Error::Corrupt("streamed frame file extent differs".into()));
        }
        let (file, mut header_bytes) = (file.try_clone()?, [0; HEADER_SIZE]);
        read_at(&file, &mut header_bytes, offset)?;
        let (header, stored, flags) = decode_header_prefix(&header_bytes)?;
        let stored_bytes = stored as u64;
        if stored_bytes.checked_add(min_len) != Some(encoded_bytes) {
            return Err(Error::Corrupt("streamed frame length differs".into()));
        }
        let mut tag = [0; TAG_SIZE];
        read_at(&file, &mut tag, offset + HEADER_SIZE as u64 + stored_bytes)?;
        let source = Self {
            file,
            identity,
            header,
            header_bytes,
            tag,
            flags,
            offset,
            stored_bytes,
        };
        source.check()?;
        Ok(source)
    }

    fn check(&self) -> Result<()> {
        let (c, id) = (self.file.metadata()?, &self.identity);
        if !c.is_file()
            || (c.dev(), c.ino(), c.len(), c.mtime(), c.mtime_nsec())
                != (id.dev(), id.ino(), id.len(), id.mtime(), id.mtime_nsec())
        {
            return Err(Error::Corrupt("streamed frame file changed".into()));
        }
        Ok(())
    }

    fn read_payload(&self, bytes: &mut [u8], position: u64) -> Result<()> {
        self.check()?;
        let pos = self
            .offset
            .checked_add(HEADER_SIZE as u64)
            .and_then(|s| s.checked_add(position))
            .ok_or_else(|| Error::Corrupt("streamed frame offset overflow".into()))?;
        read_at(&self.file, bytes, pos)?;
        self.check()
    }
}

fn read_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        match file.read_at(bytes, offset) {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated frame",
                ))
            }
            Ok(n) => {
                offset = offset
                    .checked_add(n as u64)
                    .ok_or_else(|| io_invalid("offset overflow"))?;
                bytes = &mut bytes[n..];
            }
        }
    }
    Ok(())
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
    fn new(source: Arc<Source>, data_key: &[u8; 32]) -> Result<Self> {
        let key = Zeroizing::new(chunk_key(data_key, source.header.chunk_uuid)?);
        let aes = Cipher::aes_256_gcm();
        let mut cipher = Crypter::new(
            aes,
            Mode::Decrypt,
            key.as_ref(),
            Some(&counter_nonce(source.header.counter)),
        )
        .map_err(|_| Error::Config("streamed frame cipher init failed".into()))?;
        cipher
            .aad_update(&source.header_bytes)
            .map_err(|_| Error::AeadAuthFail)?;
        cipher
            .set_tag(&source.tag)
            .map_err(|_| Error::AeadAuthFail)?;
        let limit = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        let in_len = usize::try_from(source.stored_bytes)
            .unwrap_or(usize::MAX)
            .min(limit)
            .max(1);
        let out_len = in_len
            .checked_add(aes.block_size())
            .ok_or(Error::ReadBudgetExceeded)?;
        let (input, plain) = (buffer(in_len)?, buffer(out_len)?);
        input
            .capacity()
            .checked_add(plain.capacity())
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(Self {
            source,
            cipher,
            input,
            plain,
            position: 0,
            begin: 0,
            end: 0,
            finalized: false,
        })
    }

    fn authenticate(mut self) -> Result<()> {
        while self.position < self.source.stored_bytes {
            self.fill()?;
            self.begin = self.end;
        }
        self.finish()
    }

    fn verify_consumed(&mut self) -> io::Result<()> {
        if self.position != self.source.stored_bytes || self.begin != self.end {
            return Err(io_invalid("unconsumed encoded bytes"));
        }
        self.finish().map_err(as_io)
    }

    fn fill(&mut self) -> Result<()> {
        if self.begin < self.end {
            return Ok(());
        }
        if self.position == self.source.stored_bytes {
            return self.finish();
        }
        let n = (self.source.stored_bytes - self.position).min(self.input.len() as u64) as usize;
        self.source
            .read_payload(&mut self.input[..n], self.position)?;
        let written = self
            .cipher
            .update(&self.input[..n], &mut self.plain)
            .map_err(|_| Error::AeadAuthFail)?;
        if written != n {
            return Err(Error::Corrupt("streamed GCM output length differs".into()));
        }
        (self.position, self.begin, self.end) = (self.position + n as u64, 0, written);
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
                return Err(Error::Corrupt("streamed GCM final length differs".into()));
            }
            (self.finalized, self.begin, self.end) = (true, 0, 0);
        }
        self.source.check()
    }
}

impl Read for GcmReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let available = self.fill_buf()?;
        let n = available.len().min(output.len());
        output[..n].copy_from_slice(&available[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for GcmReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.fill().map_err(as_io)?;
        Ok(&self.plain[self.begin..self.end])
    }

    fn consume(&mut self, count: usize) {
        self.begin += count.min(self.end - self.begin);
    }
}

enum Payload {
    Raw(Box<GcmReader>),
    Pending(Box<GcmReader>, u32),
    Compressed(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
    Finished,
}

impl Read for Payload {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if matches!(self, Self::Pending(..)) {
            let Self::Pending(input, window_log) = std::mem::replace(self, Self::Finished) else {
                unreachable!()
            };
            let mut decoder = zstd::stream::read::Decoder::with_buffer(*input)?;
            decoder.window_log_max(window_log)?;
            *self = Self::Compressed(Box::new(decoder));
        }
        match self {
            Self::Raw(r) => r.read(output),
            Self::Compressed(r) => r.read(output),
            Self::Pending(..) | Self::Finished => Ok(0),
        }
    }
}

impl Payload {
    fn finish(&mut self) -> io::Result<()> {
        let mut payload = std::mem::replace(self, Self::Finished);
        if payload.read(&mut [0])? != 0 {
            return Err(io_invalid("excess frame plaintext"));
        }
        match payload {
            Self::Raw(mut r) => r.verify_consumed(),
            Self::Compressed(r) => (*r).finish().verify_consumed(),
            Self::Pending(..) | Self::Finished => Ok(()),
        }
    }
}

pub(crate) struct VerifiedFrameReader {
    source: Arc<Source>,
    payload: Payload,
    pub(crate) payload_len: u64,
    pub(crate) memory_bytes: u64,
    delivered: u64,
    failed: bool,
}

impl VerifiedFrameReader {
    pub(crate) fn open(file: &File, offset: u64, encoded: u64, key: &[u8; 32]) -> Result<Self> {
        let source = Arc::new(Source::open(file, offset, encoded)?);
        GcmReader::new(Arc::clone(&source), key)?.authenticate()?;
        let mut plain = GcmReader::new(Arc::clone(&source), key)?;
        let buf_bytes = (plain.input.capacity() + plain.plain.capacity()) as u64;
        let stream_buf = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
        let (payload, payload_len, extra_mem) = if source.flags & FLAG_ZSTD == 0 {
            (Payload::Raw(Box::new(plain)), source.stored_bytes, 0)
        } else {
            let mut length = [0; UNCOMPRESSED_LEN_SIZE];
            plain.read_exact(&mut length)?;
            let expected = u64::from_le_bytes(length);
            if expected > MAX_DECOMPRESSED_FRAME_BYTES as u64 {
                return Err(Error::Corrupt(
                    "streamed decompressed frame exceeds limit".into(),
                ));
            }
            let extra = ZSTD_WINDOW_BYTES as u64 + 2 * stream_buf;
            (
                Payload::Pending(Box::new(plain), ZSTD_WINDOW_BYTES.ilog2()),
                expected,
                extra,
            )
        };
        Ok(Self {
            source,
            payload,
            payload_len,
            memory_bytes: buf_bytes + extra_mem,
            delivered: 0,
            failed: false,
        })
    }

    pub(crate) fn header(&self) -> FrameHeader {
        self.source.header
    }

    fn verify_end(&mut self) -> io::Result<()> {
        if !matches!(self.payload, Payload::Finished) {
            self.payload.finish()?;
            self.source.check().map_err(as_io)?;
        }
        Ok(())
    }

    fn read_inner(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.source.check().map_err(as_io)?;
        if self.delivered == self.payload_len {
            return self.verify_end().map(|()| 0);
        }
        let limit = (self.payload_len - self.delivered).min(output.len() as u64) as usize;
        let n = self.payload.read(&mut output[..limit])?;
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
        self.source.check().map_err(as_io)?;
        Ok(n)
    }
}

impl Read for VerifiedFrameReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.failed {
            return Err(io_invalid("frame reader failed"));
        }
        let result = self.read_inner(output);
        self.failed = result.is_err();
        result
    }
}

fn buffer(size: usize) -> Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    bytes.resize(size, 0);
    Ok(bytes)
}
