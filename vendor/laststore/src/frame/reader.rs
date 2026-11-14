//! Two-pass verified streaming AEAD reader for bounded LastStore frame reads.

use super::{
    chunk_key, corrupt, decode_header, nonce_for, FLAG_ZSTD, HEADER_SIZE, LEN_BYTES,
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

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: i64,
    mtime_nsec: i64,
}

struct PinnedSource {
    file: File,
    identity: FileIdentity,
}

struct FrameAuth<'a> {
    run_id: Uuid,
    counter: u64,
    header: &'a [u8; HEADER_SIZE],
    tag: &'a [u8; TAG_SIZE],
}

fn io_err(err: Error) -> io::Error {
    match err {
        Error::Io(io) => io,
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

fn check_file(source: &PinnedSource) -> Result<()> {
    let cur = source.file.metadata()?;
    let cur_id = FileIdentity {
        dev: cur.dev(),
        ino: cur.ino(),
        len: cur.len(),
        mtime: cur.mtime(),
        mtime_nsec: cur.mtime_nsec(),
    };
    (cur.is_file() && cur_id == source.identity)
        .then_some(())
        .ok_or_else(|| corrupt("streamed frame file changed"))
}

fn new_crypter(key: &[u8; 32], auth: FrameAuth<'_>) -> Result<Crypter> {
    let derived = chunk_key(key, auth.run_id)?;
    let nonce = nonce_for(auth.counter);
    let mut crypter = Crypter::new(
        Cipher::aes_256_gcm(),
        Mode::Decrypt,
        derived.as_ref(),
        Some(&nonce),
    )
    .map_err(|_| Error::Config("streamed frame cipher init failed".into()))?;
    crypter
        .aad_update(auth.header)
        .and_then(|()| crypter.set_tag(auth.tag))
        .map_err(|_| Error::AeadAuthFail)?;
    Ok(crypter)
}

fn zeroed_buf(len: usize) -> Result<Zeroizing<Vec<u8>>> {
    let mut buf = Zeroizing::new(Vec::new());
    buf.try_reserve_exact(len)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    buf.resize(len, 0);
    Ok(buf)
}

struct GcmReader {
    source: Arc<PinnedSource>,
    base_offset: u64,
    total_bytes: u64,
    crypter: Crypter,
    input_buf: Zeroizing<Vec<u8>>,
    output_buf: Zeroizing<Vec<u8>>,
    bytes_read: u64,
    buf_pos: usize,
    buf_end: usize,
    finalized: bool,
}

impl GcmReader {
    fn new(
        source: Arc<PinnedSource>,
        base_offset: u64,
        total_bytes: u64,
        key: &[u8; 32],
        auth: FrameAuth<'_>,
    ) -> Result<(Self, u64)> {
        let limit = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize;
        let in_len = usize::try_from(total_bytes)
            .unwrap_or(usize::MAX)
            .clamp(1, limit);
        let (input_buf, output_buf) = (zeroed_buf(in_len)?, zeroed_buf(in_len + TAG_SIZE)?);
        let memory = (input_buf.capacity() + output_buf.capacity()) as u64;
        let mut reader = Self {
            source,
            base_offset,
            total_bytes,
            crypter: new_crypter(key, FrameAuth { ..auth })?,
            input_buf,
            output_buf,
            bytes_read: 0,
            buf_pos: 0,
            buf_end: 0,
            finalized: false,
        };
        while !reader.finalized {
            reader.fill()?;
            reader.buf_pos = reader.buf_end;
        }
        reader.crypter = new_crypter(key, auth)?;
        reader.bytes_read = 0;
        reader.buf_pos = 0;
        reader.buf_end = 0;
        reader.finalized = false;
        Ok((reader, memory))
    }

    fn fill(&mut self) -> Result<()> {
        if self.buf_pos < self.buf_end {
            return Ok(());
        }
        if self.bytes_read == self.total_bytes {
            if !self.finalized {
                let tail = self
                    .crypter
                    .finalize(&mut self.output_buf)
                    .map_err(|_| Error::AeadAuthFail)?;
                if tail != 0 {
                    return Err(corrupt("streamed GCM final length differs"));
                }
                self.buf_pos = 0;
                self.buf_end = 0;
                self.finalized = true;
            }
            return check_file(&self.source);
        }
        let chunk = (self.total_bytes - self.bytes_read).min(self.input_buf.len() as u64) as usize;
        check_file(&self.source)?;
        self.source.file.read_exact_at(
            &mut self.input_buf[..chunk],
            self.base_offset + self.bytes_read,
        )?;
        check_file(&self.source)?;
        let written = self
            .crypter
            .update(&self.input_buf[..chunk], &mut self.output_buf)
            .map_err(|_| Error::AeadAuthFail)?;
        if written != chunk {
            return Err(corrupt("GCM len"));
        }
        self.bytes_read += chunk as u64;
        self.buf_pos = 0;
        self.buf_end = chunk;
        Ok(())
    }
}

impl Read for GcmReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let avail = self.fill_buf()?;
        let count = avail.len().min(out.len());
        out[..count].copy_from_slice(&avail[..count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for GcmReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.fill().map_err(io_err)?;
        Ok(&self.output_buf[self.buf_pos..self.buf_end])
    }

    fn consume(&mut self, count: usize) {
        self.buf_pos += count.min(self.buf_end - self.buf_pos);
    }
}

enum Payload {
    Raw(GcmReader),
    PendingZstd(GcmReader),
    Zstd(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
    Done,
}

pub(crate) struct VerifiedFrameReader {
    source: Arc<PinnedSource>,
    payload: Payload,
    pub(crate) run_id: Uuid,
    pub(crate) counter: u64,
    pub(crate) plain_len: u64,
    pub(crate) reserved_memory_bytes: u64,
    bytes_read: u64,
}

impl VerifiedFrameReader {
    pub(crate) fn open(file: &File, offset: u64, encoded: u64, key: &[u8; 32]) -> Result<Self> {
        let (meta, end) = (file.metadata()?, offset.checked_add(encoded));
        if !meta.is_file() || encoded < MIN_ENCODED_LEN as u64 || end.is_none_or(|e| e > meta.len())
        {
            return Err(corrupt("streamed frame file extent differs"));
        }
        let cloned = file.try_clone()?;
        let (mut header, mut tag) = ([0; HEADER_SIZE], [0; TAG_SIZE]);
        cloned.read_exact_at(&mut header, offset)?;
        let (run_id, counter, stored_sz, flags) = decode_header(&header)?;
        let (stored, data_at) = (stored_sz as u64, offset + HEADER_SIZE as u64);
        if stored.checked_add(MIN_ENCODED_LEN as u64) != Some(encoded) {
            return Err(corrupt("streamed frame length differs"));
        }
        cloned.read_exact_at(&mut tag, data_at + stored)?;
        let source = Arc::new(PinnedSource {
            file: cloned,
            identity: FileIdentity {
                dev: meta.dev(),
                ino: meta.ino(),
                len: meta.len(),
                mtime: meta.mtime(),
                mtime_nsec: meta.mtime_nsec(),
            },
        });
        check_file(&source)?;
        let auth = FrameAuth {
            run_id,
            counter,
            header: &header,
            tag: &tag,
        };
        let (mut plain, buf_bytes) =
            GcmReader::new(Arc::clone(&source), data_at, stored, key, auth)?;
        let (payload, plain_len, extra) = if flags & FLAG_ZSTD == 0 {
            (Payload::Raw(plain), stored, 0)
        } else {
            let mut len_buf = [0; LEN_BYTES];
            plain.read_exact(&mut len_buf)?;
            let (expected, max_frame) = (
                u64::from_le_bytes(len_buf),
                MAX_DECOMPRESSED_FRAME_BYTES as u64,
            );
            if expected > max_frame {
                return Err(corrupt("streamed decompressed frame exceeds limit"));
            }
            let stream_buf = lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes;
            (
                Payload::PendingZstd(plain),
                expected,
                max_frame + 2 * stream_buf,
            )
        };
        Ok(Self {
            source,
            payload,
            run_id,
            counter,
            plain_len,
            reserved_memory_bytes: buf_bytes + extra,
            bytes_read: 0,
        })
    }

    fn read_payload(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if matches!(self.payload, Payload::PendingZstd(_)) {
            if let Payload::PendingZstd(plain) = std::mem::replace(&mut self.payload, Payload::Done)
            {
                let mut dec = zstd::stream::read::Decoder::with_buffer(plain)?;
                dec.window_log_max(MAX_DECOMPRESSED_FRAME_BYTES.ilog2())?;
                self.payload = Payload::Zstd(Box::new(dec));
            }
        }
        match &mut self.payload {
            Payload::Raw(r) => r.read(out),
            Payload::Zstd(r) => r.read(out),
            Payload::PendingZstd(_) | Payload::Done => Ok(0),
        }
    }

    fn verify_end(&mut self) -> Result<()> {
        if !matches!(self.payload, Payload::Done) && self.read_payload(&mut [0])? != 0 {
            return Err(corrupt("excess frame plaintext"));
        }
        let mut reader = match std::mem::replace(&mut self.payload, Payload::Done) {
            Payload::Raw(r) | Payload::PendingZstd(r) => r,
            Payload::Zstd(r) => (*r).finish(),
            Payload::Done => return Ok(()),
        };
        if reader.bytes_read != reader.total_bytes || reader.buf_pos != reader.buf_end {
            return Err(corrupt("unconsumed encoded bytes"));
        }
        reader.fill()
    }
}

impl Read for VerifiedFrameReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.bytes_read > self.plain_len {
            return Err(io_err(corrupt("frame reader failed")));
        }
        let res = (|| -> Result<usize> {
            check_file(&self.source)?;
            if self.bytes_read == self.plain_len {
                return self.verify_end().map(|()| 0);
            }
            let cap = (self.plain_len - self.bytes_read).min(out.len() as u64) as usize;
            let count = self.read_payload(&mut out[..cap])?;
            if count == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
            }
            self.bytes_read += count as u64;
            if self.bytes_read == self.plain_len {
                self.verify_end()?;
            }
            check_file(&self.source).map(|()| count)
        })();
        if res.is_err() {
            self.bytes_read = u64::MAX;
        }
        res.map_err(io_err)
    }
}
