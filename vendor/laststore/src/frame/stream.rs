//! Two-pass bounded GCM authentication for existing immutable frame extents.

use super::gcm::GcmReader;
use super::{
    decode_header_prefix, FrameHeader, FLAG_ZSTD, HEADER_SIZE, MAX_DECOMPRESSED_FRAME_BYTES,
    TAG_SIZE, UNCOMPRESSED_LEN_SIZE,
};
use crate::{Error, Result};
use std::fs::{File, Metadata};
use std::io::{self, Read};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;

pub(super) struct Source {
    file: File,
    identity: Metadata,
    pub header: FrameHeader,
    pub header_bytes: [u8; HEADER_SIZE],
    pub tag: [u8; TAG_SIZE],
    pub flags: u8,
    pub offset: u64,
    pub stored_bytes: u64,
}

impl Source {
    pub fn open(file: &File, offset: u64, encoded_bytes: u64) -> Result<Self> {
        let identity = file.metadata()?;
        if !identity.is_file()
            || offset
                .checked_add(encoded_bytes)
                .is_none_or(|end| end > identity.len())
        {
            return Err(Error::Corrupt("streamed frame file extent differs".into()));
        }
        if encoded_bytes < (HEADER_SIZE + TAG_SIZE) as u64 {
            return Err(Error::Corrupt("streamed frame is too short".into()));
        }
        let file = file.try_clone()?;
        let mut header_bytes = [0; HEADER_SIZE];
        read_at(&file, &mut header_bytes, offset)?;
        let (header, stored_bytes, flags) = decode_header_prefix(&header_bytes)?;
        let stored_bytes = stored_bytes as u64;
        if stored_bytes.checked_add((HEADER_SIZE + TAG_SIZE) as u64) != Some(encoded_bytes) {
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

    pub fn check(&self) -> Result<()> {
        let current = self.file.metadata()?;
        if !current.is_file()
            || current.dev() != self.identity.dev()
            || current.ino() != self.identity.ino()
            || current.len() != self.identity.len()
            || current.mtime() != self.identity.mtime()
            || current.mtime_nsec() != self.identity.mtime_nsec()
        {
            return Err(Error::Corrupt("streamed frame file changed".into()));
        }
        Ok(())
    }

    pub fn read_payload(&self, bytes: &mut [u8], position: u64) -> Result<()> {
        self.check()?;
        let offset = self
            .offset
            .checked_add(HEADER_SIZE as u64)
            .and_then(|start| start.checked_add(position))
            .ok_or_else(|| Error::Corrupt("streamed frame offset overflow".into()))?;
        read_at(&self.file, bytes, offset)?;
        self.check()
    }
}

fn read_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let count = match file.read_at(bytes, offset) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "streamed frame is truncated",
            ));
        }
        offset = offset.checked_add(count as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "streamed frame offset overflow")
        })?;
        bytes = &mut bytes[count..];
    }
    Ok(())
}

enum Payload {
    Raw(Box<GcmReader>),
    CompressedPending(Option<Box<GcmReader>>, u32),
    Compressed(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
    Finished,
}

impl Read for Payload {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if let Self::CompressedPending(input, window_log) = self {
            let input = input.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "streamed frame decoder failed")
            })?;
            let mut decoder = zstd::stream::read::Decoder::with_buffer(*input)?;
            decoder.window_log_max(*window_log)?;
            *self = Self::Compressed(Box::new(decoder));
        }
        match self {
            Self::Raw(reader) => reader.read(output),
            Self::Compressed(reader) => reader.read(output),
            Self::CompressedPending(_, _) => unreachable!(),
            Self::Finished => Ok(0),
        }
    }
}

impl Payload {
    fn finish(&mut self) -> io::Result<()> {
        let mut payload = std::mem::replace(self, Self::Finished);
        if payload.read(&mut [0])? != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame has excess plaintext",
            ));
        }
        match payload {
            Self::Raw(mut reader) => reader.verify_consumed(),
            Self::Compressed(reader) => (*reader).finish().verify_consumed(),
            Self::CompressedPending(_, _) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame decoder is unavailable",
            )),
            Self::Finished => Ok(()),
        }
    }
}

pub(crate) struct VerifiedFrameReader {
    source: Arc<Source>,
    payload: Payload,
    payload_len: u64,
    memory_bytes: u64,
    delivered: u64,
    finished: bool,
    failed: bool,
}

impl VerifiedFrameReader {
    pub(crate) fn open(
        file: &File,
        offset: u64,
        encoded_bytes: u64,
        data_key: &[u8; 32],
    ) -> Result<Self> {
        let window = MAX_DECOMPRESSED_FRAME_BYTES
            .max(1024)
            .checked_next_power_of_two()
            .ok_or(Error::ReadBudgetExceeded)?;
        let source = Arc::new(Source::open(file, offset, encoded_bytes)?);
        GcmReader::new(Arc::clone(&source), data_key)?.authenticate()?;
        let mut plain = GcmReader::new(Arc::clone(&source), data_key)?;
        let buffer_bytes = plain.memory_bytes();
        let (payload, payload_len, compressed) = if source.flags & FLAG_ZSTD == 0 {
            (Payload::Raw(Box::new(plain)), source.stored_bytes, false)
        } else {
            let mut length = [0; UNCOMPRESSED_LEN_SIZE];
            plain.read_exact(&mut length)?;
            let expected = u64::from_le_bytes(length);
            if expected > MAX_DECOMPRESSED_FRAME_BYTES as u64 {
                return Err(Error::Corrupt(
                    "streamed decompressed frame exceeds existing limit".into(),
                ));
            }
            (
                Payload::CompressedPending(Some(Box::new(plain)), window.ilog2()),
                expected,
                true,
            )
        };
        let decoder_bytes = if compressed {
            (window + 2 * buffer_size()) as u64
        } else {
            0
        };
        Ok(Self {
            source,
            payload,
            payload_len,
            memory_bytes: buffer_bytes + decoder_bytes,
            delivered: 0,
            finished: false,
            failed: false,
        })
    }

    pub(crate) fn header(&self) -> FrameHeader {
        self.source.header
    }

    pub(crate) fn payload_len(&self) -> u64 {
        self.payload_len
    }

    pub(crate) fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    fn verify_end(&mut self) -> io::Result<()> {
        if !self.finished {
            self.payload.finish()?;
            self.source.check().map_err(as_io)?;
            self.finished = true;
        }
        Ok(())
    }

    fn read_inner(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.source.check().map_err(as_io)?;
        if self.delivered == self.payload_len {
            self.verify_end()?;
            return Ok(0);
        }
        let limit = (self.payload_len - self.delivered).min(output.len() as u64) as usize;
        let count = self.payload.read(&mut output[..limit])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "streamed frame plaintext is short",
            ));
        }
        self.delivered += count as u64;
        if self.delivered == self.payload_len {
            self.verify_end()?;
        }
        self.source.check().map_err(as_io)?;
        Ok(count)
    }
}

impl Read for VerifiedFrameReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.failed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame reader failed",
            ));
        }
        let result = self.read_inner(output);
        self.failed = result.is_err();
        result
    }
}

pub(super) fn buffer_size() -> usize {
    lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize
}

pub(super) fn as_io(error: Error) -> io::Error {
    match error {
        Error::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}
