//! Two-pass bounded GCM authentication for existing immutable frame extents.
use super::{FrameHeader, FLAG_ZSTD, MAX_DECOMPRESSED_FRAME_BYTES, UNCOMPRESSED_LEN_SIZE};
use crate::{
    read_receipt::{TemporaryBytes, Timer},
    Error, Result,
};
use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;

mod gcm;
mod source;
use gcm::GcmReader;
use source::Source;

enum Payload {
    Raw(Box<GcmReader>),
    CompressedPending(Option<Box<GcmReader>>),
    Compressed(Box<zstd::stream::read::Decoder<'static, GcmReader>>),
    Finished,
}

impl Read for Payload {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        self.prepare()?;
        match self {
            Self::Raw(reader) => reader.read(output),
            Self::Compressed(reader) => reader.read(output),
            Self::CompressedPending(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame decoder is unavailable",
            )),
            Self::Finished => Ok(0),
        }
    }
}

impl Payload {
    fn prepare(&mut self) -> io::Result<()> {
        if let Self::CompressedPending(input) = self {
            let input = input.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "streamed frame decoder failed")
            })?;
            let mut decoder = zstd::stream::read::Decoder::with_buffer(*input)?;
            decoder.window_log_max(MAX_DECOMPRESSED_FRAME_BYTES.ilog2())?;
            *self = Self::Compressed(Box::new(decoder));
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        let mut payload = std::mem::replace(self, Self::Finished);
        let mut extra = [0];
        if payload.read(&mut extra)? != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame has excess plaintext",
            ));
        }
        match payload {
            Self::Raw(mut reader) => reader.verify_consumed(),
            Self::Compressed(reader) => {
                // Decoder EOF alone is insufficient: require complete encoded consumption
                // and force the inner reader's second GCM tag verification explicitly.
                let mut inner = (*reader).finish();
                inner.verify_consumed()
            }
            Self::CompressedPending(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "streamed frame decoder is unavailable",
            )),
            Self::Finished => Ok(()),
        }
    }
}

/// A frame authenticates in full before this reader exposes any payload bytes.
/// EOF also verifies the second pass and exact decoded length.
pub(crate) struct VerifiedFrameReader {
    source: Arc<Source>,
    payload: Payload,
    payload_len: u64,
    memory_bytes: u64,
    decoder_charge: Option<TemporaryBytes>,
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
        let timer = Timer::start();
        let result = Self::open_inner(file, offset, encoded_bytes, data_key);
        timer.finish(|receipt, nanos| {
            receipt.decode_nanos = receipt.decode_nanos.saturating_add(nanos)
        });
        result
    }

    fn open_inner(
        file: &File,
        offset: u64,
        encoded_bytes: u64,
        data_key: &[u8; 32],
    ) -> Result<Self> {
        let source = Arc::new(Source::open(file, offset, encoded_bytes)?);
        GcmReader::new(Arc::clone(&source), data_key)?.authenticate()?;
        let mut plain = GcmReader::new(Arc::clone(&source), data_key)?;
        let buffer_bytes = plain.memory_bytes();
        let (payload, payload_len, decoder_charge) = if source.flags & FLAG_ZSTD == 0 {
            (Payload::Raw(Box::new(plain)), source.stored_bytes, None)
        } else {
            let mut length = [0; UNCOMPRESSED_LEN_SIZE];
            plain.read_exact(&mut length)?;
            let expected = u64::from_le_bytes(length);
            if expected > MAX_DECOMPRESSED_FRAME_BYTES as u64 {
                return Err(Error::Corrupt(
                    "streamed decompressed frame exceeds existing limit".into(),
                ));
            }
            // Reserve the maximum existing window plus bounded decoder context/block
            // workspace. Callers include this reservation before output allocation.
            let charge = TemporaryBytes::new(decoder_reserve());
            (
                Payload::CompressedPending(Some(Box::new(plain))),
                expected,
                Some(charge),
            )
        };
        let memory_bytes = buffer_bytes
            + if decoder_charge.is_some() {
                decoder_reserve() as u64
            } else {
                0
            };
        Ok(Self {
            source,
            payload,
            payload_len,
            memory_bytes,
            decoder_charge,
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

    /// Reserve this amount before the first nonempty read, including an empty payload's EOF probe.
    pub(crate) fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    fn verify_end(&mut self) -> io::Result<()> {
        if !self.finished {
            self.payload.finish()?;
            self.source.check().map_err(as_io)?;
            drop(self.decoder_charge.take());
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
        let timer = Timer::start();
        let result = self.read_inner(output);
        self.failed = result.is_err();
        timer.finish(|receipt, nanos| {
            receipt.decode_nanos = receipt.decode_nanos.saturating_add(nanos);
            receipt.decoded_bytes = receipt
                .decoded_bytes
                .saturating_add(result.as_ref().copied().unwrap_or(0) as u64);
        });
        result
    }
}

fn buffer_size() -> usize {
    lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes as usize
}

fn decoder_reserve() -> usize {
    MAX_DECOMPRESSED_FRAME_BYTES + 2 * buffer_size()
}

fn as_io(error: Error) -> io::Error {
    match error {
        Error::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}
