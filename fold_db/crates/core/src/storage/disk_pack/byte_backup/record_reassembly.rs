//! Reassemble one existing native record transfer from four authenticated streams.
use super::{ClientBackupStream, ClientStreamSummary, StorageResult, StoredRecordProfile};
use sha2::{Digest, Sha256};
use std::io::{self, Read};

struct VerifiedStream {
    input: Box<dyn Read + Send>,
    expected: ClientStreamSummary,
    bytes: u64,
    digest: Sha256,
}
impl VerifiedStream {
    fn new(input: Box<dyn Read + Send>, expected: &ClientStreamSummary) -> Self {
        Self {
            input,
            expected: expected.clone(),
            bytes: 0,
            digest: Sha256::new(),
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        if self.bytes != self.expected.bytes
            || crate::hex::hex_lower(self.digest.clone().finalize())
                != self.expected.sha256.as_str()
            || self.input.read(&mut [0u8; 1])? != 0
        {
            return Err(io::Error::other("record stream closure differs"));
        }
        Ok(())
    }
}
impl Read for VerifiedStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let left = self
            .expected
            .bytes
            .checked_sub(self.bytes)
            .ok_or_else(|| io::Error::other("record stream length overflow"))?;
        if left == 0 {
            self.finish()?;
            return Ok(0);
        }
        let maximum = left.min(bytes.len() as u64) as usize;
        let count = self.input.read(&mut bytes[..maximum])?;
        if count == 0 {
            return Err(io::Error::other("record stream truncated"));
        }
        self.bytes += count as u64;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }
}

/// Inputs contain decoded stream bytes in ordinal order. Only metadata and
/// sequence chunks receive client metadata decryption; bodies remain unchanged.
pub struct ReassembledRecordStream {
    metadata: VerifiedStream,
    r2: VerifiedStream,
    b2: VerifiedStream,
    sequence: VerifiedStream,
    current: Option<ClientBackupStream>,
    previous: Option<ClientBackupStream>,
    remaining: u64,
    finished: bool,
}
impl ReassembledRecordStream {
    pub fn new(
        profile: &StoredRecordProfile,
        metadata: Box<dyn Read + Send>,
        r2: Box<dyn Read + Send>,
        b2: Box<dyn Read + Send>,
        sequence: Box<dyn Read + Send>,
    ) -> StorageResult<Self> {
        profile.validate()?;
        Ok(Self {
            metadata: VerifiedStream::new(metadata, profile.summary(ClientBackupStream::Metadata)?),
            r2: VerifiedStream::new(r2, profile.summary(ClientBackupStream::R2Bodies)?),
            b2: VerifiedStream::new(b2, profile.summary(ClientBackupStream::B2Bodies)?),
            sequence: VerifiedStream::new(sequence, profile.summary(ClientBackupStream::Sequence)?),
            current: None,
            previous: None,
            remaining: 0,
            finished: false,
        })
    }
    fn next_run(&mut self) -> io::Result<bool> {
        let mut tag = [0];
        if self.sequence.read(&mut tag)? == 0 {
            self.metadata.finish()?;
            self.r2.finish()?;
            self.b2.finish()?;
            self.finished = true;
            return Ok(false);
        }
        let kind = ClientBackupStream::from_tag(tag[0])
            .map_err(|_| io::Error::other("record sequence tag differs"))?;
        let mut length = [0; 8];
        self.sequence.read_exact(&mut length)?;
        let length = u64::from_le_bytes(length);
        if length == 0 || self.previous == Some(kind) {
            return Err(io::Error::other("record sequence run differs"));
        }
        self.current = Some(kind);
        self.previous = Some(kind);
        self.remaining = length;
        Ok(true)
    }
}
impl Read for ReassembledRecordStream {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.finished {
            return Ok(0);
        }
        if self.remaining == 0 && !self.next_run()? {
            return Ok(0);
        }
        let maximum = self.remaining.min(output.len() as u64) as usize;
        let count = match self.current {
            Some(ClientBackupStream::Metadata) => self.metadata.read(&mut output[..maximum])?,
            Some(ClientBackupStream::R2Bodies) => self.r2.read(&mut output[..maximum])?,
            Some(ClientBackupStream::B2Bodies) => self.b2.read(&mut output[..maximum])?,
            _ => return Err(io::Error::other("record sequence source missing")),
        };
        if count == 0 {
            return Err(io::Error::other("record sequence exceeds source"));
        }
        self.remaining -= count as u64;
        Ok(count)
    }
}
