use super::super::bytes::*;
use super::super::{corrupt, Current};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::{durability, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use uuid::Uuid;

/// One captured durable journal boundary. All referenced batches precede publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalTail {
    /// Exact stream identity.
    pub stream: Uuid,
    /// First batch in this retained tail interval.
    pub first_batch: u64,
    /// Last complete batch, or first_batch minus one for an empty interval.
    pub last_batch: u64,
    /// Complete physical part and map bytes in the interval; excludes TAIL itself.
    pub encoded_bytes: u64,
    /// Durable publication time in Unix milliseconds.
    pub published_ms: u64,
    /// SHA-256 over the final complete COMMIT marker.
    pub last_marker_digest: [u8; 32],
}

impl JournalTail {
    pub(super) fn initial(current: &Current) -> Result<Self> {
        Ok(Self {
            stream: current.journal_stream,
            first_batch: current
                .frontier
                .checked_add(1)
                .ok_or_else(|| corrupt("batch sequence exhausted"))?,
            last_batch: current.frontier,
            encoded_bytes: 0,
            published_ms: 0,
            last_marker_digest: [0; 32],
        })
    }

    /// Decode a complete TAIL. Stream rotation remains unsupported in this artifact unit.
    pub fn decode(bytes: &[u8; 4096]) -> Result<Self> {
        if &bytes[..8] != b"LDJTL1\0\0" || u16_at(bytes, 8) != 1 || u16_at(bytes, 10) != 4096 {
            return Err(corrupt("journal TAIL format"));
        }
        zero(&bytes[12..16])?;
        zero(&bytes[128..])?;
        verify_checksum(bytes, 96)?;
        let tail = Self {
            stream: nonnil(uuid_at(bytes, 16))?,
            first_batch: u64_at(bytes, 32),
            last_batch: u64_at(bytes, 40),
            encoded_bytes: u64_at(bytes, 48),
            published_ms: u64_at(bytes, 56),
            last_marker_digest: bytes[64..96].try_into().expect("TAIL marker digest"),
        };
        tail.validate()?;
        Ok(tail)
    }

    /// Encode the complete fixed-width boundary with zero slack and a checksum.
    pub fn encode(&self) -> Result<[u8; 4096]> {
        self.validate()?;
        let mut bytes = [0; 4096];
        bytes[..8].copy_from_slice(b"LDJTL1\0\0");
        put16(&mut bytes, 8, 1);
        put16(&mut bytes, 10, 4096);
        bytes[16..32].copy_from_slice(self.stream.as_bytes());
        put64(&mut bytes, 32, self.first_batch);
        put64(&mut bytes, 40, self.last_batch);
        put64(&mut bytes, 48, self.encoded_bytes);
        put64(&mut bytes, 56, self.published_ms);
        bytes[64..96].copy_from_slice(&self.last_marker_digest);
        let sum = checksum(&bytes, 96);
        bytes[96..128].copy_from_slice(&sum);
        Ok(bytes)
    }

    fn validate(&self) -> Result<()> {
        nonnil(self.stream)?;
        if self.first_batch == 0
            || self.last_batch < self.first_batch - 1
            || self.last_batch - (self.first_batch - 1) > super::MAX_SUFFIX_BATCHES
            || self.encoded_bytes > super::MAX_SUFFIX_BYTES
        {
            return Err(corrupt("journal TAIL interval exceeds limits"));
        }
        let empty = self.last_batch == self.first_batch - 1;
        if empty != (self.encoded_bytes == 0) || (empty && self.last_marker_digest != [0; 32]) {
            return Err(corrupt("inconsistent empty journal TAIL"));
        }
        Ok(())
    }

    /// Read exactly one published boundary, without a directory walk.
    pub fn read(root: &Path) -> Result<Self> {
        let mut file = measured::open(root.join("JOURNAL_TAIL"))?;
        if file.metadata()?.len() != 4096 {
            return Err(corrupt("journal TAIL length"));
        }
        let mut bytes = [0; 4096];
        let _class = ReadClass::FormatProbe.enter();
        file.read_exact(&mut bytes)?;
        Self::decode(&bytes)
    }

    pub(super) fn publish(&self, root: &Path) -> Result<()> {
        let temporary = root.join(format!("TAIL.pending-{}", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&self.encode()?)?;
        durability::sync_dirty_file(&file)?;
        fs::rename(&temporary, root.join("JOURNAL_TAIL"))?;
        durability::sync_directory(&File::open(root)?)
    }
}
