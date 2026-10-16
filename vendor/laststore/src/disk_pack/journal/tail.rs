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
    /// Exact prior stream checkpoint during an empty-stream transition.
    pub predecessor: Option<TailPredecessor>,
}

/// A closed stream boundary. The digest covers all published old TAIL bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailPredecessor {
    /// Stream selected by CURRENT before this transition.
    pub stream: Uuid,
    /// Complete root frontier before the new stream can admit a batch.
    pub checkpoint: u64,
    /// SHA-256 of all 4,096 archived TAIL bytes, including its stored checksum.
    pub tail_digest: [u8; 32],
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
            predecessor: None,
        })
    }

    /// Decode a complete TAIL, including a bounded empty-stream predecessor.
    pub fn decode(bytes: &[u8; 4096]) -> Result<Self> {
        if &bytes[..8] != b"LDJTL1\0\0" || u16_at(bytes, 8) != 1 || u16_at(bytes, 10) != 4096 {
            return Err(corrupt("journal TAIL format"));
        }
        zero(&bytes[12..16])?;
        zero(&bytes[184..])?;
        verify_checksum(bytes, 96)?;
        let tail = Self {
            stream: nonnil(uuid_at(bytes, 16))?,
            first_batch: u64_at(bytes, 32),
            last_batch: u64_at(bytes, 40),
            encoded_bytes: u64_at(bytes, 48),
            published_ms: u64_at(bytes, 56),
            last_marker_digest: bytes[64..96].try_into().expect("TAIL marker digest"),
            predecessor: if uuid_at(bytes, 128).is_nil() {
                zero(&bytes[128..184])?;
                None
            } else {
                Some(TailPredecessor {
                    stream: uuid_at(bytes, 128),
                    checkpoint: u64_at(bytes, 144),
                    tail_digest: bytes[152..184].try_into().expect("predecessor digest"),
                })
            },
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
        if let Some(previous) = &self.predecessor {
            bytes[128..144].copy_from_slice(previous.stream.as_bytes());
            put64(&mut bytes, 144, previous.checkpoint);
            bytes[152..184].copy_from_slice(&previous.tail_digest);
        }
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
        if let Some(previous) = &self.predecessor {
            nonnil(previous.stream)?;
            if !empty
                || previous.stream == self.stream
                || previous.checkpoint != self.last_batch
                || previous.tail_digest == [0; 32]
            {
                return Err(corrupt("invalid journal stream transition boundary"));
            }
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

    pub(in crate::disk_pack) fn validate_for_current(
        &self,
        root: &Path,
        current: &Current,
    ) -> Result<()> {
        if current.required_features & crate::disk_pack::FEATURE_JOURNAL_LIFECYCLE != 0
            && current.previous_generation == 0
            && self.last_batch > current.frontier
        {
            return Err(corrupt("journal batch precedes initial stream checkpoint"));
        }
        let next = current
            .frontier
            .checked_add(1)
            .ok_or_else(|| corrupt("journal sequence exhaustion"))?;
        if self.first_batch > next || self.last_batch < current.journal_frontier {
            return Err(corrupt("journal TAIL does not extend CURRENT"));
        }
        if self.stream == current.journal_stream {
            return Ok(());
        }
        let previous = self
            .predecessor
            .as_ref()
            .ok_or_else(|| corrupt("unrelated journal stream"))?;
        if previous.stream != current.journal_stream
            || previous.checkpoint != current.frontier
            || self.last_batch != current.frontier
            || self.first_batch != next
        {
            return Err(corrupt(
                "journal transition differs from committed checkpoint",
            ));
        }
        let bytes = read_archived(root, previous.stream, previous.checkpoint)?;
        let archived = Self::decode(&bytes)?;
        if digest(&bytes) != previous.tail_digest
            || archived.stream != previous.stream
            || archived.last_batch != previous.checkpoint
        {
            return Err(corrupt("archived journal TAIL differs from transition"));
        }
        Ok(())
    }
}

pub(super) fn archive_path(root: &Path, stream: Uuid, checkpoint: u64) -> std::path::PathBuf {
    root.join("journal")
        .join(stream.to_string())
        .join(format!("tail-{checkpoint:016x}"))
}

pub(super) fn read_archived(root: &Path, stream: Uuid, checkpoint: u64) -> Result<[u8; 4096]> {
    let mut file = measured::open(archive_path(root, stream, checkpoint))?;
    if file.metadata()?.len() != 4096 {
        return Err(corrupt("archived TAIL length"));
    }
    let mut bytes = [0; 4096];
    let _class = ReadClass::FormatProbe.enter();
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}
