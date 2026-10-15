use super::batch::{BatchPlan, RecoveredBatch, RecoveredMutation};
use super::marker::Marker;
use super::stream::Stream;
use super::{JournalMutation, JournalTail};
use crate::disk_pack::bytes::*;
use crate::disk_pack::{corrupt, record, Current, Location, MutationVersion, Source};
use crate::Result;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Bounded recovery of one captured artifact TAIL, in complete batch order.
/// It never publishes roots. The caller must finish recovery before it exposes state.
pub struct JournalRecovery {
    root: PathBuf,
    current: Current,
    tail: JournalTail,
    next: u64,
    reserved_bytes: u64,
    encoded_bytes: u64,
    complete: bool,
    failed: bool,
}

impl JournalRecovery {
    /// Capture the separate artifact's CURRENT and TAIL exactly once.
    /// This unit accepts the initial suffix after CURRENT; stream rotation is refused.
    pub fn open(root: &Path) -> Result<Self> {
        let current = super::writer::artifact_current(root)?;
        let tail = JournalTail::read(root)?;
        let next = current
            .frontier
            .checked_add(1)
            .ok_or_else(|| corrupt("recovery sequence exhausted"))?;
        if tail.stream != current.journal_stream || tail.first_batch != next {
            return Err(corrupt("journal TAIL does not extend the artifact CURRENT"));
        }
        let complete = tail.last_batch < next;
        Ok(Self {
            root: root.to_path_buf(),
            current,
            tail,
            next,
            reserved_bytes: 0,
            encoded_bytes: 0,
            complete,
            failed: false,
        })
    }

    /// Captured durable boundary. Later writer publications cannot extend this call.
    pub fn tail(&self) -> &JournalTail {
        &self.tail
    }

    /// True only after every captured batch and the final aggregate boundary verify.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// Read the next complete batch. An error poisons the recovery object.
    /// None means the captured tail is complete, never a resource-limit or damage result.
    pub fn next_batch(&mut self) -> Result<Option<RecoveredBatch>> {
        if self.failed {
            return Err(super::refuse(
                "recovery failed; do not publish partial state",
            ));
        }
        if self.complete {
            return Ok(None);
        }
        self.failed = true;
        let recovered = self.read_next()?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(recovered.encoded_bytes)
            .ok_or_else(|| corrupt("recovery byte count overflow"))?;
        if self.next == self.tail.last_batch {
            if self.encoded_bytes != self.tail.encoded_bytes
                || recovered.marker_digest != self.tail.last_marker_digest
            {
                return Err(corrupt(
                    "recovered suffix differs from captured durable TAIL",
                ));
            }
            self.complete = true;
        } else {
            self.next = self
                .next
                .checked_add(1)
                .ok_or_else(|| corrupt("recovery sequence exhausted"))?;
        }
        self.failed = false;
        Ok(Some(recovered))
    }

    fn read_next(&mut self) -> Result<RecoveredBatch> {
        let mut stream = Stream::new(
            &self.root,
            self.current.clone(),
            self.tail.stream,
            self.next,
        )?;
        let mut begin = [0; 128];
        stream.exact(&mut begin)?;
        let marker = Marker::decode(&begin, false)?;
        if marker.stream != self.tail.stream
            || marker.sequence != self.next
            || marker.previous != self.next - 1
        {
            return Err(corrupt("journal batch marker breaks durable sequence"));
        }
        let reserve = BatchPlan::reserve(marker.logical_bytes)?;
        self.reserved_bytes = self
            .reserved_bytes
            .checked_add(reserve)
            .filter(|bytes| *bytes + 8192 <= super::MAX_SUFFIX_BYTES)
            .ok_or_else(|| super::refuse("recovery suffix admission limit"))?;
        stream.set_logical_length(marker.logical_bytes)?;
        let mut hash = Sha256::new();
        hash.update(begin);
        let mut mutations = Vec::new();
        mutations
            .try_reserve_exact(marker.operations as usize)
            .map_err(|_| super::refuse("recovery operation buffer admission failed"))?;
        for number in 0..marker.operations {
            mutations.push(read_operation(&mut stream, &mut hash, &marker, number)?);
        }
        if stream.position.checked_add(128) != Some(marker.logical_bytes) {
            return Err(corrupt("journal operations do not end at COMMIT"));
        }
        let mut commit = [0; 128];
        stream.exact(&mut commit)?;
        let mut expected = marker.clone();
        expected.batch_digest = hash.finalize().into();
        if Marker::decode(&commit, true)? != expected {
            return Err(corrupt("COMMIT differs from complete BEGIN and operations"));
        }
        let encoded_bytes = stream.finish()?;
        Ok(RecoveredBatch {
            sequence: marker.sequence,
            previous: marker.previous,
            accepted_ms: marker.accepted_ms,
            mutations,
            encoded_bytes,
            marker_digest: digest(&commit),
            costs: stream.call.costs(),
        })
    }
}

fn read_operation(
    stream: &mut Stream,
    batch_hash: &mut Sha256,
    marker: &Marker,
    number: u32,
) -> Result<RecoveredMutation> {
    stream.call.examine()?;
    let start = stream.position;
    let mut header = [0; 64];
    stream.exact(&mut header)?;
    if header[1] != 0 || u32_at(&header, 4) != number || !matches!(header[0], 1 | 2) {
        return Err(corrupt("journal operation kind, flags, or order"));
    }
    let collection_bytes = u32_at(&header, 8) as usize;
    let key_bytes = u32_at(&header, 12) as usize;
    if collection_bytes > super::super::KEY_BYTES - 4
        || key_bytes > super::super::KEY_BYTES
        || u16_at(&header, 2) >= 1024
    {
        return Err(corrupt("journal operation coordinates exceed limits"));
    }
    let payload_bytes = u64_at(&header, 16);
    let operation_bytes = 64u64 + collection_bytes as u64 + key_bytes as u64;
    if operation_bytes.checked_add(payload_bytes) != Some(u64_at(&header, 24)) {
        return Err(corrupt("journal operation complete length"));
    }
    let suffix_len = u64_at(&header, 24)
        .checked_sub(64)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| corrupt("journal operation allocation length"))?;
    let buffer = stream.buffer(suffix_len)?;
    let mut zeroed = header;
    zeroed[32..64].fill(0);
    let mut hash = Sha256::new();
    hash.update(zeroed);
    hash.update(&buffer.bytes);
    if hash.finalize().as_slice() != &header[32..64] {
        return Err(corrupt("journal operation checksum"));
    }
    batch_hash.update(header);
    batch_hash.update(&buffer.bytes);
    stream.call.charge((collection_bytes + key_bytes) as u64)?;
    let collection = std::str::from_utf8(&buffer.bytes[..collection_bytes])
        .map_err(|_| corrupt("journal collection is not UTF-8"))?
        .to_string();
    let key = std::str::from_utf8(&buffer.bytes[collection_bytes..collection_bytes + key_bytes])
        .map_err(|_| corrupt("journal storage key is not UTF-8"))?
        .to_string();
    let version = MutationVersion {
        batch: marker.sequence,
        operation: number,
    };
    let payload_start = collection_bytes + key_bytes;
    let location = decode_location(
        &header,
        &buffer.bytes[payload_start..],
        &collection,
        &key,
        version,
        marker.stream,
        start + 64 + payload_start as u64,
    )?;
    let mut body = buffer.into_vec();
    let body = if header[0] == 1 {
        body.drain(..payload_start + 96 + key_bytes);
        Some(body)
    } else {
        None
    };
    Ok(RecoveredMutation {
        mutation: JournalMutation {
            collection,
            group: u16_at(&header, 2),
            key,
            body,
        },
        location,
    })
}

fn decode_location(
    header: &[u8; 64],
    payload: &[u8],
    collection: &str,
    key: &str,
    version: MutationVersion,
    stream: Uuid,
    position: u64,
) -> Result<Location> {
    if header[0] == 2 {
        if !payload.is_empty() {
            return Err(corrupt("journal tombstone contains a payload"));
        }
        return Ok(Location {
            source: Source::Tombstone,
            version,
            record_bytes: 0,
            object: Uuid::nil(),
            first_block: 0,
            first_position: 0,
            blocks: 0,
            digest: [0; 32],
        });
    }
    let body_length = payload
        .len()
        .checked_sub(96 + key.len())
        .ok_or_else(|| corrupt("journal record shorter than key"))?;
    let expected = record::record_header(collection, key, body_length as u64, version)?;
    if payload[..96] != expected || payload[96..96 + key.len()] != *key.as_bytes() {
        return Err(corrupt(
            "journal record key, collection, or version differs from operation",
        ));
    }
    Ok(Location {
        source: Source::Journal,
        version,
        record_bytes: payload.len() as u64,
        object: stream,
        first_block: position / 4096,
        first_position: (position % 4096) as u32,
        blocks: u32::try_from(((position % 4096) + payload.len() as u64).div_ceil(4096))
            .map_err(|_| corrupt("journal record block count overflow"))?,
        digest: digest(payload),
    })
}
