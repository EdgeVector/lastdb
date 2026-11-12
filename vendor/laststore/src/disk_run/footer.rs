//! The run footer, block-ref entries, and replacement-file space admission.

use super::codec::{corrupt, get_len, get_slice, get_varint, put_key, put_varint};
use super::{MAX_INDEX_ENTRIES, MAX_RUN_KEY_BYTES};
use crate::space::{reserve_space, SpacePolicy, SpaceReservation};
use crate::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const MAGIC: [u8; 8] = *b"LDRUN\x01\0\0";
const VERSION: u32 = 1;
pub(super) const FOOTER_BYTES: usize = 64;
const PUBLICATION_BYTES: u64 = 4096;
const OUTPUT_CHUNK_BYTES: u64 = 1 << 20;

pub(super) fn try_vec<T>(count: usize) -> Result<Vec<T>> {
    let mut v = Vec::new();
    v.try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    Ok(v)
}

pub(super) fn refs_footprint(refs: &[BlockRef], cap: usize) -> usize {
    cap * std::mem::size_of::<BlockRef>()
        + refs.iter().map(|r| r.first_key.capacity()).sum::<usize>()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Footer {
    pub(super) run_id: Uuid,
    pub(super) top_offset: u64,
    pub(super) top_len: u32,
    pub(super) records: u64,
    pub(super) max_payload: u32,
    pub(super) data_blocks: u32,
}

impl Footer {
    pub(super) fn encode(&self) -> [u8; FOOTER_BYTES] {
        let mut out = [0u8; FOOTER_BYTES];
        out[0..8].copy_from_slice(&MAGIC);
        out[8..12].copy_from_slice(&VERSION.to_le_bytes());
        out[12..28].copy_from_slice(self.run_id.as_bytes());
        out[28..36].copy_from_slice(&self.top_offset.to_le_bytes());
        out[36..40].copy_from_slice(&self.top_len.to_le_bytes());
        out[40..48].copy_from_slice(&self.records.to_le_bytes());
        out[48..52].copy_from_slice(&self.max_payload.to_le_bytes());
        out[52..56].copy_from_slice(&self.data_blocks.to_le_bytes());
        let crc = crc32fast::hash(&out[..60]).to_le_bytes();
        out[60..64].copy_from_slice(&crc);
        out
    }

    pub(super) fn decode(bytes: &[u8; FOOTER_BYTES]) -> Result<Self> {
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        if bytes[0..8] != MAGIC || u32_at(60) != crc32fast::hash(&bytes[..60]) {
            return Err(corrupt("bad footer"));
        }
        if u32_at(8) != VERSION {
            return Err(corrupt("unknown run version"));
        }
        Ok(Self {
            run_id: Uuid::from_slice(&bytes[12..28]).unwrap(),
            top_offset: u64_at(28),
            top_len: u32_at(36),
            records: u64_at(40),
            max_payload: u32_at(48),
            data_blocks: u32_at(52),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockRef {
    pub(super) first_key: Vec<u8>,
    pub(super) number: u64,
    pub(super) offset: u64,
    pub(super) len: u32,
}

pub(super) fn put_refs(out: &mut Vec<u8>, refs: &[BlockRef]) {
    put_varint(out, refs.len() as u64);
    let mut prev: &[u8] = &[];
    for block in refs {
        put_key(out, prev, &block.first_key);
        put_varint(out, block.number);
        put_varint(out, block.offset);
        put_varint(out, u64::from(block.len));
        prev = &block.first_key;
    }
}

pub(super) fn get_refs_bounded(
    bytes: &[u8],
    at: &mut usize,
    maximum: u64,
) -> Result<(Vec<BlockRef>, u64)> {
    let count = get_len(bytes, at, MAX_INDEX_ENTRIES)?;
    let mut refs: Vec<BlockRef> = try_vec(count)?;
    let entries = refs
        .capacity()
        .checked_mul(std::mem::size_of::<BlockRef>())
        .ok_or(Error::ReadBudgetExceeded)?;
    let mut used = admit(admit(0, bytes.len(), maximum)?, entries, maximum)?;
    for _ in 0..count {
        let prev = refs.last().map_or(&[][..], |e| e.first_key.as_slice());
        let shared = get_len(bytes, at, prev.len())?;
        let suffix_len = get_len(bytes, at, MAX_RUN_KEY_BYTES - shared)?;
        let suffix = get_slice(bytes, at, suffix_len)?;
        let (number, offset) = (get_varint(bytes, at)?, get_varint(bytes, at)?);
        let len = u32::try_from(get_varint(bytes, at)?).map_err(|_| corrupt("block length"))?;
        let mut key = try_vec(shared + suffix_len)?;
        used = admit(used, key.capacity(), maximum)?;
        key.extend_from_slice(&prev[..shared]);
        key.extend_from_slice(suffix);
        if refs.last().is_some_and(|p| p.first_key >= key) {
            return Err(corrupt("block entries are not ascending"));
        }
        refs.push(BlockRef {
            first_key: key,
            number,
            offset,
            len,
        });
    }
    Ok((refs, used))
}

fn admit(used: u64, bytes: usize, maximum: u64) -> Result<u64> {
    used.checked_add(bytes as u64)
        .filter(|&v| v <= maximum)
        .ok_or(Error::ReadBudgetExceeded)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct Top {
    pub(super) records: u64,
    pub(super) data_blocks: u64,
    pub(super) max_payload: u64,
    pub(super) indexes: Vec<BlockRef>,
    pub(super) last_key: Vec<u8>,
}

impl Top {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for v in [self.records, self.data_blocks, self.max_payload] {
            put_varint(&mut out, v);
        }
        put_refs(&mut out, &self.indexes);
        put_varint(&mut out, self.last_key.len() as u64);
        out.extend_from_slice(&self.last_key);
        out
    }

    pub(super) fn decode_bounded(bytes: &[u8], maximum: u64) -> Result<Self> {
        let mut at = 0;
        let (records, data_blocks, max_payload) = (
            get_varint(bytes, &mut at)?,
            get_varint(bytes, &mut at)?,
            get_varint(bytes, &mut at)?,
        );
        let (indexes, used) = get_refs_bounded(bytes, &mut at, maximum)?;
        let last_len = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES)?;
        let last = get_slice(bytes, &mut at, last_len)?;
        if at != bytes.len() {
            return Err(corrupt("trailing bytes in top block"));
        }
        let mut last_key = try_vec(last_len)?;
        admit(used, last_key.capacity(), maximum)?;
        last_key.extend_from_slice(last);
        Ok(Self {
            records,
            data_blocks,
            max_payload,
            indexes,
            last_key,
        })
    }

    pub(super) fn agrees_with(&self, footer: &Footer) -> bool {
        self.records == footer.records
            && self.data_blocks == u64::from(footer.data_blocks)
            && self.max_payload == u64::from(footer.max_payload)
    }
}

#[derive(Clone)]
pub(crate) struct MergeSpace(Arc<Mutex<Pending>>);

struct Pending {
    policy: SpacePolicy,
    ticket: SpaceReservation,
    outputs: Vec<PathBuf>,
    keep_outputs: bool,
}

impl MergeSpace {
    pub(crate) fn new(root: &Path, policy: &SpacePolicy) -> Result<Self> {
        let mut policy = policy.clone();
        policy.replacement_reserve_bytes = 0;
        let ticket = reserve_space(root, &policy, PUBLICATION_BYTES)?;
        Ok(Self(Arc::new(Mutex::new(Pending {
            policy,
            ticket,
            outputs: Vec::new(),
            keep_outputs: false,
        }))))
    }

    pub(crate) fn admit_run(&self, bytes: u64) -> Result<()> {
        let mut pending = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let overflow = || Error::WriteCapacity("disk run: replacement reservation overflow".into());
        let required = bytes.checked_add(PUBLICATION_BYTES).ok_or_else(overflow)?;
        if required <= pending.ticket.bytes() {
            return Ok(());
        }
        let chunk = pending
            .ticket
            .bytes()
            .checked_add(OUTPUT_CHUNK_BYTES)
            .ok_or_else(overflow)?
            .max(required);
        let nospc = rustix::io::Errno::NOSPC.raw_os_error();
        let extra = match pending.ticket.reserve_extension(&pending.policy, chunk) {
            Err(Error::Io(io)) if io.raw_os_error() == Some(nospc) => {
                pending.ticket.reserve_extension(&pending.policy, required)
            }
            other => other,
        }?;
        pending.ticket.absorb(extra);
        Ok(())
    }

    pub(crate) fn materialize(&self, bytes: u64) -> Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ticket
            .materialize(bytes)
    }

    pub(crate) fn admit_current(&self, bytes: u64) -> Result<()> {
        let mut pending = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if bytes > pending.ticket.bytes() {
            let extra = pending.ticket.reserve_extension(&pending.policy, bytes)?;
            pending.ticket.absorb(extra);
        }
        Ok(())
    }

    pub(crate) fn output(&self, path: &Path) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .outputs
            .push(path.to_path_buf());
    }

    pub(crate) fn keep_outputs(&self) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keep_outputs = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.keep_outputs {
            for path in &self.outputs {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}
