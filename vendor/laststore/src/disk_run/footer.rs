//! The run footer and the entries of index and top blocks.

use super::codec::{corrupt, get_key, get_len, get_slice, get_varint, put_key, put_varint};
use super::MAX_RUN_KEY_BYTES;
use crate::{Error, Result};
use uuid::Uuid;

const MAGIC: [u8; 8] = *b"LDRUN\x01\0\0";
const VERSION: u32 = 1;
pub(super) const FOOTER_BYTES: usize = 64;
/// Largest block entry count one index or top block may claim.
const MAX_ENTRIES: usize = super::MAX_INDEX_ENTRIES;

/// The fixed clear trailer of a run file. The top block repeats the counts,
/// and the reader requires both copies to agree, so the clear copy cannot
/// change what an authenticated block says.
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
        let crc = crc32fast::hash(&out[..60]);
        out[60..64].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub(super) fn decode(bytes: &[u8; FOOTER_BYTES]) -> Result<Self> {
        let crc = u32::from_le_bytes(bytes[60..64].try_into().expect("4 bytes"));
        if bytes[0..8] != MAGIC || crc != crc32fast::hash(&bytes[..60]) {
            return Err(corrupt("bad footer"));
        }
        if u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes")) != VERSION {
            return Err(corrupt("unknown run version"));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8"));
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4"));
        Ok(Self {
            run_id: Uuid::from_slice(&bytes[12..28]).expect("16 bytes"),
            top_offset: u64_at(28),
            top_len: u32_at(36),
            records: u64_at(40),
            max_payload: u32_at(48),
            data_blocks: u32_at(52),
        })
    }
}

/// The place of one block, as an index or top block lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockRef {
    pub(super) first_key: Vec<u8>,
    /// Data block number, or index block number in the top block.
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

pub(super) fn get_refs(bytes: &[u8], at: &mut usize) -> Result<Vec<BlockRef>> {
    let count = get_len(bytes, at, MAX_ENTRIES)?;
    let mut refs: Vec<BlockRef> = Vec::with_capacity(count.min(4096));
    let mut key = Vec::new();
    for _ in 0..count {
        get_key(bytes, at, &mut key)?;
        if refs.last().is_some_and(|prev| prev.first_key >= key) {
            return Err(corrupt("block entries are not ascending"));
        }
        let number = get_varint(bytes, at)?;
        let offset = get_varint(bytes, at)?;
        let len = u32::try_from(get_varint(bytes, at)?).map_err(|_| corrupt("block length"))?;
        refs.push(BlockRef {
            first_key: key.clone(),
            number,
            offset,
            len,
        });
    }
    Ok(refs)
}

/// Backup-only decode admission includes the retained input, reference entries,
/// and every expanded prefix key. No key buffer grows before that admission.
pub(super) fn get_refs_bounded(
    bytes: &[u8],
    at: &mut usize,
    maximum: u64,
) -> Result<Vec<BlockRef>> {
    let count = get_len(bytes, at, MAX_ENTRIES)?;
    let entries = count
        .checked_mul(std::mem::size_of::<BlockRef>())
        .ok_or(Error::ReadBudgetExceeded)?;
    let mut used = admit(admit(0, bytes.len(), maximum)?, entries, maximum)?;
    let mut refs: Vec<BlockRef> = Vec::new();
    refs.try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    for _ in 0..count {
        let previous = refs
            .last()
            .map_or(&[][..], |entry| entry.first_key.as_slice());
        let shared = get_len(bytes, at, previous.len())?;
        let suffix_len = get_len(bytes, at, MAX_RUN_KEY_BYTES - shared)?;
        let suffix = get_slice(bytes, at, suffix_len)?;
        let key_len = shared
            .checked_add(suffix_len)
            .ok_or(Error::ReadBudgetExceeded)?;
        used = admit(used, key_len, maximum)?;
        let number = get_varint(bytes, at)?;
        let offset = get_varint(bytes, at)?;
        let len = u32::try_from(get_varint(bytes, at)?).map_err(|_| corrupt("block length"))?;
        let mut key = Vec::new();
        key.try_reserve_exact(key_len)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        key.extend_from_slice(&previous[..shared]);
        key.extend_from_slice(suffix);
        if refs.last().is_some_and(|prev| prev.first_key >= key) {
            return Err(corrupt("block entries are not ascending"));
        }
        refs.push(BlockRef {
            first_key: key,
            number,
            offset,
            len,
        });
    }
    Ok(refs)
}

fn admit(used: u64, bytes: usize, maximum: u64) -> Result<u64> {
    used.checked_add(u64::try_from(bytes).map_err(|_| Error::ReadBudgetExceeded)?)
        .filter(|value| *value <= maximum)
        .ok_or(Error::ReadBudgetExceeded)
}

/// The authenticated summary in the top block.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        put_varint(&mut out, self.records);
        put_varint(&mut out, self.data_blocks);
        put_varint(&mut out, self.max_payload);
        put_refs(&mut out, &self.indexes);
        put_varint(&mut out, self.last_key.len() as u64);
        out.extend_from_slice(&self.last_key);
        out
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut at = 0;
        let records = get_varint(bytes, &mut at)?;
        let data_blocks = get_varint(bytes, &mut at)?;
        let max_payload = get_varint(bytes, &mut at)?;
        let indexes = get_refs(bytes, &mut at)?;
        let last_len = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES)?;
        let last_key = get_slice(bytes, &mut at, last_len)?.to_vec();
        if at != bytes.len() {
            return Err(corrupt("trailing bytes in top block"));
        }
        Ok(Self {
            records,
            data_blocks,
            max_payload,
            indexes,
            last_key,
        })
    }

    /// The bounded backup reader keeps the source payload and decoded top
    /// together, so both count before any expanded key allocation.
    pub(super) fn decode_bounded(bytes: &[u8], maximum: u64) -> Result<Self> {
        let mut at = 0;
        let records = get_varint(bytes, &mut at)?;
        let data_blocks = get_varint(bytes, &mut at)?;
        let max_payload = get_varint(bytes, &mut at)?;
        let indexes = get_refs_bounded(bytes, &mut at, maximum)?;
        let entries = indexes
            .capacity()
            .checked_mul(std::mem::size_of::<BlockRef>())
            .ok_or(Error::ReadBudgetExceeded)?;
        let mut used = admit(admit(0, bytes.len(), maximum)?, entries, maximum)?;
        for entry in &indexes {
            used = admit(used, entry.first_key.capacity(), maximum)?;
        }
        let last_len = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES)?;
        admit(used, last_len, maximum)?;
        let last = get_slice(bytes, &mut at, last_len)?;
        if at != bytes.len() {
            return Err(corrupt("trailing bytes in top block"));
        }
        let mut last_key = Vec::new();
        last_key
            .try_reserve_exact(last_len)
            .map_err(|_| Error::ReadBudgetExceeded)?;
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
