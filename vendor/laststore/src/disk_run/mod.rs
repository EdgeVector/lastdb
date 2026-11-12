//! Disk format v2 run files: one immutable, sorted run of records.

pub mod home;
mod merge;
mod reader;
mod writer;

pub(crate) use merge::{merge_runs, MergeIter, MergeSpace, RunRange};
pub(crate) use reader::{MetadataMemo, RunReader};
pub(crate) use writer::{bloom, PreparedRun, RunWriter};

use uuid::Uuid;

pub(crate) const RUN_SUFFIX: &str = ".ldrun";

/// Largest stored key. Larger keys are refused at write time.
pub const MAX_RUN_KEY_BYTES: usize = 64 * 1024;

pub(crate) const MAX_RUN_VALUE_BYTES: usize = crate::frame::MAX_DECOMPRESSED_FRAME_BYTES / 2;
pub(crate) const MAX_TOP_BYTES: usize = crate::frame::MAX_DECOMPRESSED_FRAME_BYTES;
pub(crate) const MAX_INDEX_ENTRIES: usize = 1 << 24;
pub(crate) const INDEX_COUNTER_BASE: u64 = 1 << 62;
pub(crate) const TOP_COUNTER: u64 = 1 << 63;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Found {
    Value(Vec<u8>),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunMeta {
    pub(crate) run_id: Uuid,
    pub(crate) records: u64,
    pub(crate) tombstones: u64,
    pub(crate) record_bytes: u64,
    pub(crate) tombstone_bytes: u64,
    pub(crate) file_bytes: u64,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
}

impl RunMeta {
    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.first_key.as_slice() <= key && key <= self.last_key.as_slice()
    }
    pub(crate) fn meets(&self, other: &Self) -> bool {
        self.first_key <= other.last_key && other.first_key <= self.last_key
    }
    pub(crate) fn meets_merge_interval(
        &self,
        after: Option<&[u8]>,
        through: Option<&[u8]>,
    ) -> bool {
        after.is_none_or(|a| self.last_key.as_slice() > a)
            && through.is_none_or(|t| self.first_key.as_slice() <= t)
    }
    pub(crate) fn overlaps(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        self.last_key.as_slice() >= lower && upper.is_none_or(|u| self.first_key.as_slice() < u)
    }
}

pub(crate) mod footer {
    use super::codec::{corrupt, get_bytes, get_len, get_varint, put_key, put_varint};
    use super::{MAX_INDEX_ENTRIES, MAX_RUN_KEY_BYTES};
    use crate::{Error, Result};
    use uuid::Uuid;

    const MAGIC: [u8; 8] = *b"LDRUN\x01\0\0";
    const VERSION: u32 = 1;
    pub(crate) const FOOTER_BYTES: usize = 64;

    pub(crate) fn try_vec<T>(count: usize) -> Result<Vec<T>> {
        let mut v = Vec::new();
        v.try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        Ok(v)
    }

    pub(crate) fn refs_footprint(refs: &[BlockRef], cap: usize) -> usize {
        cap * std::mem::size_of::<BlockRef>()
            + refs.iter().map(|r| r.first_key.capacity()).sum::<usize>()
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Footer {
        pub(crate) run_id: Uuid,
        pub(crate) top_offset: u64,
        pub(crate) top_len: u32,
        pub(crate) records: u64,
        pub(crate) max_payload: u32,
        pub(crate) data_blocks: u32,
    }

    impl Footer {
        pub(crate) fn encode(&self) -> [u8; FOOTER_BYTES] {
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

        pub(crate) fn decode(b: &[u8; FOOTER_BYTES]) -> Result<Self> {
            let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
            let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
            if b[0..8] != MAGIC || u32_at(60) != crc32fast::hash(&b[..60]) {
                return Err(corrupt("bad footer"));
            }
            if u32_at(8) != VERSION {
                return Err(corrupt("unknown run version"));
            }
            Ok(Self {
                run_id: Uuid::from_slice(&b[12..28]).unwrap(),
                top_offset: u64_at(28),
                top_len: u32_at(36),
                records: u64_at(40),
                max_payload: u32_at(48),
                data_blocks: u32_at(52),
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct BlockRef {
        pub(crate) first_key: Vec<u8>,
        pub(crate) number: u64,
        pub(crate) offset: u64,
        pub(crate) len: u32,
    }

    pub(crate) fn put_refs(out: &mut Vec<u8>, refs: &[BlockRef]) {
        put_varint(out, refs.len() as u64);
        let mut prev: &[u8] = &[];
        for b in refs {
            put_key(out, prev, &b.first_key);
            [b.number, b.offset, u64::from(b.len)]
                .into_iter()
                .for_each(|v| put_varint(out, v));
            prev = &b.first_key;
        }
    }

    fn admit(used: u64, bytes: usize, max: u64) -> Result<u64> {
        used.checked_add(bytes as u64)
            .filter(|&v| v <= max)
            .ok_or(Error::ReadBudgetExceeded)
    }

    pub(crate) fn get_refs_bounded(
        bytes: &[u8],
        at: &mut usize,
        max: u64,
    ) -> Result<(Vec<BlockRef>, u64)> {
        let count = get_len(bytes, at, MAX_INDEX_ENTRIES)?;
        let mut refs: Vec<BlockRef> = try_vec(count)?;
        let sz = refs
            .capacity()
            .checked_mul(std::mem::size_of::<BlockRef>())
            .ok_or(Error::ReadBudgetExceeded)?;
        let mut used = admit(admit(0, bytes.len(), max)?, sz, max)?;
        for _ in 0..count {
            let prev = refs.last().map_or(&[][..], |e| e.first_key.as_slice());
            let shared = get_len(bytes, at, prev.len())?;
            let suffix = get_bytes(bytes, at, MAX_RUN_KEY_BYTES - shared)?;
            let (number, offset) = (get_varint(bytes, at)?, get_varint(bytes, at)?);
            let len = u32::try_from(get_varint(bytes, at)?).map_err(|_| corrupt("block length"))?;
            let mut first_key = try_vec(shared + suffix.len())?;
            used = admit(used, first_key.capacity(), max)?;
            first_key.extend_from_slice(&prev[..shared]);
            first_key.extend_from_slice(suffix);
            if refs.last().is_some_and(|p| p.first_key >= first_key) {
                return Err(corrupt("block entries are not ascending"));
            }
            refs.push(BlockRef {
                first_key,
                number,
                offset,
                len,
            });
        }
        Ok((refs, used))
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub(crate) struct Top {
        pub(crate) records: u64,
        pub(crate) data_blocks: u64,
        pub(crate) max_payload: u64,
        pub(crate) indexes: Vec<BlockRef>,
        pub(crate) last_key: Vec<u8>,
    }

    impl Top {
        pub(crate) fn encode(&self) -> Vec<u8> {
            let mut out = Vec::new();
            [self.records, self.data_blocks, self.max_payload]
                .into_iter()
                .for_each(|v| put_varint(&mut out, v));
            put_refs(&mut out, &self.indexes);
            put_varint(&mut out, self.last_key.len() as u64);
            out.extend_from_slice(&self.last_key);
            out
        }

        pub(crate) fn decode_bounded(bytes: &[u8], max: u64) -> Result<Self> {
            let mut at = 0;
            let (records, data_blocks, max_payload) = (
                get_varint(bytes, &mut at)?,
                get_varint(bytes, &mut at)?,
                get_varint(bytes, &mut at)?,
            );
            let (indexes, used) = get_refs_bounded(bytes, &mut at, max)?;
            let last = get_bytes(bytes, &mut at, MAX_RUN_KEY_BYTES)?;
            if at != bytes.len() {
                return Err(corrupt("trailing bytes in top block"));
            }
            let mut last_key = try_vec(last.len())?;
            admit(used, last_key.capacity(), max)?;
            last_key.extend_from_slice(last);
            Ok(Self {
                records,
                data_blocks,
                max_payload,
                indexes,
                last_key,
            })
        }
    }
}

pub(crate) mod codec {
    use crate::disk_run::{Found, MAX_RUN_KEY_BYTES, MAX_RUN_VALUE_BYTES};
    use crate::{Error, Result};

    pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    pub(crate) fn varint_len(value: u64) -> usize {
        ((64 - value.leading_zeros().min(63)) as usize).div_ceil(7)
    }

    pub(crate) fn get_varint(bytes: &[u8], at: &mut usize) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *bytes.get(*at).ok_or_else(|| corrupt("varint eof"))?;
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(corrupt("varint overflow"))
    }

    pub(crate) fn get_len(bytes: &[u8], at: &mut usize, limit: usize) -> Result<usize> {
        usize::try_from(get_varint(bytes, at)?)
            .ok()
            .filter(|&n| n <= limit)
            .ok_or_else(|| corrupt("length limit"))
    }

    pub(crate) fn get_slice<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
        let end = at
            .checked_add(len)
            .filter(|&e| e <= bytes.len())
            .ok_or_else(|| corrupt("slice eof"))?;
        let slice = &bytes[*at..end];
        *at = end;
        Ok(slice)
    }

    pub(crate) fn get_bytes<'a>(bytes: &'a [u8], at: &mut usize, limit: usize) -> Result<&'a [u8]> {
        let len = get_len(bytes, at, limit)?;
        get_slice(bytes, at, len)
    }

    fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b).take_while(|(x, y)| x == y).count()
    }

    pub(crate) fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
        let shared = shared_prefix(prev, key);
        put_varint(out, shared as u64);
        put_varint(out, (key.len() - shared) as u64);
        out.extend_from_slice(&key[shared..]);
    }

    pub(crate) fn record_len(prev: &[u8], key: &[u8], value: Option<&[u8]>) -> usize {
        let (shared, vlen) = (shared_prefix(prev, key), value.map_or(0, <[u8]>::len));
        let (suffix, tag) = (key.len() - shared, value.map_or(0, |v| v.len() as u64 + 1));
        varint_len(shared as u64) + varint_len(suffix as u64) + suffix + varint_len(tag) + vlen
    }

    pub(crate) fn put_record(out: &mut Vec<u8>, prev: &[u8], key: &[u8], value: Option<&[u8]>) {
        put_key(out, prev, key);
        put_varint(out, value.map_or(0, |v| v.len() as u64 + 1));
        if let Some(v) = value {
            out.extend_from_slice(v);
        }
    }

    pub(crate) type BlockRecords = Vec<(Vec<u8>, Found)>;

    pub(crate) fn decode_block(payload: &[u8]) -> Result<BlockRecords> {
        let (mut records, mut at, mut key) = (Vec::new(), 0, Vec::new());
        while at < payload.len() {
            let shared = get_len(payload, &mut at, key.len())?;
            let suffix = get_bytes(payload, &mut at, MAX_RUN_KEY_BYTES - shared)?;
            key.truncate(shared);
            key.extend_from_slice(suffix);
            if !records
                .last()
                .map_or(!key.is_empty(), |(b, _): &(Vec<u8>, Found)| *b < key)
            {
                return Err(corrupt("block keys not ascending"));
            }
            let found = match get_len(payload, &mut at, MAX_RUN_VALUE_BYTES + 1)?.checked_sub(1) {
                Some(len) => Found::Value(get_slice(payload, &mut at, len)?.to_vec()),
                None => Found::Tombstone,
            };
            records.push((key.clone(), found));
        }
        Ok(records)
    }

    pub(crate) fn corrupt(what: &str) -> Error {
        Error::Corrupt(format!("disk run: {what}"))
    }
}
