//! Disk format v2 run files: one immutable, sorted run of records.

pub mod home;
mod merge;
mod reader;
mod writer;

pub(crate) use merge::{count_merged_keys, merge_runs, Block, MergeIter, MetadataMemo, RunRange};
pub(crate) use reader::RunReader;
pub(crate) use writer::{MergeSpace, PreparedRun, RunWriter};

use crate::bloom;
use crate::{Error, Result};
use uuid::Uuid;

pub(crate) const RUN_SUFFIX: &str = ".ldrun";

/// Largest stored key. Larger keys are refused at write time.
pub const MAX_RUN_KEY_BYTES: usize = 64 * 1024;
/// Largest stored value payload in a single run record.
pub const MAX_RUN_VALUE_BYTES: usize = crate::frame::MAX_DECOMPRESSED_FRAME_BYTES / 2;

pub(crate) const MAX_TOP_BYTES: usize = crate::frame::MAX_DECOMPRESSED_FRAME_BYTES;
pub(crate) const MAX_INDEX_ENTRIES: usize = 1 << 24;
pub(crate) const INDEX_COUNTER_BASE: u64 = 1 << 62;
pub(crate) const TOP_COUNTER: u64 = 1 << 63;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Found {
    Value(Vec<u8>),
    Tombstone,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunMeta(
    pub(crate) Uuid,
    pub(crate) [u64; 5],
    pub(crate) Vec<u8>,
    pub(crate) Vec<u8>,
);

impl RunMeta {
    pub(crate) fn note_record(&mut self, key: &[u8], body: Option<u64>) -> Result<()> {
        let add = |a: u64, b: u64| a.checked_add(b).ok_or(Error::ReadBudgetExceeded);
        let klen = key.len() as u64;
        self.1[0] = add(self.1[0], 1)?;
        self.1[2] = add(self.1[2], add(klen, body.unwrap_or(0))?)?;
        if body.is_none() {
            (self.1[1], self.1[3]) = (self.1[1] + 1, self.1[3] + klen);
        }
        if self.1[0] == 1 {
            self.2 = key.to_vec();
        }
        self.3.clear();
        self.3.extend_from_slice(key);
        Ok(())
    }

    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.2.as_slice() <= key && key <= self.3.as_slice()
    }
    pub(crate) fn overlaps(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        self.3.as_slice() >= lower && upper.is_none_or(|u| self.2.as_slice() < u)
    }
}

pub(crate) struct IndexBlock(pub(crate) Vec<footer::BlockRef>, Vec<u8>);

impl IndexBlock {
    pub(crate) fn parse(payload: Vec<u8>, first_key: &[u8], avail: Option<u64>) -> Result<Self> {
        let mut at = 0;
        let (blocks, _) = footer::get_refs_bounded(&payload, &mut at, avail.unwrap_or(u64::MAX))?;
        if blocks.first().map(|b| b.0.as_slice()) != Some(first_key) {
            return Err(codec::corrupt("index block first key mismatch"));
        }
        let filter = bloom::read_filter(&payload, &mut at)?;
        if at != payload.len() {
            return Err(codec::corrupt("trailing bytes in index block"));
        }
        let fp = footer::refs_footprint(&blocks, blocks.capacity()) + filter.len();
        if avail.is_some_and(|lim| (payload.capacity() + fp) as u64 > lim) {
            return Err(Error::ReadBudgetExceeded);
        }
        Ok(Self(blocks, filter.to_vec()))
    }

    pub(crate) fn block_pos(&self, key: &[u8]) -> Option<usize> {
        self.0
            .partition_point(|b| b.0.as_slice() <= key)
            .checked_sub(1)
    }
    pub(crate) fn locate(&self, key: &[u8]) -> Option<&footer::BlockRef> {
        bloom::may_hold(&self.1, bloom::key_hash(key))
            .then(|| self.block_pos(key).map(|i| &self.0[i]))
            .flatten()
    }
    pub(crate) fn footprint(&self) -> u64 {
        (footer::refs_footprint(&self.0, self.0.capacity()) + self.1.capacity()) as u64
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
    pub(crate) type Span = (u64, u32, u64);

    pub(crate) fn try_vec<T>(count: usize) -> Result<Vec<T>> {
        let mut v = Vec::new();
        v.try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        Ok(v)
    }

    pub(crate) fn refs_footprint(refs: &[BlockRef], cap: usize) -> usize {
        cap * std::mem::size_of::<BlockRef>() + refs.iter().map(|r| r.0.capacity()).sum::<usize>()
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Footer(
        pub(crate) Uuid,
        pub(crate) u64,
        pub(crate) u32,
        pub(crate) u64,
        pub(crate) u32,
        pub(crate) u32,
    );

    impl Footer {
        pub(crate) fn encode(&self) -> [u8; FOOTER_BYTES] {
            let mut out = [0u8; FOOTER_BYTES];
            out[0..8].copy_from_slice(&MAGIC);
            out[8..12].copy_from_slice(&VERSION.to_le_bytes());
            out[12..28].copy_from_slice(self.0.as_bytes());
            out[28..36].copy_from_slice(&self.1.to_le_bytes());
            out[36..40].copy_from_slice(&self.2.to_le_bytes());
            out[40..48].copy_from_slice(&self.3.to_le_bytes());
            out[48..52].copy_from_slice(&self.4.to_le_bytes());
            out[52..56].copy_from_slice(&self.5.to_le_bytes());
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
            let id = Uuid::from_slice(&b[12..28]).unwrap();
            let f = Self(
                id,
                u64_at(28),
                u32_at(36),
                u64_at(40),
                u32_at(48),
                u32_at(52),
            );
            (u32_at(8) == VERSION)
                .then_some(f)
                .ok_or_else(|| corrupt("unknown run version"))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct BlockRef(
        pub(crate) Vec<u8>,
        pub(crate) u64,
        pub(crate) u64,
        pub(crate) u32,
    );

    impl BlockRef {
        pub(crate) fn span(&self) -> Span {
            (self.2, self.3, self.1)
        }
    }

    pub(crate) fn put_refs(out: &mut Vec<u8>, refs: &[BlockRef]) {
        put_varint(out, refs.len() as u64);
        let mut prev: &[u8] = &[];
        for b in refs {
            put_key(out, prev, &b.0);
            [b.1, b.2, u64::from(b.3)]
                .into_iter()
                .for_each(|v| put_varint(out, v));
            prev = &b.0;
        }
    }

    fn admit(used: u64, bytes: usize, max: u64) -> Result<u64> {
        used.checked_add(bytes as u64)
            .filter(|&v| v <= max)
            .ok_or(Error::ReadBudgetExceeded)
    }

    pub(crate) fn get_refs_bounded(
        b: &[u8],
        at: &mut usize,
        max: u64,
    ) -> Result<(Vec<BlockRef>, u64)> {
        let mut refs: Vec<BlockRef> = try_vec(get_len(b, at, MAX_INDEX_ENTRIES)?)?;
        let count = refs.capacity();
        let sz = count
            .checked_mul(std::mem::size_of::<BlockRef>())
            .ok_or(Error::ReadBudgetExceeded)?;
        let mut used = admit(admit(0, b.len(), max)?, sz, max)?;
        for _ in 0..count {
            let prev = refs.last().map_or(&[][..], |e| e.0.as_slice());
            let shared = get_len(b, at, prev.len())?;
            let suffix = get_bytes(b, at, MAX_RUN_KEY_BYTES - shared)?;
            let (number, offset) = (get_varint(b, at)?, get_varint(b, at)?);
            let len = u32::try_from(get_varint(b, at)?).map_err(|_| corrupt("block length"))?;
            let mut first_key = try_vec(shared + suffix.len())?;
            used = admit(used, first_key.capacity(), max)?;
            first_key.extend_from_slice(&prev[..shared]);
            first_key.extend_from_slice(suffix);
            if refs.last().is_some_and(|p| p.0 >= first_key) {
                return Err(corrupt("block entries are not ascending"));
            }
            refs.push(BlockRef(first_key, number, offset, len));
        }
        Ok((refs, used))
    }

    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub(crate) struct Top(
        pub(crate) (u64, u64, u64),
        pub(crate) Vec<BlockRef>,
        pub(crate) Vec<u8>,
    );

    impl Top {
        pub(crate) fn encode(&self) -> Vec<u8> {
            let mut out = Vec::new();
            [self.0 .0, self.0 .1, self.0 .2]
                .into_iter()
                .for_each(|v| put_varint(&mut out, v));
            put_refs(&mut out, &self.1);
            put_varint(&mut out, self.2.len() as u64);
            out.extend_from_slice(&self.2);
            out
        }

        pub(crate) fn decode_bounded(bytes: &[u8], max: u64) -> Result<Self> {
            let mut at = 0;
            let counts = (
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
            Ok(Self(counts, indexes, last_key))
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

    fn varint_len(value: u64) -> usize {
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
        let n = usize::try_from(get_varint(bytes, at)?).ok();
        n.filter(|&n| n <= limit)
            .ok_or_else(|| corrupt("length limit"))
    }

    pub(crate) fn get_slice<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
        let end = at
            .checked_add(len)
            .filter(|&e| e <= bytes.len())
            .ok_or_else(|| corrupt("eof"))?;
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

    pub(super) fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
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
        let (mut records, mut at, mut key): (BlockRecords, usize, Vec<u8>) =
            (Vec::new(), 0, Vec::new());
        while at < payload.len() {
            let shared = get_len(payload, &mut at, key.len())?;
            let suffix = get_bytes(payload, &mut at, MAX_RUN_KEY_BYTES - shared)?;
            key.truncate(shared);
            key.extend_from_slice(suffix);
            if key.is_empty() || records.last().is_some_and(|(b, _)| *b >= key) {
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
