//! Disk format v2 run files: one immutable, sorted run of records.

pub(crate) mod codec;
pub mod home;
mod memo;
mod merge;
mod reader;
mod writer;

pub(crate) use memo::MetadataMemo;
pub(crate) use merge::{merge_runs, Block, MergeIter, RunRange};
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
pub(crate) struct RunMeta {
    pub(crate) id: Uuid,
    pub(crate) records: u64,
    pub(crate) tombstones: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) tombstone_key_bytes: u64,
    pub(crate) file_bytes: u64,
    pub(crate) first_key: Vec<u8>,
    pub(crate) last_key: Vec<u8>,
}

impl RunMeta {
    pub(crate) fn note_record(&mut self, key: &[u8], body: Option<u64>) -> Result<()> {
        let add = |a: u64, b: u64| a.checked_add(b).ok_or(Error::ReadBudgetExceeded);
        let key_len = key.len() as u64;
        self.records = add(self.records, 1)?;
        self.logical_bytes = add(self.logical_bytes, add(key_len, body.unwrap_or(0))?)?;
        if body.is_none() {
            self.tombstones += 1;
            self.tombstone_key_bytes += key_len;
        }
        if self.records == 1 {
            self.first_key = key.to_vec();
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        Ok(())
    }

    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.first_key.as_slice() <= key && key <= self.last_key.as_slice()
    }

    pub(crate) fn overlaps(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        self.last_key.as_slice() >= lower && upper.is_none_or(|u| self.first_key.as_slice() < u)
    }
}

pub(crate) struct IndexBlock {
    pub(crate) blocks: Vec<footer::BlockRef>,
    filter: Vec<u8>,
}

impl IndexBlock {
    pub(crate) fn parse(payload: Vec<u8>, first_key: &[u8], avail: Option<u64>) -> Result<Self> {
        let mut at = 0;
        let (blocks, _) = footer::get_refs_bounded(&payload, &mut at, avail.unwrap_or(u64::MAX))?;
        if blocks.first().map(|b| b.first_key.as_slice()) != Some(first_key) {
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
        Ok(Self {
            blocks,
            filter: filter.to_vec(),
        })
    }

    pub(crate) fn block_pos(&self, key: &[u8]) -> Option<usize> {
        self.blocks
            .partition_point(|b| b.first_key.as_slice() <= key)
            .checked_sub(1)
    }

    pub(crate) fn locate(&self, key: &[u8]) -> Option<&footer::BlockRef> {
        bloom::may_hold(&self.filter, bloom::key_hash(key))
            .then(|| self.block_pos(key).map(|i| &self.blocks[i]))
            .flatten()
    }

    pub(crate) fn footprint(&self) -> u64 {
        (footer::refs_footprint(&self.blocks, self.blocks.capacity()) + self.filter.capacity())
            as u64
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
        let mut out = Vec::new();
        out.try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        Ok(out)
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
        pub(crate) max_payload_bytes: u32,
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
            out[48..52].copy_from_slice(&self.max_payload_bytes.to_le_bytes());
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
            let footer = Self {
                run_id: Uuid::from_slice(&b[12..28]).unwrap(),
                top_offset: u64_at(28),
                top_len: u32_at(36),
                records: u64_at(40),
                max_payload_bytes: u32_at(48),
                data_blocks: u32_at(52),
            };
            (u32_at(8) == VERSION)
                .then_some(footer)
                .ok_or_else(|| corrupt("unknown run version"))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct BlockRef {
        pub(crate) first_key: Vec<u8>,
        pub(crate) number: u64,
        pub(crate) offset: u64,
        pub(crate) len: u32,
    }

    impl BlockRef {
        pub(crate) fn span(&self) -> Span {
            (self.offset, self.len, self.number)
        }
    }

    pub(crate) fn put_refs(out: &mut Vec<u8>, refs: &[BlockRef]) {
        put_varint(out, refs.len() as u64);
        let mut prev: &[u8] = &[];
        for block in refs {
            put_key(out, prev, &block.first_key);
            for val in [block.number, block.offset, u64::from(block.len)] {
                put_varint(out, val);
            }
            prev = &block.first_key;
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
            let prev = refs.last().map_or(&[][..], |e| e.first_key.as_slice());
            let shared = get_len(b, at, prev.len())?;
            let suffix = get_bytes(b, at, MAX_RUN_KEY_BYTES - shared)?;
            let (number, offset) = (get_varint(b, at)?, get_varint(b, at)?);
            let len = u32::try_from(get_varint(b, at)?).map_err(|_| corrupt("block length"))?;
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
        pub(crate) max_payload_bytes: u64,
        pub(crate) indexes: Vec<BlockRef>,
        pub(crate) last_key: Vec<u8>,
    }

    impl Top {
        pub(crate) fn encode(&self) -> Vec<u8> {
            let mut out = Vec::new();
            for val in [self.records, self.data_blocks, self.max_payload_bytes] {
                put_varint(&mut out, val);
            }
            put_refs(&mut out, &self.indexes);
            put_varint(&mut out, self.last_key.len() as u64);
            out.extend_from_slice(&self.last_key);
            out
        }

        pub(crate) fn decode_bounded(bytes: &[u8], max: u64) -> Result<Self> {
            let mut at = 0;
            let records = get_varint(bytes, &mut at)?;
            let data_blocks = get_varint(bytes, &mut at)?;
            let max_payload_bytes = get_varint(bytes, &mut at)?;
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
                max_payload_bytes,
                indexes,
                last_key,
            })
        }
    }
}
