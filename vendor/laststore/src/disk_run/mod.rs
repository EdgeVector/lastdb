//! Disk format v2 run files: one immutable, sorted run of records.

pub(crate) mod footer;
pub mod home;
mod merge;
mod reader;
mod reader_open;
mod writer;

pub(crate) use merge::{merge_runs, MergeIter, MergeSource, RunSequence};
pub(crate) use reader::{MetadataMemo, RunReader};
pub(crate) use reader_open::SummaryCursor;
pub(crate) use writer::{PreparedRun, RunSummary, RunWriter};

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

pub(crate) mod bloom {
    use super::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
    use crate::Result;

    const PROBES: u64 = 7;
    const MAX_FILTER_BYTES: usize = 64 * 1024 * 1024;

    pub(crate) fn key_hash(key: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &byte in key {
            h = (h ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
        mix(h)
    }

    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn probes(hash: u64, bits: u64) -> impl Iterator<Item = u64> {
        let step = mix(hash ^ 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PROBES).map(move |i| hash.wrapping_add(i.wrapping_mul(step)) % bits)
    }

    pub(crate) fn put_filter(out: &mut Vec<u8>, hashes: &[u64], bits_per_key: u32) {
        let bytes = if bits_per_key == 0 || hashes.is_empty() {
            0
        } else {
            (hashes.len() * bits_per_key as usize)
                .div_ceil(8)
                .clamp(8, MAX_FILTER_BYTES)
        };
        put_varint(out, bytes as u64);
        put_varint(out, PROBES);
        let start = out.len();
        out.resize(start + bytes, 0);
        if bytes > 0 {
            let (bits, filter) = ((bytes * 8) as u64, &mut out[start..]);
            for &hash in hashes {
                for bit in probes(hash, bits) {
                    filter[(bit / 8) as usize] |= 1 << (bit % 8);
                }
            }
        }
    }

    pub(crate) struct Filter<'a>(&'a [u8]);

    impl<'a> Filter<'a> {
        pub(crate) fn read(bytes: &'a [u8], at: &mut usize) -> Result<Self> {
            let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
            if get_varint(bytes, at)? != PROBES {
                return Err(corrupt("unknown bloom probe count"));
            }
            Ok(Self(get_slice(bytes, at, len)?))
        }

        pub(crate) fn may_hold(&self, hash: u64) -> bool {
            self.0.is_empty()
                || probes(hash, (self.0.len() * 8) as u64)
                    .all(|b| self.0[(b / 8) as usize] & (1 << (b % 8)) != 0)
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
            let byte = *bytes
                .get(*at)
                .ok_or_else(|| corrupt("varint runs past the block"))?;
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(corrupt("varint longer than 64 bits"))
    }

    pub(crate) fn get_len(bytes: &[u8], at: &mut usize, limit: usize) -> Result<usize> {
        usize::try_from(get_varint(bytes, at)?)
            .ok()
            .filter(|&n| n <= limit)
            .ok_or_else(|| corrupt("length over its limit"))
    }

    pub(crate) fn get_slice<'a>(bytes: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
        let end = at
            .checked_add(len)
            .filter(|&e| e <= bytes.len())
            .ok_or_else(|| corrupt("field runs past the block"))?;
        let slice = &bytes[*at..end];
        *at = end;
        Ok(slice)
    }

    pub(crate) fn put_key(out: &mut Vec<u8>, prev: &[u8], key: &[u8]) {
        let shared = prev.iter().zip(key).take_while(|(x, y)| x == y).count();
        put_varint(out, shared as u64);
        put_varint(out, (key.len() - shared) as u64);
        out.extend_from_slice(&key[shared..]);
    }

    pub(crate) fn record_len(prev: &[u8], key: &[u8], value: Option<&[u8]>) -> usize {
        let shared = prev.iter().zip(key).take_while(|(x, y)| x == y).count();
        let (suffix, vlen) = (key.len() - shared, value.map_or(0, <[u8]>::len));
        varint_len(shared as u64)
            + varint_len(suffix as u64)
            + suffix
            + varint_len(value.map_or(0, |v| v.len() as u64 + 1))
            + vlen
    }

    pub(crate) fn put_record(out: &mut Vec<u8>, prev: &[u8], key: &[u8], value: Option<&[u8]>) {
        put_key(out, prev, key);
        match value {
            None => put_varint(out, 0),
            Some(v) => {
                put_varint(out, v.len() as u64 + 1);
                out.extend_from_slice(v);
            }
        }
    }

    pub(crate) type BlockRecords = Vec<(Vec<u8>, Found)>;

    pub(crate) fn decode_block(payload: &[u8]) -> Result<BlockRecords> {
        let (mut records, mut at, mut key) = (Vec::new(), 0, Vec::new());
        while at < payload.len() {
            let shared = get_len(payload, &mut at, key.len())?;
            let suffix_len = get_len(payload, &mut at, MAX_RUN_KEY_BYTES - shared)?;
            let suffix = get_slice(payload, &mut at, suffix_len)?;
            key.truncate(shared);
            key.extend_from_slice(suffix);
            if !records
                .last()
                .map_or(!key.is_empty(), |(b, _): &(Vec<u8>, Found)| *b < key)
            {
                return Err(corrupt("block keys are not strictly ascending"));
            }
            let found = match get_len(payload, &mut at, MAX_RUN_VALUE_BYTES + 1)?.checked_sub(1) {
                None => Found::Tombstone,
                Some(len) => Found::Value(get_slice(payload, &mut at, len)?.to_vec()),
            };
            records.push((key.clone(), found));
        }
        Ok(records)
    }

    pub(crate) fn corrupt(what: &str) -> Error {
        Error::Corrupt(format!("disk run: {what}"))
    }
}
