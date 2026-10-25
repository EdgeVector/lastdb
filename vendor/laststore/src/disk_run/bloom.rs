//! Bloom filter for the keys under one index block.
//!
//! The hash is FNV-1a 64 with a SplitMix64 finish. It is fixed here, not
//! taken from `std`, because the bits are stored on disk.

use super::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
use crate::Result;

const PROBES: u32 = 7;
/// Largest stored filter: 64 MiB of bits covers about 50 million keys.
const MAX_FILTER_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn key_hash(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key {
        h ^= u64::from(*byte);
        h = h.wrapping_mul(0x0100_0000_01b3);
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
    (0..u64::from(PROBES)).map(move |i| hash.wrapping_add(i.wrapping_mul(step)) % bits)
}

/// Appends a filter over `hashes`: `varint byte_len, varint probes, bits`.
/// Zero bits per key writes an empty filter that answers "maybe" for all.
pub(super) fn put_filter(out: &mut Vec<u8>, hashes: &[u64], bits_per_key: u32) {
    let bytes = if bits_per_key == 0 || hashes.is_empty() {
        0
    } else {
        (hashes.len() * bits_per_key as usize)
            .div_ceil(8)
            .clamp(8, MAX_FILTER_BYTES)
    };
    put_varint(out, bytes as u64);
    put_varint(out, u64::from(PROBES));
    let start = out.len();
    out.resize(start + bytes, 0);
    if bytes == 0 {
        return;
    }
    let bits = (bytes * 8) as u64;
    let filter = &mut out[start..];
    for hash in hashes {
        for bit in probes(*hash, bits) {
            filter[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }
}

/// A filter read back from an index block.
pub(super) struct Filter<'a> {
    bits: &'a [u8],
}

impl<'a> Filter<'a> {
    pub(super) fn read(bytes: &'a [u8], at: &mut usize) -> Result<Self> {
        let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
        if get_varint(bytes, at)? != u64::from(PROBES) {
            return Err(corrupt("unknown bloom probe count"));
        }
        Ok(Self {
            bits: get_slice(bytes, at, len)?,
        })
    }

    /// False only when the key is certainly absent.
    pub(super) fn may_hold(&self, hash: u64) -> bool {
        if self.bits.is_empty() {
            return true;
        }
        let bits = (self.bits.len() * 8) as u64;
        probes(hash, bits).all(|bit| self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
    }
}
