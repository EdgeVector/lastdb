//! Per-index-block bloom filters for fast point-read pruning.

use crate::disk_run::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
use crate::Result;

const PROBES: u64 = 7;
const MAX_FILTER_BYTES: usize = 64 << 20;

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub(crate) fn key_hash(key: &[u8]) -> u64 {
    mix(key.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    }))
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

pub(crate) fn read_filter<'a>(bytes: &'a [u8], at: &mut usize) -> Result<&'a [u8]> {
    let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
    (get_varint(bytes, at)? == PROBES)
        .then(|| get_slice(bytes, at, len))
        .ok_or_else(|| corrupt("bloom probes"))?
}

pub(crate) fn may_hold(filter: &[u8], hash: u64) -> bool {
    filter.is_empty()
        || probes(hash, (filter.len() * 8) as u64)
            .all(|bit| filter[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
}
