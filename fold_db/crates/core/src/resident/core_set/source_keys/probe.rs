//! Compare canonical packed sources without rebuilding a temporary packed key.

use super::{read_len, HEX_DIGITS, HEX_MARK};

pub(super) fn matches(packed: &[u8], collection: &str, key: &str) -> bool {
    let (length, mut at) = read_len(packed, 0);
    if &packed[at..at + length] != collection.as_bytes() {
        return false;
    }
    at += length;
    let mut remaining = key.as_bytes();
    while at < packed.len() {
        if packed[at] == HEX_MARK {
            let (length, start) = read_len(packed, at + 1);
            let Some(width) = length.checked_mul(2) else {
                return false;
            };
            let Some(text) = remaining.get(..width) else {
                return false;
            };
            for (pair, byte) in text.chunks_exact(2).zip(&packed[start..start + length]) {
                if pair[0] != HEX_DIGITS[usize::from(byte >> 4)]
                    || pair[1] != HEX_DIGITS[usize::from(byte & 0xF)]
                {
                    return false;
                }
            }
            remaining = &remaining[width..];
            at = start + length;
        } else {
            let tail = &packed[at..];
            let length = tail
                .iter()
                .position(|byte| *byte == HEX_MARK)
                .unwrap_or(tail.len());
            if !remaining.starts_with(&tail[..length]) {
                return false;
            }
            remaining = &remaining[length..];
            at += length;
        }
    }
    remaining.is_empty()
}
