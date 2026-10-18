//! Lossless in-memory range tokens with the original storage-byte order.

use crate::atom::RangeKeyEncoding;
use std::cmp::Ordering;
use std::sync::Arc;

/// OPE words occupy four bytes instead of eight lowercase hexadecimal bytes.
/// Plain and unrecognized forms retain every original UTF-8 byte.
#[derive(Clone, Debug)]
pub(crate) struct CompactRangeKey {
    bytes: Arc<[u8]>,
    hexadecimal: bool,
}

impl CompactRangeKey {
    pub(crate) fn from_storage(value: &str, encoding: RangeKeyEncoding) -> Self {
        let hexadecimal = compact_supported(value, encoding);
        let bytes: Arc<[u8]> = if hexadecimal {
            value
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
                .collect::<Vec<_>>()
                .into()
        } else {
            value.as_bytes().into()
        };
        Self { bytes, hexadecimal }
    }

    /// Includes Arc counters, allocator alignment, and allocator bookkeeping.
    pub(crate) fn allocation_bound(value: &str, encoding: RangeKeyEncoding) -> usize {
        let length = if compact_supported(value, encoding) {
            value.len() / 2
        } else {
            value.len()
        };
        length.saturating_add(63) & !15
    }

    pub(crate) fn to_storage(&self) -> String {
        if self.hexadecimal {
            let mut output = String::with_capacity(self.bytes.len() * 2);
            for byte in self.storage_bytes() {
                output.push(char::from(byte));
            }
            output
        } else {
            // Construction accepts only str input, and no method changes bytes.
            String::from_utf8_lossy(&self.bytes).into_owned()
        }
    }

    pub(crate) fn starts_with(&self, prefix: &str) -> bool {
        self.storage_bytes().take(prefix.len()).eq(prefix.bytes())
    }

    fn storage_bytes(&self) -> impl Iterator<Item = u8> + '_ {
        let length = self.bytes.len() * if self.hexadecimal { 2 } else { 1 };
        (0..length).map(|position| {
            if self.hexadecimal {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let byte = self.bytes[position / 2];
                HEX[usize::from(if position % 2 == 0 {
                    byte >> 4
                } else {
                    byte & 15
                })]
            } else {
                self.bytes[position]
            }
        })
    }
}

impl Ord for CompactRangeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.hexadecimal == other.hexadecimal {
            self.bytes.cmp(&other.bytes)
        } else {
            self.storage_bytes().cmp(other.storage_bytes())
        }
    }
}

impl PartialOrd for CompactRangeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CompactRangeKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for CompactRangeKey {}

fn compact_supported(value: &str, encoding: RangeKeyEncoding) -> bool {
    encoding == RangeKeyEncoding::OpeV1
        && value.len().is_multiple_of(8)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn nibble(byte: u8) -> u8 {
    if byte.is_ascii_digit() {
        byte - b'0'
    } else {
        byte - b'a' + 10
    }
}
