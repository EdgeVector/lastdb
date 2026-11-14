//! Length-prefixed binary framing, sorted-key JSON serialization, and hex digests.

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub use crate::keys::{key_value, kind_partition, QueryCursor, SortOrder};
pub use crate::limits::{content_limit, storage_policy};

pub struct SortedKeyJsonValue<'a> {
    pub value: &'a Value,
}

impl<'a> SortedKeyJsonValue<'a> {
    #[must_use]
    pub const fn new(value: &'a Value) -> Self {
        Self { value }
    }
}

pub type CanonicalValue<'a> = SortedKeyJsonValue<'a>;

impl Serialize for SortedKeyJsonValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.value {
            Value::Object(values) => {
                let sorted: BTreeMap<_, _> = values.iter().collect();
                let mut map = serializer.serialize_map(Some(sorted.len()))?;
                for (name, value) in sorted {
                    map.serialize_entry(name, &Self::new(value))?;
                }
                map.end()
            }
            Value::Array(values) => serializer.collect_seq(values.iter().map(Self::new)),
            value => value.serialize(serializer),
        }
    }
}

pub trait ByteSink {
    fn put(&mut self, bytes: &[u8]);
}

impl ByteSink for Vec<u8> {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

impl ByteSink for Sha256 {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.update(bytes);
    }
}

#[inline]
pub fn push_field<S: ByteSink>(sink: &mut S, field: &[u8]) {
    let len = u32::try_from(field.len()).expect("canonical field exceeds 4 GiB");
    sink.put(&len.to_be_bytes());
    sink.put(field);
}

#[derive(Debug, Default, Clone)]
pub struct CanonicalWriter {
    buf: Vec<u8>,
}

impl CanonicalWriter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    #[must_use]
    pub fn field(mut self, field: &[u8]) -> Self {
        push_field(&mut self.buf, field);
        self
    }

    #[must_use]
    pub fn u64(mut self, value: u64) -> Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

pub mod hex {
    use sha2::{Digest, Sha256};

    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    pub fn hex_lower(bytes: impl AsRef<[u8]>) -> String {
        let bytes = bytes.as_ref();
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(DIGITS[(b >> 4) as usize] as char);
            out.push(DIGITS[(b & 0x0f) as usize] as char);
        }
        out
    }

    fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }

    pub fn hex_decode_array<const N: usize>(text: &str) -> Option<[u8; N]> {
        let bytes = text.as_bytes();
        if bytes.len() != N * 2 {
            return None;
        }
        let mut out = [0u8; N];
        for (slot, pair) in out.iter_mut().zip(bytes.chunks_exact(2)) {
            *slot = (nibble(pair[0])? << 4) | nibble(pair[1])?;
        }
        Some(out)
    }

    pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
        hex_lower(Sha256::digest(bytes.as_ref()))
    }
}
