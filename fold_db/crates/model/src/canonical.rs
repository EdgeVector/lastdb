//! Unambiguous canonical byte framing (`u32` big-endian length prefixes) and
//! deterministic JSON object serialization for hashing and signing.

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use sha2::Sha256;
use std::collections::BTreeMap;

/// Borrowed JSON value wrapper that serializes nested objects in sorted key order.
pub struct CanonicalValue<'a>(pub &'a Value);

impl Serialize for CanonicalValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(values) => {
                let sorted: BTreeMap<_, _> = values.iter().collect();
                let mut map = serializer.serialize_map(Some(sorted.len()))?;
                for (name, value) in sorted {
                    map.serialize_entry(name, &Self(value))?;
                }
                map.end()
            }
            Value::Array(values) => serializer.collect_seq(values.iter().map(Self)),
            value => value.serialize(serializer),
        }
    }
}

/// Sink that canonical framing can be written into (`Vec<u8>` or [`Sha256`]).
pub trait ByteSink {
    /// Append raw bytes to the sink.
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
        use sha2::Digest;
        self.update(bytes);
    }
}

/// Write `field` into `sink` preceded by its length as a 4-byte big-endian `u32`.
#[inline]
pub fn push_field<S: ByteSink>(sink: &mut S, field: &[u8]) {
    let len: u32 = field
        .len()
        .try_into()
        .expect("canonical field exceeds 4 GiB");
    sink.put(&len.to_be_bytes());
    sink.put(field);
}

/// Builder for assembling canonical framed bytes into a `Vec<u8>`.
#[derive(Debug, Default, Clone)]
pub struct CanonicalWriter {
    buf: Vec<u8>,
}

impl CanonicalWriter {
    #[must_use]
    pub fn new() -> Self {
        Self { buf: Vec::new() }
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
