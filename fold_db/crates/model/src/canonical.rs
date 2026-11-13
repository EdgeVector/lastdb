//! Canonical `u32` big-endian length-prefixed framing and deterministic JSON serialization.

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

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
