//! Canonical framing, hex digests, prefix bounds, key coordinates, cursors, and resource bounds.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
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

pub mod content_limit {
    pub const DEFAULT_MAX_FIELD_VALUE_BYTES: usize = 256 * 1024;
    pub const ABSOLUTE_MAX_FIELD_VALUE_BYTES: usize = 1024 * 1024;
    pub const MIN_MAX_FIELD_VALUE_BYTES: usize = 1024;
    pub const MAX_FIELD_VALUE_BYTES_ENV: &str = "LASTDB_MAX_FIELD_VALUE_BYTES";
    pub const RECORD_TOO_LARGE_HINT: &str = "A record holds every field of one key, and its \
        encoded value must fit the 64 KiB record cap. Store large bytes as the record's one file \
        (`put-blob`), or write a list that grows as one record per entry in a HashRange schema \
        (hash = partition, range = entry key).";
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

pub mod kind_partition {
    #[must_use]
    pub fn anchored(kind: &str, rest: &str) -> String {
        format!("{kind}\0{rest}")
    }

    #[must_use]
    pub fn exact_prefix_bounds(prefix: &str) -> (String, String) {
        let mut bytes = prefix.as_bytes().to_vec();
        match bytes.last_mut() {
            Some(last) if *last < 0xff => *last += 1,
            Some(_) => bytes.push(0),
            None => {}
        }
        let end = String::from_utf8(bytes)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        (prefix.to_owned(), end)
    }
}

pub mod storage_policy {
    pub const BACKUP_FILES_MAX: usize = 65_552;

    #[derive(Clone, Copy, Debug)]
    pub struct BackupResources {
        pub working_memory_bytes: u64,
        pub decoded_values_bytes: u64,
        pub stream_buffer_bytes: u64,
        pub concurrent_streams: usize,
        pub durable_spool_bytes: u64,
    }

    pub const BACKUP_RESOURCES: BackupResources = BackupResources {
        working_memory_bytes: 96 * 1024 * 1024,
        decoded_values_bytes: 64 * 1024 * 1024,
        stream_buffer_bytes: 1024 * 1024,
        concurrent_streams: 4,
        durable_spool_bytes: 1024 * 1024 * 1024,
    };

    pub mod native_read {
        pub const EAGER_WORK_BYTES: u64 = 4 * super::native_write::PENDING_MEMORY_BYTES;
        pub const EAGER_BLOCKS: u64 = super::native_write::PENDING_MEMORY_BYTES.div_ceil(4_096) + 1;
    }

    pub mod native_write {
        pub const PENDING_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
        pub const LOCAL_FILE_INPUT_MEMORY_BYTES: u64 = PENDING_MEMORY_BYTES;
    }

    pub mod backup_decode {
        pub const RECORD_BODY_MAX_BYTES: u64 = 4 * 1024 * 1024;
        pub const DIRECTORY_READ_BYTES: u64 = 8 * 1024 * 1024;
        pub const RECORD_INVENTORY_READ_BYTES: u64 = 64 * 1024 * 1024;
        pub const RECORD_INVENTORY_PAGES: u64 = 4096;
        pub const NATIVE_STREAM_LOOKUP_READ_BYTES: u64 = 128 * 1024 * 1024;
        pub const NATIVE_STREAM_BLOCKS: u64 = 16_384;
        pub const JSON_PAGE_RESERVE_BYTES: u64 = 4 * 1024 * 1024;
        pub const RESTORE_SPOOL_RESERVE_BYTES: u64 = 16 * 1024 * 1024;
        pub const RECEIPT_MEMORY_BYTES: u64 = 512;
        pub const IDENTITY_MEMORY_BYTES: u64 = 192;
    }

    const _: () = assert!(
        BACKUP_RESOURCES.stream_buffer_bytes > 0
            && BACKUP_RESOURCES.stream_buffer_bytes <= (i32::MAX - 16) as u64
            && BACKUP_RESOURCES.stream_buffer_bytes <= (usize::MAX - 16) as u64
    );
    const _: () = assert!(BACKUP_RESOURCES.concurrent_streams > 0);
}

pub mod key_value {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
    #[serde(deny_unknown_fields)]
    pub struct KeyValue {
        pub hash: Option<String>,
        pub range: Option<String>,
    }

    impl KeyValue {
        pub fn new(hash: Option<String>, range: Option<String>) -> Self {
            Self { hash, range }
        }

        #[must_use]
        pub fn to_storage_key(&self) -> String {
            serde_json::to_string(self).expect("KeyValue is always JSON-serializable")
        }

        #[must_use]
        pub fn cmp_page_order_with(
            &self,
            other: &Self,
            cmp_ranges: impl FnOnce(&str, &str) -> std::cmp::Ordering,
        ) -> std::cmp::Ordering {
            cmp_ranges(
                self.range.as_deref().unwrap_or(""),
                other.range.as_deref().unwrap_or(""),
            )
            .then_with(|| {
                self.hash
                    .as_deref()
                    .unwrap_or("")
                    .cmp(other.hash.as_deref().unwrap_or(""))
            })
        }
    }

    impl std::fmt::Display for KeyValue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match (&self.hash, &self.range) {
                (Some(h), Some(r)) => write!(f, "{h}:{r}"),
                (Some(s), None) | (None, Some(s)) => f.write_str(s),
                (None, None) => Ok(()),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryCursor {
    pub hash: Option<String>,
    pub range: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate_primary: Option<key_value::KeyValue>,
}

impl QueryCursor {
    #[must_use]
    pub fn new(key: key_value::KeyValue, comparison: Option<String>) -> Self {
        Self {
            hash: key.hash,
            range: key.range,
            comparison,
            record: None,
            alternate_primary: None,
        }
    }

    #[must_use]
    pub fn at_record(suffix: &str, comparison: Option<String>) -> Self {
        Self {
            hash: None,
            range: None,
            comparison,
            record: Some(URL_SAFE_NO_PAD.encode(suffix)),
            alternate_primary: None,
        }
    }

    pub fn record_position(&self) -> Result<Option<String>, String> {
        self.record
            .as_deref()
            .map(|r| {
                let b = URL_SAFE_NO_PAD
                    .decode(r)
                    .map_err(|_| "the record cursor is not base64url".to_owned())?;
                String::from_utf8(b).map_err(|_| "the record cursor is not a record key".to_owned())
            })
            .transpose()
    }

    #[must_use]
    pub fn into_key(self) -> key_value::KeyValue {
        key_value::KeyValue::new(self.hash, self.range)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl Serialize for SortOrder {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        })
    }
}

impl<'de> Deserialize<'de> for SortOrder {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s.eq_ignore_ascii_case("asc") {
            Ok(Self::Asc)
        } else if s.eq_ignore_ascii_case("desc") {
            Ok(Self::Desc)
        } else {
            Err(serde::de::Error::custom(format!(
                "unknown sort order '{s}', expected 'asc' or 'desc'"
            )))
        }
    }
}
