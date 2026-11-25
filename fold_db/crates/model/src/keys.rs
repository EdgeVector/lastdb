//! Key coordinates, prefix partition bounds, resume cursors, sort order, canonical framing, and resource bounds.

use serde::{Deserialize, Serialize, Serializer};

pub mod canonical {
    use sha2::{Digest, Sha256};

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
}

pub mod storage_policy {
    #[derive(Clone, Copy, Debug)]
    pub struct BackupResources {
        pub durable_spool_bytes: u64,
    }

    pub const BACKUP_RESOURCES: BackupResources = BackupResources {
        durable_spool_bytes: 1024 * 1024 * 1024,
    };

    pub mod native_read {
        pub const EAGER_WORK_BYTES: u64 = 4 * super::native_write::PENDING_MEMORY_BYTES;
    }

    pub mod native_write {
        pub const PENDING_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
        pub const LOCAL_FILE_INPUT_MEMORY_BYTES: u64 = PENDING_MEMORY_BYTES;
    }
}

pub mod kind_partition {
    #[must_use]
    pub fn anchored(kind: &str, rest: &str) -> String {
        format!("{kind}\0{rest}")
    }

    #[must_use]
    pub fn prefix_upper_bound(prefix: &str) -> Option<String> {
        let mut end = prefix.to_owned();
        while let Some(c) = end.pop() {
            let next_cp = match c as u32 + 1 {
                0xD800 => 0xE000,
                n => n,
            };
            if let Some(next) = char::from_u32(next_cp) {
                end.push(next);
                return Some(end);
            }
        }
        None
    }

    #[must_use]
    pub fn exact_prefix_bounds(prefix: &str) -> (String, String) {
        let end = prefix_upper_bound(prefix).unwrap_or_else(|| format!("{prefix}\u{10FFFF}"));
        (prefix.to_owned(), end)
    }
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
        #[must_use]
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

pub use key_value::KeyValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryCursor {
    pub hash: Option<String>,
    pub range: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternate_primary: Option<KeyValue>,
}

impl QueryCursor {
    #[must_use]
    pub fn new(key: KeyValue, comparison: Option<String>) -> Self {
        Self {
            hash: key.hash,
            range: key.range,
            comparison,
            alternate_primary: None,
        }
    }

    #[must_use]
    pub fn into_key(self) -> KeyValue {
        KeyValue::new(self.hash, self.range)
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
