//! Key coordinates, prefix partition bounds, resume cursors, and sort order.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize, Serializer};

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
    pub record: Option<String>,
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
