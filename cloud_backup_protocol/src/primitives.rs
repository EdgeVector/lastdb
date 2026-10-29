//! Strict scalar encodings for the version-one wire contract.

use crate::ProtocolError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The numeric wire version. Version one accepts only the JSON integer `1`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version;

impl Version {
    /// The only version-one value. It serializes as the JSON integer 1.
    pub const V1: Self = Self;

    #[must_use]
    /// Return `1`.
    pub const fn get(self) -> u8 {
        1
    }
}

impl Serialize for Version {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(1)
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match u8::deserialize(deserializer)? {
            1 => Ok(Self::V1),
            _ => Err(serde::de::Error::custom(ProtocolError::invalid(
                "unsupported protocol version",
            ))),
        }
    }
}

/// An exact unsigned integer encoded as a canonical decimal JSON string.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DecimalU64(u64);

impl DecimalU64 {
    #[must_use]
    /// Wrap `value`. This does not check a decimal wire encoding.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    /// Return the integer.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<String> for DecimalU64 {
    type Error = ProtocolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > 20
            || !value.bytes().all(|byte| byte.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return Err(ProtocolError::invalid("invalid decimal integer"));
        }
        value
            .parse::<u64>()
            .map(Self)
            .map_err(|_| ProtocolError::invalid("decimal integer exceeds its limit"))
    }
}

impl Serialize for DecimalU64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DecimalU64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

macro_rules! string_primitive {
    ($name:ident, $validate:ident, $message:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Borrow the validated string.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = ProtocolError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                if $validate(&value) {
                    Ok(Self(value))
                } else {
                    Err(ProtocolError::invalid($message))
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

string_primitive!(
    Digest,
    valid_digest,
    "invalid SHA-256 digest",
    "Lowercase hex SHA-256 digest, 64 characters."
);
string_primitive!(
    Id,
    valid_id,
    "invalid nonnil UUID",
    "Non-nil UUID: 36 lowercase hex characters with hyphens."
);
string_primitive!(
    AccountId,
    valid_account,
    "invalid account identity",
    "32-character lowercase hex account identity."
);
string_primitive!(
    RelativePath,
    valid_path,
    "invalid relative path",
    "Relative path of at most 512 bytes and eight components. Empty parts, `.`, `..`, a backslash, a drive prefix, and control characters are rejected."
);

fn lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && lower_hex(value)
}

fn valid_account(value: &str) -> bool {
    value.len() == 32 && lower_hex(value)
}

fn valid_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
        && value.bytes().any(|byte| byte != b'0' && byte != b'-')
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

fn valid_path(value: &str) -> bool {
    if !valid_text(value, 512) || value.contains('\\') {
        return false;
    }
    let mut components = value.split('/');
    let Some(first) = components.next() else {
        return false;
    };
    let bytes = first.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return false;
    }
    let normal = |part: &str| !part.is_empty() && part != "." && part != "..";
    normal(first) && value.split('/').count() <= 8 && components.all(normal)
}

/// Nonempty, control-free UTF-8 text, limited by its stored byte length.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Text<const N: usize>(String);

impl<const N: usize> Text<N> {
    #[must_use]
    /// Borrow the validated text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<const N: usize> TryFrom<String> for Text<N> {
    type Error = ProtocolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if valid_text(&value, N) {
            Ok(Self(value))
        } else {
            Err(ProtocolError::invalid("invalid bounded text"))
        }
    }
}

impl<'de, const N: usize> Deserialize<'de> for Text<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
