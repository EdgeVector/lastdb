//! Fixed-size identifiers: 32-byte hex keys/hashes and UUID device ids.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Parse / format failure for wire identifiers.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IdError {
    /// Value is not 64 lowercase hex characters.
    #[error("expected 64 lowercase hex characters")]
    BadHex,
    /// Value is not a UUID string.
    #[error("expected UUID device_id")]
    BadUuid,
}

/// 32 raw bytes shown on the wire as 64 lowercase hex characters.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hex32(#[serde(with = "hex32_serde")] [u8; 32]);

impl Hex32 {
    /// All-zero value (genesis `prev_hash`).
    pub const ZERO: Self = Self([0u8; 32]);

    /// Wrap raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Copy the raw bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Parse 64 lowercase hex characters.
    pub fn from_hex(s: &str) -> Result<Self, IdError> {
        if s.len() != 64 || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(IdError::BadHex);
        }
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = (nibble(chunk[0]) << 4) | nibble(chunk[1]);
        }
        Ok(Self(out))
    }

    /// Format as 64 lowercase hex characters.
    #[must_use]
    pub fn to_hex(&self) -> String {
        app_identity_crypto::hex_lower(&self.0)
    }
}

impl std::fmt::Debug for Hex32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::fmt::Display for Hex32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

fn nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        _ => 0,
    }
}

mod hex32_serde {
    use super::Hex32;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&Hex32::from_bytes(*v).to_hex())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let s = String::deserialize(d)?;
        Hex32::from_hex(&s)
            .map(Hex32::to_bytes)
            .map_err(serde::de::Error::custom)
    }
}

/// Ed25519 public key (32 bytes) as lowercase hex.
pub type NodeKey = Hex32;
/// Self-certifying database id (db public key) as lowercase hex.
pub type DatabaseId = Hex32;
/// SHA-256 digest / entry hash as lowercase hex.
pub type EntryHash = Hex32;

/// Device stream id (UUID string on the wire, 16 bytes in signed material).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(Uuid);

impl DeviceId {
    /// Wrap a UUID.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Parse a UUID string.
    pub fn parse(s: &str) -> Result<Self, IdError> {
        Uuid::parse_str(s).map(Self).map_err(|_| IdError::BadUuid)
    }

    /// Raw 16 bytes (big-endian UUID layout).
    #[must_use]
    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Borrow the UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Debug for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

/// Stream identity: one hash chain per `(node_key, device_id)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    /// Writer node public key.
    pub node_key: NodeKey,
    /// Device id for this stream.
    pub device_id: DeviceId,
}
