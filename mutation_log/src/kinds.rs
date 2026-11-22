//! Entry kinds, roles, domains, and grant/data wire enums.

use serde::{Deserialize, Serialize};

/// Domain strings written into `signed_bytes` (UTF-8 + 0x00 terminator).
pub mod domain {
    /// Birth entry domain (`lastdb-birth-v1`). Doc prose said "birth material in chain"; OpenAPI/kind uses this fixed string.
    pub const BIRTH: &str = "lastdb-birth-v1";
    /// Data / mutation domain.
    pub const DATA: &str = "lastdb-mutation-v1";
    /// Grant / revoke domain.
    pub const GRANT: &str = "lastdb-grant-v1";
    /// Schema declaration domain.
    pub const SCHEMA: &str = "lastdb-schema-v1";
    /// Blob delete domain.
    pub const BLOB_DELETE: &str = "lastdb-blob-delete-v1";
    /// Publisher heads attestation domain.
    pub const HEADS: &str = "lastdb-heads-v1";
    /// Publisher relay list domain.
    pub const RELAYS: &str = "lastdb-relays-v1";
    /// Multi-change bundle domain.
    pub const BUNDLE: &str = "lastdb-bundle-v1";
}

/// Wire kind discriminant (OpenAPI `kind` field).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// First publisher-device chain entry.
    Birth,
    /// Record mutation (direct or proposal).
    Data,
    /// Encrypted multi-change batch, signed by the publisher.
    Bundle,
    /// Grant / role change / revoke.
    Grant,
    /// Schema declaration for a namespace used by later data entries.
    Schema,
    /// Publisher blob reclaim marker.
    BlobDelete,
    /// Publisher tip vector for hidden-entry detection.
    Heads,
    /// Publisher-signed relay URL list.
    Relays,
}

impl EntryKind {
    /// Domain string for this kind.
    #[must_use]
    pub const fn domain(self) -> &'static str {
        match self {
            Self::Birth => domain::BIRTH,
            Self::Data => domain::DATA,
            Self::Bundle => domain::BUNDLE,
            Self::Grant => domain::GRANT,
            Self::Schema => domain::SCHEMA,
            Self::BlobDelete => domain::BLOB_DELETE,
            Self::Heads => domain::HEADS,
            Self::Relays => domain::RELAYS,
        }
    }

    /// Publisher-only kinds (owner from birth may author).
    #[must_use]
    pub const fn is_publisher_kind(self) -> bool {
        matches!(
            self,
            Self::Birth
                | Self::Grant
                | Self::Schema
                | Self::BlobDelete
                | Self::Heads
                | Self::Relays
        )
    }
}

/// Data stream flag: direct mutation vs proposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    /// Applied content (publisher or granted write).
    Direct,
    /// Proposal; content only after publisher incorporation.
    Proposal,
}

impl StreamKind {
    /// Wire `u8` in signed bytes: 0=direct, 1=proposal.
    #[must_use]
    pub const fn wire_byte(self) -> u8 {
        match self {
            Self::Direct => 0,
            Self::Proposal => 1,
        }
    }

    /// Parse wire `u8`.
    pub fn from_wire(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Direct),
            1 => Some(Self::Proposal),
            _ => None,
        }
    }
}

/// Grant role (OpenAPI `GrantRole`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Read-only.
    Read,
    /// May submit proposals; the publisher accepts creates and changes to
    /// records the member created.
    Submit,
    /// May submit proposals; the publisher accepts a change to any record.
    Write,
    /// Revoked (wins equal-lamport grant ties).
    Revoked,
}

impl Role {
    /// Wire `u8`: read=0, submit=1, write=2, revoked=3.
    #[must_use]
    pub const fn wire_byte(self) -> u8 {
        match self {
            Self::Read => 0,
            Self::Submit => 1,
            Self::Write => 2,
            Self::Revoked => 3,
        }
    }

    /// Parse wire `u8`.
    pub fn from_wire(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Read),
            1 => Some(Self::Submit),
            2 => Some(Self::Write),
            3 => Some(Self::Revoked),
            _ => None,
        }
    }

    /// True when `self` is strictly lower privilege than `other`.
    #[must_use]
    pub fn is_lower_than(self, other: Self) -> bool {
        rank(self) < rank(other)
    }
}

fn rank(r: Role) -> u8 {
    match r {
        Role::Revoked => 0,
        Role::Read => 1,
        Role::Submit => 2,
        Role::Write => 3,
    }
}

/// Grant grantee: public `any` or a specific node key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Grantee {
    /// Public-read grantee (wire string `"any"`).
    Any,
    /// Member node public key (hex).
    Member(super::ids::NodeKey),
}

impl Grantee {
    /// Wire tag: 0=`any`, 1=member (+32-byte key).
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Any => 0,
            Self::Member(_) => 1,
        }
    }
}

impl Serialize for Grantee {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Any => s.serialize_str("any"),
            Self::Member(k) => s.serialize_str(&k.to_hex()),
        }
    }
}

impl<'de> Deserialize<'de> for Grantee {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s == "any" {
            return Ok(Self::Any);
        }
        super::ids::Hex32::from_hex(&s)
            .map(Self::Member)
            .map_err(serde::de::Error::custom)
    }
}
