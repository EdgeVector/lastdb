//! Signed entry shapes matching OpenAPI `SignedEntry` schemas.

use serde::{Deserialize, Serialize};

use crate::ids::{DatabaseId, DeviceId, EntryHash, Hex32, NodeKey};
use crate::kinds::{EntryKind, Grantee, Role, StreamKind};

/// Common header fields shared by every signed entry (OpenAPI).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryHeader {
    /// Self-certifying database id.
    pub database_id: DatabaseId,
    /// Writer Ed25519 public key.
    pub node_key: NodeKey,
    /// Device stream id.
    pub device_id: DeviceId,
    /// Gapless sequence within the stream (≥ 1).
    pub writer_seq: u64,
    /// Previous entry hash in this stream (zeros when `writer_seq == 1`).
    pub prev_hash: EntryHash,
    /// Display-only wall clock.
    pub authored_at_ms: u64,
    /// Checked Lamport clock.
    pub lamport: u64,
    /// Entry hashes this entry depends on (sorted ascending on the wire).
    pub seen_heads: Vec<EntryHash>,
    /// Ed25519 signature over `signed_bytes`, base64url no-pad (86 chars).
    pub signature: String,
}

/// File attachment on a data entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationFileRef {
    /// Opaque file handle (1..=256 UTF-8 bytes).
    pub file_handle: String,
    /// SHA-256 of committed file bytes.
    pub sha256: Hex32,
}

/// Publisher incorporation of a proposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncorporatedFrom {
    /// Proposal writer node key.
    pub node_key: NodeKey,
    /// Proposal device id.
    pub device_id: DeviceId,
    /// Proposal writer_seq.
    pub writer_seq: u64,
    /// SHA-256 of the proposal signature raw bytes.
    pub signature_sha256: Hex32,
}

/// Per-device tip cited in a `heads` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceHead {
    /// Device id of the tip stream (same publisher node).
    pub device_id: DeviceId,
    /// Tip writer_seq.
    pub writer_seq: u64,
    /// Tip entry hash.
    pub entry_hash: EntryHash,
}

/// Role-reduction cutoff for one device stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevocationCutoff {
    /// Cutoff device id.
    pub device_id: DeviceId,
    /// Last accepted writer_seq (0 = unseen device).
    pub writer_seq: u64,
    /// Hash at that seq (zeros when `writer_seq == 0`).
    pub entry_hash: EntryHash,
}

/// Birth body fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BirthBody {
    /// Owner node key (from birth).
    pub owner: NodeKey,
    /// Database salt (hex).
    pub db_salt: Hex32,
    /// Db-key signature over `owner ‖ db_salt` (base64url 86).
    pub birth_signature: String,
}

/// Data body fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataBody {
    /// Direct vs proposal.
    pub stream: StreamKind,
    /// Application namespace (1..=128).
    pub namespace: String,
    /// Payload bytes (wire `payload_b64`); see [`crate::payload`].
    #[serde(rename = "payload_b64", with = "payload_b64_serde")]
    pub payload: Vec<u8>,
    /// Optional file ref.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<MutationFileRef>,
    /// Optional incorporation of a proposal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incorporated_from: Option<IncorporatedFrom>,
    /// Grant device binding for member direct writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_at_device: Option<DeviceId>,
    /// Grant seq binding for member direct writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_at_seq: Option<u64>,
}

mod payload_b64_serde {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&URL_SAFE_NO_PAD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        URL_SAFE_NO_PAD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

/// Grant body fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantBody {
    /// Grantee (`any` or member key).
    pub grantee: Grantee,
    /// Role assigned (or `revoked`).
    pub role: Role,
    /// Cutoffs required only when lowering / revoking.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cutoffs: Vec<RevocationCutoff>,
}

/// Blob-delete body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobDeleteBody {
    /// Blob content hash.
    pub sha256: Hex32,
}

/// Heads attestation body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadsBody {
    /// Publisher tip vector.
    pub publisher_heads: Vec<DeviceHead>,
}

/// Relays body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelaysBody {
    /// Relay URLs.
    pub relays: Vec<RelayUrl>,
}

/// One relay URL object (OpenAPI).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayUrl {
    /// Relay base URL.
    pub url: String,
}

/// Full signed entry (OpenAPI `SignedEntry` oneOf).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignedEntry {
    /// Birth.
    Birth {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"birth"`.
        namespace: String,
        /// Birth body fields.
        #[serde(flatten)]
        body: BirthBody,
    },
    /// Data mutation.
    Data {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Data body fields.
        #[serde(flatten)]
        body: DataBody,
    },
    /// Grant / revoke.
    Grant {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"grant"`.
        namespace: String,
        /// Grant body fields.
        #[serde(flatten)]
        body: GrantBody,
    },
    /// Blob delete.
    #[serde(rename = "blob_delete")]
    BlobDelete {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"blob_delete"`.
        namespace: String,
        /// Blob-delete body fields.
        #[serde(flatten)]
        body: BlobDeleteBody,
    },
    /// Heads.
    Heads {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"heads"`.
        namespace: String,
        /// Heads body fields.
        #[serde(flatten)]
        body: HeadsBody,
    },
    /// Relays.
    Relays {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"relays"`.
        namespace: String,
        /// Relays body fields.
        #[serde(flatten)]
        body: RelaysBody,
    },
}

impl SignedEntry {
    /// Borrow the common header.
    #[must_use]
    pub fn header(&self) -> &EntryHeader {
        match self {
            Self::Birth { header, .. }
            | Self::Data { header, .. }
            | Self::Grant { header, .. }
            | Self::BlobDelete { header, .. }
            | Self::Heads { header, .. }
            | Self::Relays { header, .. } => header,
        }
    }

    /// Mutable header (for signing helpers that fill `signature`).
    pub fn header_mut(&mut self) -> &mut EntryHeader {
        match self {
            Self::Birth { header, .. }
            | Self::Data { header, .. }
            | Self::Grant { header, .. }
            | Self::BlobDelete { header, .. }
            | Self::Heads { header, .. }
            | Self::Relays { header, .. } => header,
        }
    }

    /// Kind discriminant.
    #[must_use]
    pub fn kind(&self) -> EntryKind {
        match self {
            Self::Birth { .. } => EntryKind::Birth,
            Self::Data { .. } => EntryKind::Data,
            Self::Grant { .. } => EntryKind::Grant,
            Self::BlobDelete { .. } => EntryKind::BlobDelete,
            Self::Heads { .. } => EntryKind::Heads,
            Self::Relays { .. } => EntryKind::Relays,
        }
    }
}
