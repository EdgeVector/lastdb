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
    /// Wall clock of the write, in milliseconds. Display only.
    pub authored_at_ms: u64,
    /// The order value: a time in milliseconds, above the previous entry of the
    /// stream and every cited entry, and below [`crate::LAMPORT_LIMIT`].
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

/// Per-device-stream position carried in a grant entry. Nothing reads it; it
/// stays because the signed bytes include it.
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

/// Bundle body fields (encrypted blob uploaded before append).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleBody {
    /// SHA-256 of encrypted bundle blob bytes.
    pub blob_sha256: Hex32,
    /// Encrypted blob length in bytes.
    pub blob_size: u64,
    /// Inner direct data changes (must match decrypted body).
    pub change_count: u32,
    /// Grant device binding. Bundles are the publisher's, so this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_at_device: Option<DeviceId>,
    /// Grant seq binding. Bundles are the publisher's, so this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_at_seq: Option<u64>,
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
    /// Grant device binding. Rejected on a proposal and unused on direct entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant_at_device: Option<DeviceId>,
    /// Grant seq binding. Rejected on a proposal and unused on direct entries.
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
    /// Carried for the wire format only; nothing reads it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cutoffs: Vec<RevocationCutoff>,
}

/// Schema declaration body fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaBody {
    /// FoldDB schema name (history namespace).
    pub namespace: String,
    /// UTF-8 JSON schema definition (wire `definition_b64`).
    #[serde(rename = "definition_b64", with = "payload_b64_serde")]
    pub definition: Vec<u8>,
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

/// One stream position a snapshot covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotThrough {
    /// Device id of the stream.
    pub device_id: DeviceId,
    /// Last `writer_seq` of that stream that the snapshot holds.
    pub writer_seq: u64,
    /// Hash of that entry.
    pub entry_hash: EntryHash,
    /// Order value (`lamport`) of that entry.
    pub order: u64,
}

/// Snapshot body. The manifest and every file it lists are blobs on the relay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBody {
    /// SHA-256 of the manifest blob.
    pub manifest_sha256: Hex32,
    /// Length of the manifest blob in bytes.
    pub manifest_bytes: u64,
    /// The stream positions the snapshot covers, ascending by device id. A
    /// reader of the snapshot reads each stream after its position.
    pub through: Vec<SnapshotThrough>,
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
    /// Multi-change bundle.
    Bundle {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"bundle"`.
        namespace: String,
        /// Bundle body fields.
        #[serde(flatten)]
        body: BundleBody,
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
    /// Schema declaration.
    Schema {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Schema body fields (includes the schema name as `namespace`).
        #[serde(flatten)]
        body: SchemaBody,
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
    /// Snapshot.
    Snapshot {
        /// Common signed header.
        #[serde(flatten)]
        header: EntryHeader,
        /// Const namespace `"snapshot"`.
        namespace: String,
        /// Snapshot body fields.
        #[serde(flatten)]
        body: SnapshotBody,
    },
}

impl SignedEntry {
    /// Borrow the common header.
    #[must_use]
    pub fn header(&self) -> &EntryHeader {
        match self {
            Self::Birth { header, .. }
            | Self::Data { header, .. }
            | Self::Bundle { header, .. }
            | Self::Grant { header, .. }
            | Self::Schema { header, .. }
            | Self::BlobDelete { header, .. }
            | Self::Heads { header, .. }
            | Self::Relays { header, .. }
            | Self::Snapshot { header, .. } => header,
        }
    }

    /// Mutable header (for signing helpers that fill `signature`).
    pub fn header_mut(&mut self) -> &mut EntryHeader {
        match self {
            Self::Birth { header, .. }
            | Self::Data { header, .. }
            | Self::Bundle { header, .. }
            | Self::Grant { header, .. }
            | Self::Schema { header, .. }
            | Self::BlobDelete { header, .. }
            | Self::Heads { header, .. }
            | Self::Relays { header, .. }
            | Self::Snapshot { header, .. } => header,
        }
    }

    /// Kind discriminant.
    #[must_use]
    pub fn kind(&self) -> EntryKind {
        match self {
            Self::Birth { .. } => EntryKind::Birth,
            Self::Data { .. } => EntryKind::Data,
            Self::Bundle { .. } => EntryKind::Bundle,
            Self::Grant { .. } => EntryKind::Grant,
            Self::Schema { .. } => EntryKind::Schema,
            Self::BlobDelete { .. } => EntryKind::BlobDelete,
            Self::Heads { .. } => EntryKind::Heads,
            Self::Relays { .. } => EntryKind::Relays,
            Self::Snapshot { .. } => EntryKind::Snapshot,
        }
    }
}
