//! Wire types for logical and at-rest photographs.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// How values in a [`Snapshot`] are encoded under the outer cloud seal.
///
/// Mini personal at-rest uses the same account E2E content key as cloud
/// (`design-portable-same-key-at-rest-cloud`). When that invariant holds,
/// checkpoints can carry on-disk `ENC:…` envelopes without decrypting to
/// logical plaintext (cheaper heal / backup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotValueEncoding {
    /// Values are logical plaintext (post at-rest decrypt). Historical default.
    /// Restore through the encrypting store so puts re-seal under at-rest.
    #[default]
    Logical,
    /// Values are on-disk at-rest envelopes (`ENC:<base64(ct)>` or raw sealed
    /// bytes as stored). Create/restore use the **raw** store so we never
    /// decrypt-all or double-encrypt. Requires same-key Mini content-key at-rest.
    AtRestEnc,
}

/// A serialized snapshot of the entire database.
///
/// The snapshot format is backend-agnostic: a list of namespaces,
/// each containing a list of key-value pairs. Keys and values are
/// base64-encoded bytes.
///
/// For streaming, the snapshot is written namespace-by-namespace to
/// a temp file, then encrypted and uploaded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Format version for forward compatibility.
    pub version: u32,
    /// Timestamp when the snapshot was created (millis since epoch).
    pub created_at_ms: u64,
    /// Device ID that created this snapshot.
    pub device_id: String,
    /// The log sequence number this snapshot covers up to (inclusive).
    pub last_seq: u64,
    /// Exact incorporated writers for a scoped multi-writer replacement.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub writer_frontier: BTreeMap<String, u64>,
    /// How namespace entry values are encoded. Omitted on pre-2026-07-18
    /// cloud objects → deserializes as [`SnapshotValueEncoding::Logical`].
    #[serde(default)]
    pub value_encoding: SnapshotValueEncoding,
    /// All namespaces and their key-value pairs.
    pub namespaces: Vec<NamespaceData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceData {
    pub name: String,
    /// Key-value pairs, both base64-encoded.
    pub entries: Vec<SnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: String,
}
