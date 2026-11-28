//! The signed snapshot manifest and stream positions.

use crate::ids::{DeviceId, EntryHash, Hex32, NodeKey};
use serde::{Deserialize, Serialize};

/// One stream position a snapshot covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotThrough {
    /// Signer of this stream. Omitted for a stream signed by the snapshot publisher.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_key: Option<NodeKey>,
    /// Device id of the stream.
    pub device_id: DeviceId,
    /// Last `writer_seq` of that stream that the snapshot holds.
    pub writer_seq: u64,
    /// Hash of that entry.
    pub entry_hash: EntryHash,
    /// Order value (`lamport`) of that entry.
    pub order: u64,
}

impl SnapshotThrough {
    /// Resolve a covered stream using the signer of the enclosing snapshot.
    pub fn stream_id(&self, publisher: NodeKey) -> crate::StreamId {
        crate::StreamId {
            node_key: self.writer_key.unwrap_or(publisher),
            device_id: self.device_id,
        }
    }

    /// Whether this position extends the same chain position in a previous snapshot.
    pub fn covers(&self, previous: &Self, publisher: NodeKey) -> bool {
        self.stream_id(publisher) == previous.stream_id(publisher)
            && (self.writer_seq > previous.writer_seq
                || (self.writer_seq == previous.writer_seq
                    && self.entry_hash == previous.entry_hash
                    && self.order == previous.order))
    }
}

/// Snapshot body. The manifest and every file it lists are blobs on the relay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBody {
    /// SHA-256 of the manifest blob.
    pub manifest_sha256: Hex32,
    /// Length of the manifest blob in bytes.
    pub manifest_bytes: u64,
    /// The stream positions the snapshot covers, ascending by signer and device id. A
    /// reader of the snapshot reads each stream after its position.
    pub through: Vec<SnapshotThrough>,
}
