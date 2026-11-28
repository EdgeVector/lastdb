//! The signed snapshot manifest and stream positions.

use crate::ids::{DeviceId, EntryHash, Hex32, NodeKey};
use serde::{Deserialize, Serialize};

/// One stream position a snapshot covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotThrough {
    /// Signer of this stream. Omitted for a stream signed by the snapshot publisher.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) writer_key: Option<NodeKey>,
    /// Device id of the stream.
    pub(crate) device_id: DeviceId,
    /// Last `writer_seq` of that stream that the snapshot holds.
    pub writer_seq: u64,
    /// Hash of that entry.
    pub entry_hash: EntryHash,
    /// Order value (`lamport`) of that entry.
    pub order: u64,
}

impl SnapshotThrough {
    /// Record a position with the publisher implicit in its wire form.
    pub fn new(
        publisher: NodeKey,
        stream: crate::StreamId,
        writer_seq: u64,
        entry_hash: EntryHash,
        order: u64,
    ) -> Self {
        Self {
            writer_key: (stream.node_key != publisher).then_some(stream.node_key),
            device_id: stream.device_id,
            writer_seq,
            entry_hash,
            order,
        }
    }

    /// The device of a publisher-only position. Single-writer consumers must decline other positions.
    pub fn publisher_device(&self) -> Option<DeviceId> {
        self.writer_key.is_none().then_some(self.device_id)
    }

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
