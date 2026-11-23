//! Body of a `lease` entry.

use serde::{Deserialize, Serialize};

use crate::ids::{DeviceId, NodeKey};

/// One device stream that a lease closing records: the last entry of the stream
/// that the publisher had accepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedStream {
    /// Writer node key of the stream.
    pub node_key: NodeKey,
    /// Device id of the stream.
    pub device_id: DeviceId,
    /// Last `writer_seq` of the stream that counts under the leases this entry closes.
    pub writer_seq: u64,
}

/// Lease body. A lease covers every key whose role is `write` in the publisher's
/// grant entries that come before the lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseBody {
    /// A covered member signs direct entries only before this time, by its own clock.
    pub expires_at_ms: u64,
    /// Later than `expires_at_ms`. A lease entry signed at or after this time closes the lease.
    pub closes_at_ms: u64,
    /// For the earlier leases that this entry closes: per device stream that wrote
    /// under them, the last entry the publisher had accepted. Ascending by
    /// `(node_key, device_id)`.
    pub closed: Vec<ClosedStream>,
}
