//! Snapshot pointers and delivery positions for one database log.
//!
//! These types name the objects. They do not fetch bytes. Pending submissions
//! are not part of a snapshot. Integers are decimal strings.

use crate::{DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

/// Opaque pointer to the published snapshot bytes of one database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotReference {
    /// Database whose published prefix this snapshot covers.
    pub database_id: Id,
    /// Last delivery sequence included. Zero is an empty database.
    pub through_delivery_seq: DecimalU64,
    /// Unix milliseconds when the snapshot was cut. Zero is rejected.
    pub at_ms: DecimalU64,
    /// Service pointer to the snapshot bytes. This is not the bytes.
    pub snapshot_ref: Text<1024>,
}

impl SnapshotReference {
    /// Reject a zero cut time. The pointer's own bounds are checked on decode.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.snapshot.at_ms"));
        }
        Ok(())
    }
}

/// One position in a database's delivery sequence.
///
/// The sequence counts every stored entry, including entries this reader
/// cannot see. Zero means nothing has been scanned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCursor {
    /// Database this position belongs to.
    pub database_id: Id,
    /// Last delivery sequence scanned.
    pub delivery_seq: DecimalU64,
}

/// One device's ack of a delivery sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCursor {
    /// Database this ack belongs to.
    pub database_id: Id,
    /// Node key of the reading device.
    pub node_public_key: Digest,
    /// Device of that node key. One to 64 bytes of ASCII letters, digits, `-`, or `_`.
    pub device_id: Text<64>,
    /// Highest delivery sequence this device has scanned.
    pub delivery_seq: DecimalU64,
}

impl DeviceCursor {
    /// Reject a device id outside the log-core label alphabet.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !valid_device_id(self.device_id.as_str()) {
            return Err(ProtocolError::invalid("share.cursor.device_id"));
        }
        Ok(())
    }
}

/// Snapshot a reader installs when its position is below the trim floor.
///
/// The caller applies `snapshot`, applies the still-stored suffix, persists
/// that, then acks `next`. The pull itself does not ack.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotReset {
    /// Published prefix to install.
    pub snapshot: SnapshotReference,
    /// Position to ack after the snapshot and the still-stored suffix are applied.
    pub next: DeliveryCursor,
}

impl SnapshotReset {
    /// Require one database, and a cursor at or after the snapshot prefix.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.snapshot.validate()?;
        if self.next.database_id != self.snapshot.database_id
            || self.next.delivery_seq < self.snapshot.through_delivery_seq
        {
            return Err(ProtocolError::invalid("share.snapshot.reset"));
        }
        Ok(())
    }
}

fn valid_device_id(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
