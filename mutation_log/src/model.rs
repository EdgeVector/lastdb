use crate::{DatabaseId, DeviceId, Namespace, PrincipalId};

/// One writer inside a database log: the node key plus the device.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Writer {
    /// Node key that authored the entry.
    pub key: PrincipalId,
    /// Device of that node key.
    pub device: DeviceId,
}

/// Merge key. Global delivery order is not this order.
///
/// The tuple is `(ordering time, writer, sequence)`. The ordering time is
/// the older of the author time and the acceptance time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp {
    /// Older of the author time and the acceptance time.
    pub authored_at_ms: u64,
    /// Writer (node key, device) that composed the entry.
    pub writer: Writer,
    /// Writer sequence for that writer. Gap-free per writer, not global.
    pub sequence: u64,
}

impl Stamp {
    /// Ordering time. A sent time later than acceptance uses the acceptance time.
    #[must_use]
    pub fn ordering_millis(authored_at_ms: u64, accepted_at_ms: u64) -> u64 {
        authored_at_ms.min(accepted_at_ms)
    }
}

/// Where a publisher re-append came from.
///
/// The key is the submission's writer and its writer sequence, once in total.
/// A second device of the publisher key does not copy it again.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Incorporation {
    /// Writer (node key, device) that stored the submission.
    pub writer: Writer,
    /// Writer sequence of that submission.
    pub writer_seq: u64,
}

/// One mutation the log can store. The payload is opaque to this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Database this entry belongs to.
    pub database_id: DatabaseId,
    /// Writer (node key, device) that authored it.
    pub writer: Writer,
    /// Gap-free sequence for `writer`. Zero is rejected.
    pub writer_seq: u64,
    /// Authoring time, supplied by the shell. Opening the payload uses this
    /// number. Merge order uses the older of this time and the acceptance time.
    pub authored_at_ms: u64,
    /// Namespace of the schema, without the schema name.
    pub namespace: Namespace,
    /// Set when the publisher key copied a submission into the log.
    pub incorporated_from: Option<Incorporation>,
    /// Opaque record bytes. This crate does not decode them.
    pub payload: Vec<u8>,
}

impl Entry {
    /// Merge stamp. `authored_at_ms` on this entry stays as the writer sealed it.
    #[must_use]
    pub fn merge_stamp(&self, accepted_at_ms: u64) -> Stamp {
        Stamp {
            authored_at_ms: Stamp::ordering_millis(self.authored_at_ms, accepted_at_ms),
            writer: self.writer.clone(),
            sequence: self.writer_seq,
        }
    }
}

/// Tiny head row. `delivery_seq` is delivery order, not merge order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    /// Last delivery sequence in the log. The first accepted entry is 1.
    pub delivery_seq: u64,
    /// Ordering stamp of the entry stored at `delivery_seq`.
    pub stamp: Stamp,
}

/// An entry plus the delivery sequence assigned when it was accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEntry {
    /// Position in delivery order.
    pub delivery_seq: u64,
    /// Time the shell accepted the batch. Retention uses this, not `authored_at_ms`.
    pub accepted_at_ms: u64,
    /// The accepted entry. Its author time stays as sealed.
    pub entry: Entry,
}

impl StoredEntry {
    /// Merge stamp. A future author time uses [`Self::accepted_at_ms`].
    #[must_use]
    pub fn merge_stamp(&self) -> Stamp {
        self.entry.merge_stamp(self.accepted_at_ms)
    }
}

/// A database is personal or shareable. It is never both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseKind {
    /// The user's own database. It is not shareable.
    ///
    /// App-private namespaces are stored here. A receiver grant is rejected.
    Personal,
    /// A database receivers may be granted.
    ///
    /// Only official namespaces can be appended. A receiver reads entries
    /// whose writer key is `publisher`, including re-appends.
    Shareable {
        /// Node key that authors the readable entries and incorporates submissions.
        publisher: PrincipalId,
    },
}

/// What a grant allows. A grant does not name storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Capability {
    /// Read and append a personal database from any device of this node key.
    Owner,
    /// Read entries whose writer key is the publisher key.
    Receive,
    /// Append pending submissions from any device of this node key, and read those entries.
    Submit,
}

/// One read or append grant on the log's database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    /// Node key that holds the capability.
    pub principal: PrincipalId,
    /// What that key may do.
    pub capability: Capability,
}

/// One append transaction for a single writer (node key, device).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendBatch {
    /// Writer (node key, device) the entries extend.
    pub writer: Writer,
    /// Entries in writer-sequence order. At most [`crate::APPEND_BATCH_LIMIT`].
    pub entries: Vec<Entry>,
    /// When the shell accepted the batch. Every stored entry in the batch shares it.
    pub accepted_at_ms: u64,
}

/// Why a dropped pending submission cannot be copied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionStatus {
    /// The publisher rejected the submission.
    Rejected,
    /// The submission aged out before it was copied.
    Expired,
}

/// A pending submission whose entry has been dropped.
///
/// The value is the writer, that writer's sequence, why it ended, and the
/// time the entry was dropped. It carries no payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmissionRecord {
    /// Writer (node key, device) that stored the submission.
    pub writer: Writer,
    /// Writer sequence of that submission.
    pub writer_seq: u64,
    /// Rejected or expired.
    pub status: SubmissionStatus,
    /// When trim dropped the entry. The shell supplies this clock reading.
    pub time_ms: u64,
}

/// A snapshot cut. The bytes live in the shell at `snapshot_ref`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Last delivery sequence the snapshot includes.
    pub through_delivery_seq: u64,
    /// When the shell cut the snapshot.
    pub at_ms: u64,
    /// Shell pointer to the snapshot bytes.
    pub snapshot_ref: crate::SnapshotRef,
}
