use crate::{DatabaseId, Namespace, PrincipalId, StreamId};

/// Merge key. Global delivery order is not this order.
///
/// The tuple is `(authored_at_ms, source_stream, sequence)`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stamp {
    /// When the author composed the entry. The shell supplies the clock reading.
    pub authored_at_ms: u64,
    /// Stream that wrote the entry.
    pub source_stream: StreamId,
    /// Writer sequence on that stream.
    pub sequence: u64,
}

/// Where a publisher re-append came from. Receivers see this on the publisher stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incorporation {
    /// Submission stream the contributor wrote.
    pub submission_stream: StreamId,
    /// Writer sequence on that submission stream.
    pub writer_seq: u64,
    /// Contributor who wrote the submission.
    pub contributor: PrincipalId,
}

/// One mutation the log can store. The payload is opaque to this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Database this entry belongs to.
    pub database_id: DatabaseId,
    /// Stream that wrote it.
    pub stream: StreamId,
    /// Gap-free sequence on `stream`. Zero is rejected.
    pub writer_seq: u64,
    /// Authoring time, supplied by the shell.
    pub authored_at_ms: u64,
    /// Principal who wrote this stream entry.
    pub author: PrincipalId,
    /// Namespace of the schema, without the schema name.
    pub namespace: Namespace,
    /// Set when the publisher copied a submission onto the publisher stream.
    pub incorporated_from: Option<Incorporation>,
    /// Opaque record bytes. This crate does not decode them.
    pub payload: Vec<u8>,
}

impl Entry {
    /// Stamp used for merge order.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        Stamp {
            authored_at_ms: self.authored_at_ms,
            source_stream: self.stream.clone(),
            sequence: self.writer_seq,
        }
    }
}

/// Tiny head row. `delivery_seq` is delivery order, not merge order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    /// Last delivery sequence in the log. The first accepted entry is 1.
    pub delivery_seq: u64,
    /// Stamp of the entry stored at `delivery_seq`.
    pub stamp: Stamp,
}

/// An entry plus the delivery sequence assigned when it was accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEntry {
    /// Position in delivery order.
    pub delivery_seq: u64,
    /// Time the shell accepted the batch. Retention uses this, not `authored_at_ms`.
    pub accepted_at_ms: u64,
    /// The accepted entry.
    pub entry: Entry,
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
    /// Only official namespaces can be appended. Receivers read `publisher_stream`.
    Shareable {
        /// Principal who authors the readable log and incorporates submissions.
        publisher: PrincipalId,
        /// The only stream a receiver may read.
        publisher_stream: StreamId,
    },
}

/// Why a stream exists inside one database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamRole {
    /// The shareable database's publisher stream.
    Publisher,
    /// One device of the personal database.
    Device {
        /// Device principal allowed to append this stream.
        owner: PrincipalId,
    },
    /// A contributor's pending submissions.
    Submission {
        /// Contributor allowed to append and read this stream.
        author: PrincipalId,
    },
}

/// What a grant allows. A grant does not name storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Capability {
    /// Append one personal-database device stream and read that database.
    Device {
        /// Device stream this principal appends.
        stream: StreamId,
    },
    /// Read the publisher stream of a shareable database.
    Receive,
    /// Append one submission stream and read that principal's own entries there.
    Submit {
        /// Submission stream this contributor appends.
        stream: StreamId,
    },
}

/// One read or append grant on the log's database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    /// Principal who holds the capability.
    pub principal: PrincipalId,
    /// What that principal may do.
    pub capability: Capability,
}

/// One append transaction for a single stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendBatch {
    /// Principal writing the stream.
    pub author: PrincipalId,
    /// Stream the entries extend.
    pub stream: StreamId,
    /// Entries in writer-sequence order. At most [`crate::APPEND_BATCH_LIMIT`].
    pub entries: Vec<Entry>,
    /// When the shell accepted the batch. Every stored entry in the batch shares it.
    pub accepted_at_ms: u64,
}

/// A snapshot cut. The bytes of the snapshot live in the shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Last delivery sequence the snapshot includes.
    pub through_delivery_seq: u64,
    /// When the shell cut the snapshot.
    pub at_ms: u64,
}
