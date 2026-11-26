//! `log.append`, `log.read`, `log.streams` and `log.trim`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `log.append`: signed entries on the caller's streams, or one log segment on the caller's
/// stream. Exactly one of `entries` and `segment` (else 400 `invalid_request`, field `segment`);
/// [`LogAppendRequest::body`] reads which.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LogAppendRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// Signed entries, each the JSON of a `mutation_log::SignedEntry`, in stream order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<Vec<Value>>,
    /// A log segment: entries whose bytes the caller put on the relay as one blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment: Option<SegmentAppend>,
}

/// What a `log.append` carries.
#[derive(Clone, Copy, Debug)]
pub enum AppendBody<'a> {
    /// Inline signed entries.
    Entries(&'a [Value]),
    /// One log segment.
    Segment(&'a SegmentAppend),
}

impl LogAppendRequest {
    /// An append of inline entries.
    #[must_use]
    pub const fn entries(database_id: String, entries: Vec<Value>) -> Self {
        Self {
            database_id,
            entries: Some(entries),
            segment: None,
        }
    }

    /// An append of one log segment.
    #[must_use]
    pub const fn segment(database_id: String, segment: SegmentAppend) -> Self {
        Self {
            database_id,
            entries: None,
            segment: Some(segment),
        }
    }

    /// The entries or the segment. `Err` names the field of the 400 answer when the request
    /// carries both or neither.
    ///
    /// # Errors
    ///
    /// `"segment"` when the request has both or neither.
    pub fn body(&self) -> Result<AppendBody<'_>, &'static str> {
        match (&self.entries, &self.segment) {
            (Some(entries), None) => Ok(AppendBody::Entries(entries)),
            (None, Some(segment)) => Ok(AppendBody::Segment(segment)),
            _ => Err("segment"),
        }
    }
}

/// A log segment in a `log.append`: the signed entries `first_seq` through `last_seq` of the
/// caller's stream `device_id`, whose bytes are the blob `sha256` (each entry's JSON followed by
/// one `\n`, in order). The blob is committed through `blob.put` before the append.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SegmentAppend {
    /// The stream, 64 lowercase hex.
    pub device_id: String,
    /// The first entry's sequence number, at least 1.
    pub first_seq: u64,
    /// The last entry's sequence number, at least `first_seq`.
    pub last_seq: u64,
    /// The entry hash of entry `last_seq`, 64 lowercase hex.
    pub last_hash: String,
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
    /// The length of the blob, at least 1.
    pub bytes: u64,
}

impl SegmentAppend {
    /// How many entries the segment holds.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.last_seq
            .saturating_sub(self.first_seq)
            .saturating_add(1)
    }
}

/// The answer to a `log.append`, counted in entries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogAppendAnswer {
    /// Entries of this call the relay now holds, counting those it held already; for a segment,
    /// the entries the segment holds. Equal to the entries sent when the call succeeds.
    pub stored: u64,
    /// Present and true when some or all of the entries were on the relay already. They count in
    /// `stored`, but nothing was written or charged for them.
    #[serde(default, skip_serializing_if = "crate::is_false")]
    pub duplicate: bool,
}

/// `log.read`: one writer's entries, from `from_seq` of one device stream or across every device
/// stream, or the page after `cursor`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LogReadRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The writer's node key, 64 lowercase hex. Required without `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_key: Option<String>,
    /// One device stream, 64 lowercase hex. Without it, every device stream of the writer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    /// The first sequence number to read (default 1). A read of one device stream from a
    /// position a trim dropped answers 410 `trimmed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<u64>,
    /// The `cursor` of the previous page; it carries the writer, the device and the position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// At most this many items (an entry or a segment each).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// One page of a `log.read`. A page stops before the answer would pass 6 MB, so a short page is
/// not the end of a stream: the reader follows `cursor`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogReadAnswer {
    /// The entries in stream order; a stored segment stands in its place as one item.
    pub entries: Vec<ReadItem>,
    /// Present when more may follow: pass it as `cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// One item of a `log.read` page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum ReadItem {
    /// A stored log segment, in its place in the stream.
    Segment {
        /// The segment and a presigned download of its blob.
        segment: SegmentRef,
    },
    /// One signed entry, the JSON of a `mutation_log::SignedEntry`.
    Entry(Value),
}

/// A stored log segment as `log.read` names it, with the `blob.get` answer for its blob. The
/// reader checks that the blob holds exactly entries `first_seq` through `last_seq` of the
/// stream, in order, and that the last one's hash is `last_hash`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SegmentRef {
    /// The writer, 64 lowercase hex.
    pub node_key: String,
    /// The stream, 64 lowercase hex.
    pub device_id: String,
    /// The first entry's sequence number.
    pub first_seq: u64,
    /// The last entry's sequence number.
    pub last_seq: u64,
    /// The entry hash of entry `last_seq`, 64 lowercase hex.
    pub last_hash: String,
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
    /// The length of the blob.
    pub bytes: u64,
    /// Presigned GET of the blob.
    pub url: String,
    /// Headers to send with the GET, exactly as given.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// When `url` stops working, unix milliseconds.
    pub expires_at_ms: u64,
}

/// `log.streams`: one writer's device streams.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LogStreamsRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The writer's node key, 64 lowercase hex.
    pub writer_key: String,
}

/// The answer to a `log.streams`. A writer with no stream gets an empty list.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogStreamsAnswer {
    /// The streams, ascending by `device_id`. A stream whose entry 1 is not stored is not listed.
    pub streams: Vec<StreamHead>,
}

/// One device stream and its contiguous head.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StreamHead {
    /// The stream, 64 lowercase hex.
    pub device_id: String,
    /// The highest sequence number such that every one from 1 up to it is stored.
    pub head_seq: u64,
    /// The entry hash of entry `head_seq`, 64 lowercase hex.
    pub head_hash: String,
}

/// `log.trim`: drop what the `snapshot` entry at `writer_seq` of stream `device_id` covers.
/// Publisher of a personal database only.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LogTrimRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The snapshot stream, 64 lowercase hex.
    pub device_id: String,
    /// The sequence number of the snapshot entry.
    pub writer_seq: u64,
}

/// The answer to a `log.trim`. A call does a bounded amount of work; the caller repeats while
/// `done` is false and adds the counts of every answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LogTrimAnswer {
    /// Nothing more to drop.
    pub done: bool,
    /// Entries this call dropped, those of dropped segments included.
    pub entries_deleted: u64,
    /// The stored length of the inline entries this call dropped.
    pub released_bytes: u64,
}
