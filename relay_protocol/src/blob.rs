//! `blob.put`, `blob.get`, `blob.list` and `blob.drop`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The two steps of a `blob.put`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BlobPutStage {
    /// Ask for a presigned upload. A blob the relay holds already is answered without `upload`.
    Begin,
    /// Confirm the upload. The relay checks the length and the recorded checksum and reads no
    /// object bytes.
    Commit,
}

/// `blob.put`: begin or commit the upload of one blob, named by the SHA-256 of its bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BlobPutRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
    /// The length of the blob. A relay that holds a limit on one blob answers a larger begin with
    /// 400 `invalid_request`, field `bytes`.
    pub bytes: u64,
    /// Begin or commit.
    pub stage: BlobPutStage,
}

/// The answer to a `blob.put`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BlobPutAnswer {
    /// The stage this answers.
    pub stage: BlobPutStage,
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
    /// On a begin for a blob the relay does not hold: where to upload it. Absent when the blob is
    /// committed already, and on a commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload: Option<Upload>,
}

/// A presigned upload. The client sends exactly `headers` (they carry the checksum the store
/// enforces), except `content-length`, which it sets itself.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Upload {
    /// Presigned PUT.
    pub url: String,
    /// Headers to send with the PUT.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// When `url` stops working, unix milliseconds.
    pub expires_at_ms: u64,
}

/// `blob.get`: a presigned download of one blob.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BlobGetRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
}

/// The answer to a `blob.get`: exactly one download path, a presigned URL.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BlobGetAnswer {
    /// Presigned GET.
    pub url: String,
    /// Headers to send with the GET, exactly as given.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// When `url` stops working, unix milliseconds.
    pub expires_at_ms: u64,
    /// The length of the blob.
    pub bytes: u64,
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
}

/// `blob.list`: the committed blobs of a database, ascending by digest, a page at a time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BlobListRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The `next` of the previous page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

/// One page of a `blob.list`, at most 1,000 blobs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BlobListAnswer {
    /// The committed blobs of this page.
    pub blobs: Vec<BlobListed>,
    /// Present when more follow: pass it as `after`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// One committed blob.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BlobListed {
    /// The SHA-256 of the blob, 64 lowercase hex.
    pub sha256: String,
    /// The length of the blob.
    pub bytes: u64,
}

/// `blob.drop`: drop named blobs while all stream heads are exactly as named. Only
/// the database publisher can drop blobs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BlobDropRequest {
    /// The database, 64 lowercase hex.
    pub database_id: String,
    /// The head of the publisher's snapshot stream. Its manifest may not be dropped.
    pub snapshot: StreamPosition,
    /// The head of every other stream in the database, as `log.streams` listed them. At most 32.
    pub heads: Vec<StreamPosition>,
    /// The blobs to drop: 1 to 25 digests, each once, 64 lowercase hex.
    pub sha256: Vec<String>,
}

/// One stream at one entry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct StreamPosition {
    /// Stream signer when different from the snapshot publisher.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_key: Option<String>,
    /// The stream, 64 lowercase hex.
    pub device_id: String,
    /// The sequence number.
    pub writer_seq: u64,
}

impl StreamPosition {
    /// Resolve this position against the authenticated snapshot publisher.
    pub fn writer_or<'a>(&'a self, publisher: &'a str) -> &'a str {
        self.writer_key.as_deref().unwrap_or(publisher)
    }
}

/// The answer to a `blob.drop`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BlobDropAnswer {
    /// Blobs dropped.
    pub dropped: u64,
    /// Their summed length, released from the publisher's stored bytes.
    pub released_bytes: u64,
    /// Named digests with no committed blob.
    pub absent: u64,
}
