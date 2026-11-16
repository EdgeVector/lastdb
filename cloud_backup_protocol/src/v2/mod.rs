//! Generic byte backup transport. Pure wire validation; no I/O or admission.
//! Native formats, recovery keys and client completeness stay with the client.

use crate::{AccountId, CloudScope, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod share_access;
pub mod transfer;

/// Record blob objects at client-derived paths.
pub mod blobs {
    pub use super::{BlobPath, MAX_BLOB_SEGMENT_BYTES};
}

/// Advisory account context read wire types.
pub mod context {
    pub use super::{ContextReadRequest, ContextReadResponse};
}

/// Commitment domains starting with `lastdb:cloud-byte-backup:` and ending with `:v2`.
pub mod hash {
    use crate::{Digest, ProtocolError};
    use serde::Serialize;

    /// RFC 8785 bytes of `value`. Fails as invalid JSON if serialization or canonicalization fails.
    pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
        crate::canonical_json_bytes(value, "v2.json", "v2.canonical_json")
    }

    /// SHA-256 of a length-prefixed V2 domain and the canonical body.
    pub fn input<T: Serialize>(domain: &'static str, value: &T) -> Result<Digest, ProtocolError> {
        if !domain.starts_with("lastdb:cloud-byte-backup:") || !domain.ends_with(":v2") {
            return Err(ProtocolError::invalid("v2.digest_domain"));
        }
        let bytes = canonical_bytes(value)?;
        let domain_length =
            u32::try_from(domain.len()).map_err(|_| ProtocolError::limit("v2.domain"))?;
        let body_length =
            u64::try_from(bytes.len()).map_err(|_| ProtocolError::limit("v2.body"))?;
        crate::framed_sha256_digest(domain_length, domain, body_length, &bytes)
    }
}

/// Status and close messages for one cold-blob job.
pub mod jobs {
    pub use super::{CloseReason, JobCloseRequest, JobResult, JobStatus};
}

/// Transport metadata bounds, independent of client disk or object file size.
pub mod limits {
    use crate::ProtocolError;
    use serde::Serialize;

    /// Maximum canonical JSON bytes of a V2 control message.
    pub const CONTROL_JSON_MAX_BYTES: u64 = 256 * 1024;
    /// Maximum JSON bytes of one signed mutation-log snapshot part response.
    pub const MUTATION_LOG_SNAPSHOT_PART_MAX_BYTES: u64 = 1024 * 1024;
    /// Maximum items in one V2 batch.
    pub const MAX_BATCH_ITEMS: usize = 4;

    /// Reject canonical JSON of `value` larger than `maximum`.
    pub fn encoded<T: Serialize>(value: &T, maximum: u64) -> Result<(), ProtocolError> {
        let bytes = super::hash::canonical_bytes(value)?;
        if bytes.len() as u64 > maximum {
            return Err(ProtocolError::limit("v2.encoded_bytes"));
        }
        Ok(())
    }
}

/// The protocol version. It accepts only the JSON integer 2.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version;

impl Version {
    /// The only V2 value. It serializes as the JSON integer 2.
    pub const V2: Self = Self;
    /// Return `2`.
    #[must_use]
    pub const fn get(self) -> u8 {
        2
    }
}

impl Serialize for Version {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(2)
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match u8::deserialize(deserializer)? {
            2 => Ok(Self::V2),
            _ => Err(serde::de::Error::custom(ProtocolError::invalid(
                "v2.version",
            ))),
        }
    }
}

/// Maximum bytes of one path segment (space, write id).
pub const MAX_BLOB_SEGMENT_BYTES: usize = 128;

/// The client-derived location of one record blob version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobPath {
    /// Database identity that owns the record, stable across devices.
    pub database: Id,
    /// Record space of the schema.
    pub space: Text<MAX_BLOB_SEGMENT_BYTES>,
    /// SHA-256 of the record key. Key text never reaches the provider.
    pub record: Digest,
    /// Write id of the blob version; each version is its own object.
    pub write_id: Text<MAX_BLOB_SEGMENT_BYTES>,
}

impl BlobPath {
    /// Reject a segment that is not a plain path segment.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        for segment in [self.space.as_str(), self.write_id.as_str()] {
            if !segment.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            }) || segment.starts_with('.')
            {
                return Err(ProtocolError::invalid("v2.blob_path.segment"));
            }
        }
        Ok(())
    }

    /// The path below the account prefix:
    /// `<database>/<space>/<sha256(record key)>/<write id>`.
    pub fn key_tail(&self) -> Result<String, ProtocolError> {
        self.validate()?;
        Ok(format!(
            "{}/{}/{}/{}",
            self.database.as_str(),
            self.space.as_str(),
            self.record.as_str(),
            self.write_id.as_str()
        ))
    }
}

/// One backup stream: an account scope plus a stream id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    /// Account id and account generation.
    pub scope: CloudScope,
    /// Stream id inside the account scope.
    pub stream_id: Id,
}

impl Stream {
    /// Validate the account scope.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()
    }
}

/// One commit attempt on a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    /// Account scope and stream id.
    pub stream: Stream,
    /// Commit id within the stream.
    pub commit_id: Id,
}

impl Job {
    /// Validate the stream.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()
    }
}

/// Immutable provider destination without any client file-kind meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Destination {
    /// Cloudflare R2.
    R2,
    /// Backblaze B2.
    B2,
}

fn validate_destinations(values: &[Destination]) -> Result<(), ProtocolError> {
    if values.len() > 2
        || values
            .iter()
            .enumerate()
            .any(|(index, value)| values[..index].contains(value))
    {
        return Err(ProtocolError::invalid("v2.required_destinations"));
    }
    Ok(())
}

/// Positive stored byte length. Provider, account and working-resource admission
/// remain runtime policy; a transport object has no native file-size ceiling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    /// SHA-256 of the stored object bytes.
    pub sha256: Digest,
    /// Stored object length, in bytes. Zero is rejected.
    pub bytes: DecimalU64,
    /// The provider that stores the object.
    pub destination: Destination,
}

impl Object {
    /// Reject a zero byte length.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.get() == 0 {
            return Err(ProtocolError::invalid("v2.object.bytes"));
        }
        Ok(())
    }
}

/// Ask which account generation is current and whether new work is allowed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// 32-character lowercase hex account identity.
    pub account_id: AccountId,
    /// Providers the caller requires. More than two, or a duplicate, is rejected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_destinations: Vec<Destination>,
}

impl ContextReadRequest {
    /// Reject a bad destination list, an account id that is not 32 characters, or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_destinations(&self.required_destinations)?;
        if self.account_id.as_str().len() != 32 {
            return Err(ProtocolError::invalid("v2.context.account_id"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Advisory context only. Each mutation transaction enforces current authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextReadResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Account scope the service reports for the requested account id.
    pub scope: CloudScope,
    /// Whether the service currently allows new backup work. Mutations still enforce authority themselves.
    pub new_work_allowed: bool,
}

impl ContextReadResponse {
    /// Require the response account id to match the request, and check encoded size.
    pub fn validate_for(&self, request: &ContextReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.scope.validate()?;
        if self.scope.account_id != request.account_id {
            return Err(ProtocolError::invalid("v2.context.response.account_id"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Why a job was closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    /// The caller asked to close the job.
    UserRequested,
    /// The client source is no longer available.
    SourceUnavailable,
    /// The account generation changed.
    GenerationChanged,
}

/// Close fences acceptance and never proves that an older provider effect failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobStatus {
    /// The job is open. Close validation rejects this state.
    Active,
    /// The job closed.
    Closed {
        /// Why the job closed.
        reason: CloseReason,
    },
}

/// Current state of one job, bound to the begin digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobResult {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// SHA-256 of the Begin value.
    pub begin_sha256: Digest,
    /// Whether the job is active or closed.
    pub status: JobStatus,
}

impl JobResult {
    /// Reject a bad job or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Require the same job and begin digest, and reject a result that is still `Active`.
    pub fn validate_for_close(&self, request: &JobCloseRequest) -> Result<(), ProtocolError> {
        self.validate()?;
        request.validate()?;
        if self.job != request.job
            || self.begin_sha256 != request.begin_sha256
            || matches!(self.status, JobStatus::Active)
        {
            return Err(ProtocolError::invalid("v2.job_result.close"));
        }
        Ok(())
    }
}

/// Close one job. Closing does not prove an older provider write failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobCloseRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Stream and commit id this value belongs to.
    pub job: Job,
    /// SHA-256 of the Begin value.
    pub begin_sha256: Digest,
    /// Why the caller is closing the job.
    pub reason: CloseReason,
}

impl JobCloseRequest {
    /// Validate the job and the encoded size.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.job.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
