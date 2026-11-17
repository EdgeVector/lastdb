//! Generic byte backup transport. Pure wire validation; no I/O or admission.
//! Native formats, recovery keys and client completeness stay with the client.

use crate::{AccountId, CloudScope, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod share_access;

/// Temporary presigned object upload grants.
pub mod transfer {
    pub use super::UploadGrant;
}

/// Record blob objects at client-derived paths.
pub mod blobs {
    pub use super::BlobPath;
}

/// Advisory account context read wire types.
pub mod context {
    pub use super::{ContextReadRequest, ContextReadResponse};
}

/// Commitment domains starting with `lastdb:cloud-byte-backup:` and ending with `:v2`.
pub mod hash {
    pub use crate::canonical_bytes;
    use crate::{Digest, ProtocolError};
    use serde::Serialize;

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

/// Transport metadata bounds, independent of client disk or object file size.
pub mod limits {
    use crate::ProtocolError;
    use serde::Serialize;

    /// Maximum canonical JSON bytes of a V2 control message.
    pub const CONTROL_JSON_MAX_BYTES: u64 = 256 * 1024;

    /// Reject canonical JSON of `value` larger than `maximum`.
    pub(crate) fn encoded<T: Serialize>(value: &T, maximum: u64) -> Result<(), ProtocolError> {
        let bytes = crate::canonical_bytes(value)?;
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

/// Temporary upload authority for a signed conditional PUT with an exact `Content-Length`.
/// Never save this URL in a durable record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadGrant {
    /// Temporary upload URL. Do not store it on a durable record.
    pub url: Text<8192>,
    /// Unix epoch milliseconds when this grant expires. Zero is rejected.
    pub expires_ms: DecimalU64,
}

impl UploadGrant {
    /// Reject a zero expiry or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.expires_ms.get() == 0 {
            return Err(ProtocolError::invalid("upload_grant.expires_ms"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

const MAX_BLOB_SEGMENT_BYTES: usize = 128;

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
    /// Reject a bad destination list or oversized JSON.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_destinations(&self.required_destinations)?;
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
