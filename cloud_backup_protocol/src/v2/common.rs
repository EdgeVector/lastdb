use crate::{CloudScope, DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One backup stream: an account scope plus a stream id.
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// One commit attempt on a stream.
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

pub(super) fn validate_destinations(values: &[Destination]) -> Result<(), ProtocolError> {
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
