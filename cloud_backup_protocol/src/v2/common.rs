use super::{hash, limits};
use crate::{CloudScope, DecimalU64, Digest, Id, ProtocolError};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::Value;

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
    /// Absence preserves the original immutable provider binding and V2 identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<Destination>,
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

/// Bounded opaque client JSON. Only the client interprets its contents.
/// Transparent serialization preserves the existing descriptor commitment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ClientDescriptor(Value);
impl ClientDescriptor {
    /// Wrap JSON that fits in the control-message size limit.
    pub fn new(value: Value) -> Result<Self, ProtocolError> {
        let descriptor = Self(value);
        descriptor.validate()?;
        Ok(descriptor)
    }
    #[must_use]
    /// A JSON null. It still has to pass the encoded-size check when committed.
    pub const fn empty() -> Self {
        Self(Value::Null)
    }
    #[must_use]
    /// Borrow the raw JSON.
    pub fn as_value(&self) -> &Value {
        &self.0
    }
    #[must_use]
    /// Take the raw JSON.
    pub fn into_value(self) -> Value {
        self.0
    }
    /// Client-only typed interpretation; the transport does not call this method.
    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, ProtocolError> {
        self.validate()?;
        serde_json::from_value(self.0.clone())
            .map_err(|_| ProtocolError::invalid("v2.client_descriptor"))
    }
    /// Reject canonical JSON larger than the control-message limit.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    /// Domain-separated SHA-256 of this descriptor, after the size check.
    pub fn commitment(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:client-descriptor:v2", self)
    }
}
impl<'de> Deserialize<'de> for ClientDescriptor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
