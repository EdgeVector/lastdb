use super::{hash, limits};
use crate::{CloudScope, DecimalU64, Digest, Id, ProtocolError};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    pub scope: CloudScope,
    pub stream_id: Id,
}
impl Stream {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub stream: Stream,
    pub commit_id: Id,
}
impl Job {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()
    }
}

/// Immutable provider destination without any client file-kind meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Destination {
    R2,
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
    pub sha256: Digest,
    pub bytes: DecimalU64,
    /// Absence preserves the original immutable provider binding and V2 identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<Destination>,
}
impl Object {
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
    pub fn new(value: Value) -> Result<Self, ProtocolError> {
        let descriptor = Self(value);
        descriptor.validate()?;
        Ok(descriptor)
    }
    #[must_use]
    pub const fn empty() -> Self {
        Self(Value::Null)
    }
    #[must_use]
    pub fn as_value(&self) -> &Value {
        &self.0
    }
    #[must_use]
    pub fn into_value(self) -> Value {
        self.0
    }
    /// Client-only typed interpretation; the transport does not call this method.
    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, ProtocolError> {
        self.validate()?;
        serde_json::from_value(self.0.clone())
            .map_err(|_| ProtocolError::invalid("v2.client_descriptor"))
    }
    pub fn validate(&self) -> Result<(), ProtocolError> {
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
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
