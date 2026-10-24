use super::{hash, limits};
use crate::{CloudScope, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

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

/// Positive stored byte length. Provider, account and working-resource admission
/// remain runtime policy; a transport object has no native file-size ceiling.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub sha256: Digest,
    pub bytes: DecimalU64,
}
impl Object {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.get() == 0 {
            return Err(ProtocolError::invalid("v2.object.bytes"));
        }
        Ok(())
    }
}

/// Exact public UTF8 bootstrap text, never secret keys. The cloud preserves this
/// string without parsing, normalizing, or selecting client format behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDescriptor {
    pub format_id: Text<{ limits::MAX_FORMAT_ID_BYTES }>,
    pub format_version: DecimalU64,
    pub public_bootstrap: Text<{ limits::MAX_BOOTSTRAP_BYTES }>,
}
impl ClientDescriptor {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.format_version.get() == 0 {
            return Err(ProtocolError::invalid("v2.client_format_version"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
    pub fn commitment(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        hash::input("lastdb:cloud-byte-backup:client-descriptor:v2", self)
    }
}
