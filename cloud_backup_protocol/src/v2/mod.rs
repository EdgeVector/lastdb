//! Generic byte backup transport. Pure wire validation; no I/O or admission.
//! Native formats, recovery keys and client completeness stay with the client.

use crate::{CloudScope, Id, ProtocolError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod share_access;

/// Canonical bytes of a V2 body.
pub mod hash {
    pub use crate::canonical_bytes;
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
