//! Member roles, states, and the revision a node may learn for one database.

use crate::primitives::exact_lower_hex;
use crate::v2::limits;
use crate::v2::{Stream, Version};
use crate::{DecimalU64, Id, ProtocolError};
use serde::{Deserialize, Deserializer, Serialize};

const SIGNATURE_HEX_LEN: usize = 128;

/// Lowercase hex encoding of a 64-byte Ed25519 signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Ed25519Signature(String);

impl Ed25519Signature {
    /// Borrow the 128-character hex text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Ed25519Signature {
    type Error = ProtocolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if exact_lower_hex(&value, SIGNATURE_HEX_LEN) {
            Ok(Self(value))
        } else {
            Err(ProtocolError::invalid("share.signature"))
        }
    }
}

impl<'de> Deserialize<'de> for Ed25519Signature {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// What one node key may do. A key has one role. v1 does not rotate keys.
///
/// `Read` is the reader capability. `Submit` is the contributor capability:
/// reader rights plus pending submissions. The log core maps `Read` to
/// `Receive`, and `Submit` to both capabilities for legacy log history.
/// While `write` is `proposals`, either active role may append to the proposal log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareMemberRole {
    /// Read published entries, snapshots, and files.
    Read,
    /// Read published data and carry the publisher's schema write grant.
    Submit,
}

/// Whether a member row still grants access.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareMemberState {
    /// The key may use its role.
    #[default]
    Active,
    /// The key stays on the record and is refused until an explicit regrant.
    Revoked,
}

/// Current revision a still-authorized node may learn.
///
/// This object names no members. The member-list read returns it unless the
/// caller asks for its own access view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberListRevision {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Database whose member list this revision counts.
    pub database_id: Id,
    /// Stored revision. Zero means the publisher has not written a list.
    pub revision: DecimalU64,
}

/// Ask for the current member list and revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberListReadRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database to read.
    pub stream: Stream,
}

impl MemberListReadRequest {
    /// Check the encoded size. The request carries no members.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}
