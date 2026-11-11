//! Publisher-signed member list for one shared database stream.

use super::{account_key_matches, Ed25519Signature, SHARE_MEMBER_LIST_UPDATE_DOMAIN};
use crate::v2::{limits, Stream, Version};
use crate::{input_digest, DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

/// What one node key may do. A key has one role. v1 does not rotate keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareMemberRole {
    /// Read the shared database.
    Read,
    /// Submit to the shared database.
    Submit,
}

/// One node admitted by a member-list revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareMember {
    /// Ed25519 node identity, lowercase hex.
    pub node_public_key: Digest,
    /// Authority granted to this key.
    pub role: ShareMemberRole,
}

/// Signed replacement of the member set at one revision.
///
/// The service compares `revision` with the stored revision and accepts the
/// update only when it is exactly one greater. The signature is over the
/// digest's lowercase hexadecimal bytes and is not part of that digest.
/// The member set has no maximum length.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberListUpdate {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database. `stream_id` is the database id.
    pub stream: Stream,
    /// Monotonic revision, starting at one.
    pub revision: DecimalU64,
    /// Full member set, strictly increasing by node public key.
    pub members: Vec<ShareMember>,
    /// Publisher Ed25519 key. It must be the stream account key.
    pub publisher_public_key: Digest,
    /// Publisher signature over the digest hex.
    pub signature: Ed25519Signature,
    /// Unix milliseconds when the publisher signed this revision.
    pub updated_at_ms: DecimalU64,
}

impl MemberListUpdate {
    /// Check revision, time, publisher account, and member order.
    ///
    /// This does not verify the signature and does not limit how many members
    /// the set contains.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        if self.revision.get() == 0 {
            return Err(ProtocolError::invalid("share.member_list.revision"));
        }
        if self.updated_at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.member_list.updated_at_ms"));
        }
        validate_member_order(&self.members)?;
        account_key_matches(&self.publisher_public_key, &self.stream.scope.account_id)
    }

    /// Domain-separated SHA-256 of the update with the signature removed.
    ///
    /// The canonical body is a JSON array, in order: `version`, `stream`,
    /// `revision`, `members`, `publisher_public_key`, `updated_at_ms`.
    /// Framing is a big-endian u32 domain length, the UTF-8 domain
    /// `lastdb:share:member-list-update:v1`, a big-endian u64 body length,
    /// and the RFC 8785 body. The result is lowercase hex.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        input_digest(
            SHARE_MEMBER_LIST_UPDATE_DOMAIN,
            &(
                &self.version,
                &self.stream,
                self.revision,
                &self.members,
                &self.publisher_public_key,
                self.updated_at_ms,
            ),
        )
    }
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
    /// Check the stream and the encoded size. The request carries no members.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.stream.validate()?;
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Current member list. Revision zero has no signed update yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberListReadResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database this list belongs to.
    pub stream: Stream,
    /// Stored revision. Zero means the publisher has not written a list.
    pub revision: DecimalU64,
    /// Accepted update. Absent when `revision` is zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update: Option<MemberListUpdate>,
}

impl MemberListReadResponse {
    /// Require the same stream as the request, and an update exactly at `revision`.
    pub fn validate_for(&self, request: &MemberListReadRequest) -> Result<(), ProtocolError> {
        request.validate()?;
        self.stream.validate()?;
        if self.stream != request.stream {
            return Err(ProtocolError::invalid("share.member_list.read.stream"));
        }
        let Some(update) = &self.update else {
            if self.revision.get() != 0 {
                return Err(ProtocolError::invalid("share.member_list.read.update"));
            }
            return Ok(());
        };
        if self.revision.get() == 0 {
            return Err(ProtocolError::invalid("share.member_list.read.empty"));
        }
        update.validate()?;
        if update.stream != self.stream || update.revision != self.revision {
            return Err(ProtocolError::invalid("share.member_list.read.identity"));
        }
        Ok(())
    }
}

fn validate_member_order(members: &[ShareMember]) -> Result<(), ProtocolError> {
    if members
        .windows(2)
        .any(|pair| pair[0].node_public_key >= pair[1].node_public_key)
    {
        return Err(ProtocolError::invalid("share.members.order"));
    }
    Ok(())
}
