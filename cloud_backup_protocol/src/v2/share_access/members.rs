//! Publisher-signed member list for one shared database stream.

use super::{account_key_matches, Ed25519Signature, SHARE_MEMBER_LIST_UPDATE_DOMAIN};
use crate::v2::{limits, Stream, Version};
use crate::{input_digest, DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// What one node key may do. A key has one role. v1 does not rotate keys.
///
/// `Read` is the reader capability. `Submit` is the contributor capability:
/// reader rights plus pending submissions. The log core maps `Read` to
/// `Receive`, and `Submit` to both `Receive` and `Submit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareMemberRole {
    /// Read published entries, snapshots, and files.
    Read,
    /// Read published data and submit a pending contribution.
    Submit,
}

impl ShareMemberRole {
    /// Reader capability. Wire value `read`.
    pub const READER: Self = Self::Read;
    /// Contributor capability. Wire value `submit`.
    pub const CONTRIBUTOR: Self = Self::Submit;
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

/// One node named by a member-list revision.
///
/// `state` and `regrant` are omitted from JSON at their defaults, so an
/// active member that is not a regrant keeps the original two-field object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareMember {
    /// Ed25519 node identity, lowercase hex.
    pub node_public_key: Digest,
    /// Capability granted to this key.
    pub role: ShareMemberRole,
    /// `active` or `revoked`. Omitted when active.
    #[serde(default, skip_serializing_if = "state_is_active")]
    pub state: ShareMemberState,
    /// True only on the revision that restores a revoked key. Omitted when false.
    #[serde(default, skip_serializing_if = "is_false")]
    pub regrant: bool,
}

fn state_is_active(state: &ShareMemberState) -> bool {
    *state == ShareMemberState::Active
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Signed replacement of the member set at one revision.
///
/// The service compares `revision` with the stored revision and accepts the
/// update only when [`MemberListUpdate::validate_successor_of`] succeeds.
/// Revoked keys stay in `members`. Omitting a key is not a revoke.
/// The signature is over the digest's lowercase hexadecimal bytes and is not
/// part of that digest. The member set has no maximum length. v1 does not
/// rotate content keys.
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
        if self
            .members
            .iter()
            .any(|member| member.node_public_key == self.publisher_public_key)
        {
            return Err(ProtocolError::invalid("share.members.publisher"));
        }
        account_key_matches(&self.publisher_public_key, &self.stream.scope.account_id)
    }

    /// Accept `self` only as the revision immediately after `previous`.
    ///
    /// `previous` is absent when nothing is stored yet. The signed set is the
    /// whole member set, including revoked keys. A revoked key becomes active
    /// only when that entry sets `regrant`.
    pub fn validate_successor_of(&self, previous: Option<&Self>) -> Result<(), ProtocolError> {
        super::grant::validate_successor(previous, self)
    }

    /// Domain-separated SHA-256 of the update with the signature removed.
    ///
    /// The canonical body is a JSON array, in order: `version`, `stream`,
    /// `revision`, `members`, `publisher_public_key`, `updated_at_ms`.
    /// A member object includes `state` and `regrant` only when they are not
    /// the defaults `active` and `false`.
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

/// Current revision a still-authorized node may learn.
///
/// This object names no members. The member-list read returns it unless the
/// caller is the publisher and already named this revision.
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
