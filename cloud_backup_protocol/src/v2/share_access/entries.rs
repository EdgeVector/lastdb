//! Publisher-signed member entries: one signed statement per member.
//!
//! An entry says what one node key may do in one shared database. The
//! publisher signs each entry on its own, so a change to one member is one
//! small statement and the size of the member set bounds nothing. The service
//! stores the latest entry of each member and accepts a new one only as the
//! next sequence number of that member.
//!
//! A signature proves the publisher wrote the entry. It does not prove the
//! entry is the newest one. A reader that needs that keeps the sequence it
//! last saw.

use super::{
    account_key_matches, Ed25519Signature, ShareEnvironment, ShareMemberRole, ShareMemberState,
};
use crate::v2::{limits, Stream, Version};
use crate::{input_digest, DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

const SHARE_MEMBER_ENTRY_DOMAIN: &str = "lastdb:share:member-entry:v1";

/// Complete HTTP path for a publisher write of signed member entries.
pub const SHARE_MEMBER_SET_PATH: &str = "/api/cloud/backups/v2/share/members/set";
/// Complete HTTP path for a read of one member entry.
pub const SHARE_MEMBER_GET_PATH: &str = "/api/cloud/backups/v2/share/members/get";
/// Complete HTTP path for a publisher's paged read of every member entry.
pub const SHARE_MEMBER_LIST_PATH: &str = "/api/cloud/backups/v2/share/members/list";

/// Most entries in one [`MemberSetRequest`]. The service commits them in one
/// transaction of 100 writes, beside its own two.
pub const MEMBER_SET_MAX: usize = 98;
/// Most entries in one [`MemberListResponse`] page.
pub const MEMBER_LIST_PAGE_MAX: usize = 200;

/// What one node key may do in one shared database, signed by the publisher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberEntry {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// `dev` or `prod`. The signature covers this value, so an entry signed
    /// for one realm is not an entry in the other.
    pub environment: ShareEnvironment,
    /// Shared database. `stream_id` is the database id.
    pub stream: Stream,
    /// Ed25519 node identity of the member, lowercase hex.
    pub node_public_key: Digest,
    /// Capability granted to this key.
    pub role: ShareMemberRole,
    /// `active` or `revoked`. Omitted when active.
    #[serde(default, skip_serializing_if = "state_is_active")]
    pub state: ShareMemberState,
    /// Count of signed entries for this member, starting at one.
    pub sequence: DecimalU64,
    /// Publisher Ed25519 key. It must be the stream account key.
    pub publisher_public_key: Digest,
    /// Unix milliseconds when the publisher signed this entry.
    pub signed_at_ms: DecimalU64,
    /// Publisher signature over the digest hex.
    pub signature: Ed25519Signature,
}

fn state_is_active(state: &ShareMemberState) -> bool {
    *state == ShareMemberState::Active
}

impl MemberEntry {
    /// Check the sequence, time, and publisher account.
    ///
    /// This does not verify the signature.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.sequence.get() == 0 {
            return Err(ProtocolError::invalid("share.member.sequence"));
        }
        if self.signed_at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.member.signed_at_ms"));
        }
        if self.node_public_key == self.publisher_public_key {
            return Err(ProtocolError::invalid("share.members.publisher"));
        }
        account_key_matches(&self.publisher_public_key, &self.stream.scope.account_id)
    }

    /// Whether this entry lets its key use its role.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state == ShareMemberState::Active
    }

    /// Accept `self` only as the entry right after `previous` for the same member.
    ///
    /// `previous` is absent when the member has no entry yet. The first entry
    /// is active and has sequence one. A later entry has the next sequence
    /// number and changes the role or the state.
    pub fn validate_successor_of(&self, previous: Option<&Self>) -> Result<(), ProtocolError> {
        self.validate()?;
        let Some(previous) = previous else {
            if self.sequence.get() != 1 {
                return Err(ProtocolError::invalid("share.member.sequence"));
            }
            if !self.is_active() {
                return Err(ProtocolError::invalid("share.grant.revoke_unknown"));
            }
            return Ok(());
        };
        if self.environment != previous.environment
            || self.stream != previous.stream
            || self.node_public_key != previous.node_public_key
            || self.publisher_public_key != previous.publisher_public_key
        {
            return Err(ProtocolError::invalid("share.grant.identity"));
        }
        if previous.sequence.get().checked_add(1) != Some(self.sequence.get()) {
            return Err(ProtocolError::invalid("share.member.sequence"));
        }
        if self.same_grant(previous) {
            return Err(ProtocolError::invalid("share.member.unchanged"));
        }
        Ok(())
    }

    /// Whether two entries grant the same thing, whatever their sequence.
    #[must_use]
    pub fn same_grant(&self, other: &Self) -> bool {
        self.role == other.role && self.state == other.state
    }

    /// Domain-separated SHA-256 of the entry with the signature removed.
    ///
    /// The canonical body is a JSON array, in order: `version`, `environment`,
    /// `stream`, `node_public_key`, `role`, `state`, `sequence`,
    /// `publisher_public_key`, `signed_at_ms`. `state` is always present in
    /// the array. Framing is a big-endian u32 domain length, the UTF-8 domain
    /// `lastdb:share:member-entry:v1`, a big-endian u64 body length, and the
    /// RFC 8785 body. The result is lowercase hex.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        input_digest(
            SHARE_MEMBER_ENTRY_DOMAIN,
            &(
                &self.version,
                self.environment,
                &self.stream,
                &self.node_public_key,
                self.role,
                self.state,
                self.sequence,
                &self.publisher_public_key,
                self.signed_at_ms,
            ),
        )
    }
}

/// Publisher write of signed entries for one shared database.
///
/// Each entry carries its own signature, so the request has no other
/// authorization. The service commits all entries or none.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberSetRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database every entry belongs to.
    pub stream: Stream,
    /// One to [`MEMBER_SET_MAX`] entries, strictly increasing by node public key.
    pub entries: Vec<MemberEntry>,
}

impl MemberSetRequest {
    /// Check the count, the order, and that every entry names this stream, one realm, and one publisher.
    ///
    /// This does not verify signatures and does not compare with stored entries.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.entries.is_empty() || self.entries.len() > MEMBER_SET_MAX {
            return Err(ProtocolError::invalid("share.member_set.count"));
        }
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].node_public_key >= pair[1].node_public_key)
        {
            return Err(ProtocolError::invalid("share.members.order"));
        }
        let first = &self.entries[0];
        for entry in &self.entries {
            entry.validate()?;
            if entry.stream != self.stream
                || entry.environment != first.environment
                || entry.publisher_public_key != first.publisher_public_key
            {
                return Err(ProtocolError::invalid("share.grant.identity"));
            }
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Service acknowledgement of a [`MemberSetRequest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberSetResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Count of accepted member writes for this database after this request.
    pub revision: DecimalU64,
}

/// Ask for the stored entry of one member.
///
/// The request is signed with the `LastDBNode` scheme. The publisher may name
/// any member. Any other caller may name only its own key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberGetRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database to read.
    pub stream: Stream,
    /// Member whose entry is wanted.
    pub node_public_key: Digest,
}

/// Stored entry of one member. Absent when the key has no entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberGetResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Count of accepted member writes for this database.
    pub revision: DecimalU64,
    /// The latest signed entry of the named member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<MemberEntry>,
}

impl MemberGetResponse {
    /// Require an entry, when present, for the requested stream and member.
    ///
    /// This does not verify the signature.
    pub fn validate_for(&self, request: &MemberGetRequest) -> Result<(), ProtocolError> {
        let Some(entry) = &self.entry else {
            return Ok(());
        };
        entry.validate()?;
        if entry.stream != request.stream || entry.node_public_key != request.node_public_key {
            return Err(ProtocolError::invalid("share.member.identity"));
        }
        Ok(())
    }
}

/// Ask for one page of every member entry. Publisher only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberListRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database to read.
    pub stream: Stream,
    /// Return entries whose node public key is greater than this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Digest>,
}

/// One page of member entries, increasing by node public key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberListResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Count of accepted member writes for this database.
    pub revision: DecimalU64,
    /// At most [`MEMBER_LIST_PAGE_MAX`] entries.
    pub entries: Vec<MemberEntry>,
    /// Pass as `after` to read the next page. Absent on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<Digest>,
}

impl MemberListResponse {
    /// Require a page in order, after the requested key, for the requested stream.
    ///
    /// This does not verify signatures.
    pub fn validate_for(&self, request: &MemberListRequest) -> Result<(), ProtocolError> {
        if self.entries.len() > MEMBER_LIST_PAGE_MAX {
            return Err(ProtocolError::invalid("share.member_list.count"));
        }
        let mut last = request.after.as_ref();
        for entry in &self.entries {
            entry.validate()?;
            if entry.stream != request.stream
                || last.is_some_and(|key| key >= &entry.node_public_key)
            {
                return Err(ProtocolError::invalid("share.members.order"));
            }
            last = Some(&entry.node_public_key);
        }
        // The cursor is the last row the service read. That row can be one
        // with no entry, so it is at or after the last entry of the page.
        if self
            .next
            .as_ref()
            .is_some_and(|next| last.is_some_and(|key| next < key))
        {
            return Err(ProtocolError::invalid("share.member_list.next"));
        }
        Ok(())
    }
}
