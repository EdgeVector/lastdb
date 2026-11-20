//! Publisher-signed share settings: who may read, and where a Submit append goes.
//! Legacy share settings. Live contract: docs/designs/cloud-api.md.
//!
//! One statement per shared database. `read` and `write` are independent.
//! Every pair is a statement. `proposals` means a Submit member's next append
//! goes to the proposal log. `direct` means that append goes to the mutation
//! log. Entries already in the proposal log stay until the publisher settles
//! them. The statement does not look at either log.
//!
//! `member_writes` says what a member with write access may do to the records
//! of each schema. The publisher's LastDB enforces that map when it checks a
//! proposal. This crate only carries and checks the statement.
//!
//! The publisher signs the statement on its own. A signature proves the
//! publisher wrote it. It does not prove the statement is the newest one.
//! A reader that needs that keeps the sequence it last saw.
//!
//! No stored statement means [`absent_share_settings`]: members read,
//! members append proposals, and the member-write map is empty.

use std::collections::BTreeMap;

use super::{account_key_matches, Ed25519Signature, ShareEnvironment};
use crate::v2::{limits, Stream, Version};
use crate::{input_digest, DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

/// Most schema names in one [`ShareSettings::member_writes`] map.
pub const MEMBER_WRITES_MAX: usize = 256;

/// Most bytes in one schema name in [`ShareSettings::member_writes`].
///
/// A name must be non-empty, without leading or trailing whitespace, and
/// without a control character. 128 bytes is the canonical schema-name cap
/// used for app grants.
///
/// A name `validate` accepts is at most 128 bytes, and `"` and `\` are the
/// characters that expand the most in JSON (two bytes each). 256 such names,
/// each mapped to `own_records`, make a [`ShareSettingsSetRequest`] of 70,635
/// bytes. [`crate::v2::limits::CONTROL_JSON_MAX_BYTES`] is 262,144.
pub const MEMBER_WRITE_SCHEMA_NAME_MAX_BYTES: usize = 128;

const SHARE_SETTINGS_DOMAIN: &str = "lastdb:share:settings:v1";

/// Complete HTTP path for a publisher write of share settings.
///
/// The operation suffix is `share/members/settings/set`. It sits under the
/// existing `share/members/{proxy+}` route. `BackupTransport` inserts `/api`.
pub const SHARE_SETTINGS_SET_PATH: &str = "/api/cloud/backups/v2/share/members/settings/set";
/// Complete HTTP path for a read of share settings.
///
/// The operation suffix is `share/members/settings/get`. It sits under the
/// existing `share/members/{proxy+}` route. `BackupTransport` inserts `/api`.
pub const SHARE_SETTINGS_GET_PATH: &str = "/api/cloud/backups/v2/share/members/settings/get";

/// Who may read the mutation log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareRead {
    /// An active member reads the mutation log. This is the absent value.
    Members,
    /// A caller with no member entry reads what a member reads and cannot append.
    Anyone,
}

/// Where a Submit member's append goes from now on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareWrite {
    /// A Submit member's next append goes to the proposal log.
    /// This is the absent value.
    Proposals,
    /// A Submit member's next append goes to the mutation log.
    Direct,
}

/// What a member with write access may do to records of one schema.
///
/// A schema that is not in [`ShareSettings::member_writes`] has
/// [`MemberWriteBehaviour::OwnerOnly`]. The same is true of every schema when
/// the map is empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberWriteBehaviour {
    /// A member cannot write records of this schema.
    OwnerOnly,
    /// The member may create, and may replace or delete only records they created.
    OwnRecords,
    /// The member may create, replace, and delete any record.
    Shared,
}

/// Read and write when no [`ShareSettings`] is stored.
///
/// The pair is `(read, write)`. Every caller that has no statement uses this
/// function. The result is members and proposals. The member-write map is
/// empty: [`member_write_behaviour`] returns [`MemberWriteBehaviour::OwnerOnly`]
/// for every schema. That empty map is also the default of
/// [`ShareSettings::member_writes`].
#[must_use]
pub fn absent_share_settings() -> (ShareRead, ShareWrite) {
    (ShareRead::Members, ShareWrite::Proposals)
}

/// Behaviour for `schema` in `member_writes`.
///
/// A name that is not in the map is [`MemberWriteBehaviour::OwnerOnly`].
/// The publisher's LastDB uses this when it checks a proposal. This function
/// does not check that the name is a canonical schema name.
#[must_use]
pub fn member_write_behaviour(
    member_writes: &BTreeMap<String, MemberWriteBehaviour>,
    schema: &str,
) -> MemberWriteBehaviour {
    member_writes
        .get(schema)
        .copied()
        .unwrap_or(MemberWriteBehaviour::OwnerOnly)
}

/// What one shared database allows, signed by the publisher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSettings {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// `dev` or `prod`. The signature covers this value, so a statement signed
    /// for one realm is not a statement in the other.
    pub environment: ShareEnvironment,
    /// Shared database. `stream_id` is the database id.
    pub stream: Stream,
    /// Who may read the mutation log.
    pub read: ShareRead,
    /// Where a Submit member's append goes from now on.
    pub write: ShareWrite,
    /// Canonical schema name to what a member with write access may do.
    ///
    /// Empty, and a name that is absent, mean [`MemberWriteBehaviour::OwnerOnly`].
    /// Omitted from JSON when empty, so a statement stored before this field
    /// still deserializes. [`Self::digest`] also omits an empty map.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub member_writes: BTreeMap<String, MemberWriteBehaviour>,
    /// Count of signed settings statements for this database, starting at one.
    pub sequence: DecimalU64,
    /// Publisher Ed25519 key. It must be the stream account key.
    pub publisher_public_key: Digest,
    /// Unix milliseconds when the publisher signed this statement.
    pub signed_at_ms: DecimalU64,
    /// Publisher signature over the digest hex.
    pub signature: Ed25519Signature,
}

impl ShareSettings {
    /// Check the sequence, the time, and the publisher account.
    ///
    /// Every `read` and `write` pair is valid. This does not verify the
    /// signature and does not look at either log.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.sequence.get() == 0 {
            return Err(ProtocolError::invalid("share.settings.sequence"));
        }
        if self.signed_at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.settings.signed_at_ms"));
        }
        validate_member_writes(&self.member_writes)?;
        account_key_matches(&self.publisher_public_key, &self.stream.scope.account_id)
    }

    /// Accept `self` only as the statement right after `previous` for this database.
    ///
    /// `previous` is absent when the database has no statement yet. The first
    /// statement has sequence one. A later statement has the next sequence
    /// number and changes `read`, `write`, or [`Self::member_writes`].
    pub fn validate_successor_of(&self, previous: Option<&Self>) -> Result<(), ProtocolError> {
        self.validate()?;
        let Some(previous) = previous else {
            if self.sequence.get() != 1 {
                return Err(ProtocolError::invalid("share.settings.sequence"));
            }
            return Ok(());
        };
        if self.environment != previous.environment
            || self.stream != previous.stream
            || self.publisher_public_key != previous.publisher_public_key
        {
            return Err(ProtocolError::invalid("share.settings.identity"));
        }
        if previous.sequence.get().checked_add(1) != Some(self.sequence.get()) {
            return Err(ProtocolError::invalid("share.settings.sequence"));
        }
        if self.same_mode(previous) {
            return Err(ProtocolError::invalid("share.settings.unchanged"));
        }
        Ok(())
    }

    /// Whether two statements set the same read, write, and member-write map.
    ///
    /// Sequence, time, and signature are ignored. A successor that matches on
    /// all three is unchanged.
    #[must_use]
    pub fn same_mode(&self, other: &Self) -> bool {
        self.read == other.read
            && self.write == other.write
            && self.member_writes == other.member_writes
    }

    /// Domain-separated SHA-256 of the statement with the signature removed.
    ///
    /// The canonical body is a JSON array. When [`Self::member_writes`] is
    /// empty, the array has the same eight values, in the same order, as a
    /// statement signed before this field existed: `version`, `environment`,
    /// `stream`, `read`, `write`, `sequence`, `publisher_public_key`,
    /// `signed_at_ms`. An absent field deserializes as an empty map, so a
    /// statement already stored still produces that array and the same digest.
    /// The empty map is not encoded as `{}` or as `null`.
    ///
    /// When the map is not empty, the array has a ninth value: the map as a
    /// JSON object. RFC 8785 sorts the object's keys. Framing is unchanged: a
    /// big-endian u32 domain length, the UTF-8 domain `lastdb:share:settings:v1`,
    /// a big-endian u64 body length, and the RFC 8785 body. The result is
    /// lowercase hex.
    ///
    /// The publisher signature is Ed25519 over the UTF-8 bytes of that hex,
    /// the same bytes a member entry signs. This function does not sign.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        if self.member_writes.is_empty() {
            return input_digest(
                SHARE_SETTINGS_DOMAIN,
                &(
                    &self.version,
                    self.environment,
                    &self.stream,
                    self.read,
                    self.write,
                    self.sequence,
                    &self.publisher_public_key,
                    self.signed_at_ms,
                ),
            );
        }
        input_digest(
            SHARE_SETTINGS_DOMAIN,
            &(
                &self.version,
                self.environment,
                &self.stream,
                self.read,
                self.write,
                self.sequence,
                &self.publisher_public_key,
                self.signed_at_ms,
                &self.member_writes,
            ),
        )
    }
}

fn validate_member_writes(
    member_writes: &BTreeMap<String, MemberWriteBehaviour>,
) -> Result<(), ProtocolError> {
    if member_writes.len() > MEMBER_WRITES_MAX {
        return Err(ProtocolError::limit("share.settings.member_writes"));
    }
    for name in member_writes.keys() {
        if name.is_empty() || name.trim() != name || name.chars().any(char::is_control) {
            return Err(ProtocolError::invalid("share.settings.member_writes.name"));
        }
        if name.len() > MEMBER_WRITE_SCHEMA_NAME_MAX_BYTES {
            return Err(ProtocolError::limit("share.settings.member_writes.name"));
        }
    }
    Ok(())
}

/// Publisher write of one signed settings statement.
///
/// The statement carries its own signature, so the request has no other
/// authorization. That is the same rule as a member set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSettingsSetRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database the statement belongs to.
    pub stream: Stream,
    /// The signed statement.
    pub settings: ShareSettings,
}

impl ShareSettingsSetRequest {
    /// Check that the statement names this stream.
    ///
    /// This does not verify the signature and does not compare with the stored statement.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.settings.validate()?;
        if self.settings.stream != self.stream {
            return Err(ProtocolError::invalid("share.settings.identity"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Service acknowledgement of a [`ShareSettingsSetRequest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSettingsSetResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Sequence of the accepted statement.
    pub sequence: DecimalU64,
}

/// Ask for the stored settings of one shared database.
///
/// The request is signed with the `LastDBNode` scheme.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSettingsGetRequest {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Shared database to read.
    pub stream: Stream,
}

impl ShareSettingsGetRequest {
    /// Check the encoded size. The request carries no statement.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }
}

/// Stored settings of one database. Absent when the publisher has not written one.
///
/// Absent means [`absent_share_settings`]. `sequence` is zero in that case.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSettingsGetResponse {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Sequence of the stored statement. Zero when `settings` is absent.
    pub sequence: DecimalU64,
    /// The latest signed statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<ShareSettings>,
}

impl ShareSettingsGetResponse {
    /// Require a statement, when present, for the requested stream and sequence.
    ///
    /// This does not verify the signature.
    pub fn validate_for(&self, request: &ShareSettingsGetRequest) -> Result<(), ProtocolError> {
        let Some(settings) = &self.settings else {
            if self.sequence.get() != 0 {
                return Err(ProtocolError::invalid("share.settings.sequence"));
            }
            return Ok(());
        };
        settings.validate()?;
        if settings.stream != request.stream || settings.sequence != self.sequence {
            return Err(ProtocolError::invalid("share.settings.identity"));
        }
        Ok(())
    }
}
