//! Publisher-signed share settings: who may read, and where a Submit append goes.
//! Decided state: docs/designs/shared-database-mutation-logs.md (mutation log, proposal log, share settings).
//!
//! One statement per shared database. `read` and `write` are independent.
//! Every pair is a statement. `proposals` means a Submit member's next append
//! goes to the proposal log. `direct` means that append goes to the mutation
//! log. Entries already in the proposal log stay until the publisher settles
//! them. The statement does not look at either log.
//!
//! The publisher signs the statement on its own. A signature proves the
//! publisher wrote it. It does not prove the statement is the newest one.
//! A reader that needs that keeps the sequence it last saw.
//!
//! No stored statement means [`absent_share_settings`]: members read, and
//! members append proposals.

use super::{account_key_matches, Ed25519Signature, ShareEnvironment};
use crate::v2::{limits, Stream, Version};
use crate::{input_digest, DecimalU64, Digest, ProtocolError};
use serde::{Deserialize, Serialize};

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

/// Read and write when no [`ShareSettings`] is stored.
///
/// The pair is `(read, write)`. Every caller that has no statement uses this
/// function. The result is members and proposals, which is today's share.
#[must_use]
pub fn absent_share_settings() -> (ShareRead, ShareWrite) {
    (ShareRead::Members, ShareWrite::Proposals)
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
        account_key_matches(&self.publisher_public_key, &self.stream.scope.account_id)
    }

    /// Accept `self` only as the statement right after `previous` for this database.
    ///
    /// `previous` is absent when the database has no statement yet. The first
    /// statement has sequence one. A later statement has the next sequence
    /// number and changes `read` or `write`.
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

    /// Whether two statements set the same read and write, whatever their sequence.
    #[must_use]
    pub fn same_mode(&self, other: &Self) -> bool {
        self.read == other.read && self.write == other.write
    }

    /// Domain-separated SHA-256 of the statement with the signature removed.
    ///
    /// The canonical body is a JSON array, in order: `version`, `environment`,
    /// `stream`, `read`, `write`, `sequence`, `publisher_public_key`,
    /// `signed_at_ms`. Framing is a big-endian u32 domain length, the UTF-8
    /// domain `lastdb:share:settings:v1`, a big-endian u64 body length, and the
    /// RFC 8785 body. The result is lowercase hex.
    ///
    /// The publisher signature is Ed25519 over the UTF-8 bytes of that hex,
    /// the same bytes a member entry signs. This function does not sign.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
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
            ),
        )
    }
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
