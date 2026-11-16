//! Share access v2 wire types.
//!
//! A member list names the Ed25519 node keys that may read one shared database.
//! On a whole-database share, any active receiver may submit encrypted mutations.
//! Only the publisher may read or settle the pending log. Receivers follow the
//! published log, and the publisher's LastDB checks schema write permissions.
//! The publisher signs each new revision. A revoked key stays
//! on that revision until a later revision sets `regrant`. v1 does not rotate
//! keys. These types do no I/O, do not verify signatures, and do not cap the
//! member count.
//!
//! A signed request uses the `LastDBNode` authorization scheme. The token is
//! `NodeReadToken` JSON. The second segment is the raw 64-byte Ed25519
//! signature over `NodeReadToken::digest` of the exact HTTP body. The signing
//! key is the node key in that token. It is not the publisher account key.
//! Data routes call `authorize_request` on the mutation-log paths. Those paths
//! are the same in DEV and PROD. A native create and a legacy cutover are
//! separate routes on that same prefix. The member-list read calls
//! `member_list_read_view` so an authorized node can learn the revision
//! without receiving the member list. A node-key revoke covers every device
//! that signs with that key. The body-less proof remains only while that
//! database is still open.

mod grant;
mod members;
mod proof;

pub use grant::{
    authorize, authorize_request, member_list_read_view, member_list_revision, MemberListReadView,
    ShareAccess,
};
pub use members::{
    AllShareSchemas, MemberListReadRequest, MemberListReadResponse, MemberListRevision,
    MemberListUpdate, ShareMember, ShareMemberRole, ShareMemberState, ShareWriteScope,
};
pub use proof::{NodeReadToken, ShareEnvironment, ShareHttpMethod};

use crate::primitives::exact_lower_hex;
use crate::v2::Version;
use crate::{AccountId, Digest, Id, ProtocolError};
use grant::bind_request_scope;
use serde::{Deserialize, Deserializer, Serialize};

const SHARE_MEMBER_LIST_UPDATE_DOMAIN: &str = "lastdb:share:member-list-update:v1";
const SHARE_NODE_READ_AUTHORITY_DOMAIN: &str = "lastdb:share:node-read-authority:v1";
const MUTATION_LOG_CREATE_DATABASE_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/create-database";
const MUTATION_LOG_ACCESS_PATH: &str = "/api/cloud/backups/v2/mutation-log/access";

/// Authorization scheme for [`NodeReadToken`]. Bearer share passes are not this scheme.
pub const SHARE_NODE_AUTHORIZATION_SCHEME: &str = "LastDBNode";
/// Symmetric clock skew for a signed share request, in milliseconds.
pub const SHARE_REQUEST_SKEW_MS: u64 = 60_000;

/// Complete HTTP path for a publisher member-list update.
///
/// `BackupTransport` inserts `/api` itself, so the operation suffix is
/// `cloud/backups/v2/share/members/update`.
pub const SHARE_MEMBER_LIST_UPDATE_PATH: &str = "/api/cloud/backups/v2/share/members/update";
/// Complete HTTP path for a member-list read.
///
/// `BackupTransport` inserts `/api` itself, so the operation suffix is
/// `cloud/backups/v2/share/members/read`.
pub const SHARE_MEMBER_LIST_READ_PATH: &str = "/api/cloud/backups/v2/share/members/read";

/// Read or upload signed cold-blob bytes and account context.
pub const MUTATION_LOG_BLOB_BYTES_PATH: &str = "/api/cloud/backups/v2/mutation-log/blob/bytes";
/// Read or publish the reference to a client-uploaded encrypted snapshot.
pub const MUTATION_LOG_SNAPSHOT_BYTES_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/snapshot/bytes";
/// Read a signed snapshot manifest or part.
pub const MUTATION_LOG_DELIVERY_SNAPSHOT_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/delivery/snapshot";
/// Read one signed ciphertext range of a record file.
pub const MUTATION_LOG_DELIVERY_OBJECT_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/delivery/object";
/// Read a pending page from the publisher-only contribution inbox.
pub const MUTATION_LOG_INBOX_LIST_PATH: &str = "/api/cloud/backups/v2/mutation-log/inbox/list";
/// Settle a durable page in the publisher-only contribution inbox.
pub const MUTATION_LOG_INBOX_SETTLE_PATH: &str = "/api/cloud/backups/v2/mutation-log/inbox/settle";

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

/// The publisher key is the account key: the first 16 bytes of SHA-256 over
/// the raw 32-byte key equal `account`.
pub(super) fn account_key_matches(
    public_key: &Digest,
    account: &AccountId,
) -> Result<(), ProtocolError> {
    crate::verify_account_public_key(public_key, account, "share.publisher_public_key")
}

/// `native`. Not `open`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeMark {
    /// Fresh signed database.
    Native,
}

/// `open`. This tag does not select a signed digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenMark {
    /// No native row.
    Open,
}

/// Native create response. No migration evidence and no service signature.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDatabase {
    /// Always `native`. This object is not an open database and not a receipt.
    pub state: NativeMark,
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Realm of the create. A DEV database is not a PROD database.
    pub environment: ShareEnvironment,
    /// Database the create inserted.
    pub database_id: Id,
}

impl NativeDatabase {
    /// Response body for a successful native create.
    #[must_use]
    pub fn new(environment: ShareEnvironment, database_id: Id) -> Self {
        Self {
            state: NativeMark::Native,
            version: Version::V2,
            environment,
            database_id,
        }
    }
}

/// Signed state of one database. Open is the absence of this value.
///
/// This enum is not the TLS body. Send [`NativeDatabase`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignedDatabase {
    /// Spec stored at create. No evidence digest.
    Native(NativeDatabase),
}

impl SignedDatabase {
    /// Database this state names.
    #[must_use]
    pub fn database_id(&self) -> &Id {
        match self {
            Self::Native(native) => &native.database_id,
        }
    }

    /// Realm this state names.
    #[must_use]
    pub fn environment(&self) -> ShareEnvironment {
        match self {
            Self::Native(native) => native.environment,
        }
    }

    /// Reject a state for another database or realm.
    pub fn bind(
        &self,
        token: &NodeReadToken,
        service: ShareEnvironment,
    ) -> Result<(), ProtocolError> {
        if self.database_id() != &token.database_id || self.environment() != service {
            return Err(ProtocolError::unauthorized("share.cutover.identity"));
        }
        Ok(())
    }
}

/// Authenticated response while the database is still open.
///
/// This is not a [`SignedDatabase`]. It does not select the raw-body proof and
/// it does not close legacy reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenDatabase {
    /// Always `open`. This object is not [`NativeDatabase`].
    pub state: OpenMark,
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Realm the service answered in.
    pub environment: ShareEnvironment,
    /// Database that is still open.
    pub database_id: Id,
}

impl OpenDatabase {
    /// Status body for a database with no native create and no cutover row.
    #[must_use]
    pub fn new(environment: ShareEnvironment, database_id: Id) -> Self {
        Self {
            state: OpenMark::Open,
            version: Version::V2,
            environment,
            database_id,
        }
    }
}

/// HTTP target of one signed request, plus the stored signed state if any.
///
/// `path` is the raw request path. The body octets are not here. The caller
/// hashed those octets with `NodeReadToken::digest` before this call.
#[derive(Clone, Copy, Debug)]
pub struct SignedRequestTarget<'a> {
    /// Realm this service is.
    pub environment: ShareEnvironment,
    /// Method the service received.
    pub method: ShareHttpMethod,
    /// Raw path the service received, including `/api`.
    pub path: &'a str,
    /// Stored native database or cutover row. Absent while the database is open.
    pub access: Option<&'a SignedDatabase>,
}

/// Authorize the first signed create, or an equal native retry.
///
/// The body signature is already checked. Success writes only in the service
/// transaction that inserts the native row. A refusal writes nothing.
pub fn authorize_native_create(
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<(), ProtocolError> {
    require_post(
        target.method,
        target.path,
        MUTATION_LOG_CREATE_DATABASE_PATH,
    )?;
    token.require_target(target.environment, target.method, target.path)?;
    if token.database_id != token.stream.stream_id {
        return Err(ProtocolError::unauthorized("share.node_read.scope"));
    }
    require_publisher_key(token)?;
    match target.access {
        None => {
            if token.member_list_revision.get() != 0 {
                return Err(ProtocolError::invalid("share.create.revision"));
            }
            Ok(())
        }
        Some(SignedDatabase::Native(native)) => {
            if native.database_id != token.database_id || native.environment != target.environment {
                return Err(ProtocolError::unauthorized("share.cutover.identity"));
            }
            Ok(())
        }
    }
}

/// Authorize a body-signed status read. A stale revision does not reject it.
///
/// Success is not log access.
pub fn authorize_access_read(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<(), ProtocolError> {
    require_post(target.method, target.path, MUTATION_LOG_ACCESS_PATH)?;
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    authorize(current, &token.node_public_key)?;
    if let Some(state) = target.access {
        state.bind(token, target.environment)?;
    }
    Ok(())
}

fn require_publisher_key(token: &NodeReadToken) -> Result<(), ProtocolError> {
    account_key_matches(&token.node_public_key, &token.stream.scope.account_id)
        .map_err(|_| ProtocolError::unauthorized("share.create.publisher"))
}

fn require_post(method: ShareHttpMethod, path: &str, expected: &str) -> Result<(), ProtocolError> {
    if method == ShareHttpMethod::Post && path == expected {
        Ok(())
    } else {
        Err(ProtocolError::invalid("share.node_read.route"))
    }
}
