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
//! are the same in DEV and PROD. Native create is a separate route on that same
//! prefix. The member-list read calls `member_list_read_view` so an authorized
//! node can learn the revision without receiving the member list. A node-key
//! revoke covers every device that signs with that key.

mod entries;
mod grant;
mod members;

pub use entries::{
    MemberEntry, MemberGetRequest, MemberGetResponse, MemberListRequest, MemberListResponse,
    MemberSetRequest, MemberSetResponse, MEMBER_LIST_PAGE_MAX, MEMBER_SET_MAX,
    SHARE_MEMBER_GET_PATH, SHARE_MEMBER_LIST_PATH, SHARE_MEMBER_SET_PATH,
};
pub use grant::{
    authorize, authorize_access_read, authorize_native_create, authorize_request,
    member_list_read_view, member_list_revision, MemberListReadView, ShareAccess,
    SignedRequestTarget,
};
pub use members::{
    Ed25519Signature, MemberListReadRequest, MemberListReadResponse, MemberListRevision,
    MemberListUpdate, ShareMember, ShareMemberRole, ShareMemberState, ShareWriteScope,
};

use crate::primitives::exact_lower_hex;
use crate::v2::{limits, Stream, Version};
use crate::{bytes_sha256, input_digest, AccountId, DecimalU64, Digest, Id, ProtocolError, Text};
use serde::{Deserialize, Serialize};

const SHARE_NODE_READ_AUTHORITY_DOMAIN: &str = "lastdb:share:node-read-authority:v1";

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
/// Read one signed ciphertext range of a record file.
pub const MUTATION_LOG_DELIVERY_OBJECT_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/delivery/object";
/// Read a pending page from the publisher-only contribution inbox.
pub const MUTATION_LOG_INBOX_LIST_PATH: &str = "/api/cloud/backups/v2/mutation-log/inbox/list";
/// Settle a durable page in the publisher-only contribution inbox.
pub const MUTATION_LOG_INBOX_SETTLE_PATH: &str = "/api/cloud/backups/v2/mutation-log/inbox/settle";

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

    /// Reject a state for another database or realm.
    pub fn bind(
        &self,
        token: &NodeReadToken,
        service: ShareEnvironment,
    ) -> Result<(), ProtocolError> {
        if self.database_id != token.database_id || self.environment != service {
            return Err(ProtocolError::unauthorized("share.cutover.identity"));
        }
        Ok(())
    }
}

/// Authenticated response while the database is still open.
///
/// This is not a [`NativeDatabase`].
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
    /// Status body for a database with no native create.
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

/// HTTP method bound into a node-read proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShareHttpMethod {
    /// HTTP POST.
    #[serde(rename = "POST")]
    Post,
}

/// Realm the signature is allowed to address.
///
/// DEV and PROD use the same routes. This value is the credential realm, not a
/// second path and not a feature flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareEnvironment {
    /// Development service.
    Dev,
    /// Production service.
    Prod,
}

impl ShareEnvironment {
    /// Wire spelling: `dev` or `prod`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }

    /// Accept only `dev` or `prod`.
    pub fn parse(value: &str) -> Result<Self, ProtocolError> {
        match value {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            _ => Err(ProtocolError::invalid("share.node_read.environment")),
        }
    }
}

/// Signed statement carried in a `LastDBNode` authorization token.
///
/// The token JSON is this object. The Ed25519 signature is the second token
/// segment, not a field here. `digest` covers the raw HTTP body, not the token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeReadToken {
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// `dev` or `prod`. The signature covers this value.
    pub environment: ShareEnvironment,
    /// Database the request addresses. It must equal `stream.stream_id`.
    pub database_id: Id,
    /// Account scope and database id.
    pub stream: Stream,
    /// HTTP method of the request.
    pub method: ShareHttpMethod,
    /// HTTP path of the request, including the `/api` prefix.
    pub path: Text<512>,
    /// Unix milliseconds when the node signed this proof.
    pub issued_at_ms: DecimalU64,
    /// 32 random bytes as 64 lowercase hex characters. One nonce per request.
    pub nonce: Text<64>,
    /// Member-list revision this request is bound to. Zero is the empty list.
    pub member_list_revision: DecimalU64,
    /// Ed25519 node key that signs this proof.
    pub node_public_key: Digest,
}

impl NodeReadToken {
    /// Check the path, time, nonce, and database binding.
    ///
    /// This does not verify the signature or consult a replay cache.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.database_id != self.stream.stream_id {
            return Err(ProtocolError::invalid("share.node_read.database_id"));
        }
        validate_path(self.path.as_str())?;
        if self.issued_at_ms.get() == 0 {
            return Err(ProtocolError::invalid("share.node_read.issued_at_ms"));
        }
        if !exact_lower_hex(self.nonce.as_str(), 64) {
            return Err(ProtocolError::invalid("share.node_read.nonce"));
        }
        limits::encoded(self, limits::CONTROL_JSON_MAX_BYTES)
    }

    /// Reject a token aimed at another realm, method, or path.
    ///
    /// `path` is the raw request path, including `/api`. It is not a rewritten
    /// route key.
    pub fn require_target(
        &self,
        service: ShareEnvironment,
        method: ShareHttpMethod,
        path: &str,
    ) -> Result<(), ProtocolError> {
        if self.environment != service {
            return Err(ProtocolError::unauthorized("share.node_read.environment"));
        }
        if self.method != method || self.path.as_str() != path {
            return Err(ProtocolError::unauthorized("share.node_read.target"));
        }
        Ok(())
    }

    /// Domain-separated SHA-256 of this statement and the raw request body.
    ///
    /// The canonical array is `version`, `database_id`, `stream`, `method`,
    /// `path`, `body_sha256`, `issued_at_ms`, `nonce`, `member_list_revision`,
    /// `node_public_key`, `environment`. `body_sha256` is SHA-256 of `body`
    /// with no domain frame. Framing is a big-endian u32 domain length, the
    /// UTF-8 domain `lastdb:share:node-read-authority:v1`, a big-endian u64
    /// body length, and the RFC 8785 array.
    pub fn digest(&self, body: &[u8]) -> Result<Digest, ProtocolError> {
        self.validate()?;
        let body_sha256 = bytes_sha256(body);
        input_digest(
            SHARE_NODE_READ_AUTHORITY_DOMAIN,
            &(
                &self.version,
                &self.database_id,
                &self.stream,
                self.method,
                self.path.as_str(),
                &body_sha256,
                self.issued_at_ms,
                self.nonce.as_str(),
                self.member_list_revision,
                &self.node_public_key,
                self.environment,
            ),
        )
    }

    /// Reject an invalid statement or a clock reading outside ±[`SHARE_REQUEST_SKEW_MS`].
    pub fn validate_fresh(&self, now_ms: u64) -> Result<(), ProtocolError> {
        self.validate()?;
        let issued = self.issued_at_ms.get();
        let earliest = now_ms.saturating_sub(SHARE_REQUEST_SKEW_MS);
        let latest = now_ms.saturating_add(SHARE_REQUEST_SKEW_MS);
        if issued < earliest || issued > latest {
            return Err(ProtocolError::unauthorized("share.node_read.issued_at_ms"));
        }
        Ok(())
    }
}

fn validate_path(path: &str) -> Result<(), ProtocolError> {
    let Some(rest) = path.strip_prefix("/api/cloud/backups/v2/") else {
        return Err(ProtocolError::invalid("share.node_read.path"));
    };
    if rest.is_empty()
        || !path.is_ascii()
        || path.contains(['?', '#', '\\'])
        || rest
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ProtocolError::invalid("share.node_read.path"));
    }
    Ok(())
}
