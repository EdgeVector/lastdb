//! Signed database state. Exemem's stored row is the authority.
//! A client may use it only as the live authenticated TLS body.

use super::grant::{authorize, bind_request_scope};
use super::proof::{NodeReadToken, ShareEnvironment, ShareHttpMethod};
use super::{MemberListReadResponse, MUTATION_LOG_ACCESS_PATH, MUTATION_LOG_CREATE_DATABASE_PATH};
use crate::v2::Version;
use crate::{DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// `native`. Not `open` or `committed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeMark {
    /// Fresh signed database.
    Native,
}

/// `committed`. Not `native`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommittedMark {
    /// Legacy migration row.
    Committed,
}

/// `open`. This tag does not select a signed digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenMark {
    /// No native row and no cutover row.
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

/// Legacy migration row. No service signature. The node proof signs the body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutoverReceipt {
    /// Always `committed`. A native database does not use this object.
    pub state: CommittedMark,
    /// Protocol version. V2 serializes this field as the JSON integer 2.
    pub version: Version,
    /// Realm of this commit. A DEV receipt is not a PROD commit.
    pub environment: ShareEnvironment,
    /// Database this commit switches.
    pub database_id: Id,
    /// SHA-256 of the cutover evidence. Not a timestamp and not a signature.
    pub evidence_sha256: Digest,
    /// Member-list revision named by that evidence. Zero is rejected.
    pub grant_revision: DecimalU64,
}

impl CutoverReceipt {
    /// Reject a zero grant revision.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.grant_revision.get() == 0 {
            return Err(ProtocolError::invalid("share.cutover.grant_revision"));
        }
        Ok(())
    }
}

/// Signed state of one database. Open is the absence of this value.
///
/// This enum is not the TLS body. Send [`NativeDatabase`] or [`CutoverReceipt`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignedDatabase {
    /// Spec stored at create. No evidence digest.
    Native(NativeDatabase),
    /// Migration row stored. Legacy data reads are closed only after this is visible.
    Committed(CutoverReceipt),
}

impl SignedDatabase {
    /// Database this state names.
    #[must_use]
    pub fn database_id(&self) -> &Id {
        match self {
            Self::Native(native) => &native.database_id,
            Self::Committed(receipt) => &receipt.database_id,
        }
    }

    /// Realm this state names.
    #[must_use]
    pub fn environment(&self) -> ShareEnvironment {
        match self {
            Self::Native(native) => native.environment,
            Self::Committed(receipt) => receipt.environment,
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
        if let Self::Committed(receipt) = self {
            receipt.validate()?;
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
/// hashed those octets with `NodeReadProof::digest` before this call.
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
        Some(SignedDatabase::Committed(_)) => Err(ProtocolError::invalid("share.create.migrated")),
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
    super::account_key_matches(&token.node_public_key, &token.stream.scope.account_id)
        .map_err(|_| ProtocolError::unauthorized("share.create.publisher"))
}

fn require_post(method: ShareHttpMethod, path: &str, expected: &str) -> Result<(), ProtocolError> {
    if method == ShareHttpMethod::Post && path == expected {
        Ok(())
    } else {
        Err(ProtocolError::invalid("share.node_read.route"))
    }
}
