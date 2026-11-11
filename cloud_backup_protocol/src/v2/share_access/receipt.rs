//! Signed database state. Exemem's stored row is the authority.
//! A client may use it only as the live authenticated TLS body.

use super::grant::{authorize, bind_request_scope, ShareAccess};
use super::proof::{LegacyNodeReadProof, NodeReadToken, ShareEnvironment, ShareHttpMethod};
use super::{
    MemberListReadResponse, MUTATION_LOG_ACCESS_PATH, MUTATION_LOG_CREATE_DATABASE_PATH,
    MUTATION_LOG_CUTOVER_PATH,
};
use crate::v2::Version;
use crate::{input_digest, DecimalU64, Digest, Id, ProtocolError};
use serde::{Deserialize, Serialize};

/// Domain for [`CutoverReceipt::digest`].
///
/// The digest names the binding. It is not a service signature.
pub const SHARE_CUTOVER_RECEIPT_DOMAIN: &str = "lastdb:share:cutover-receipt:v1";

/// Which node-read digest a database accepts on data routes.
///
/// The stored state decides. The service realm does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptedProof {
    /// No native database and no cutover row. The body-less proof is the only
    /// data proof. Data routes still refuse it.
    Legacy,
    /// Native create or a stored cutover row. The raw-body proof is the only
    /// data proof.
    SignedBody,
}

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

    /// Domain-separated SHA-256 of the binding fields.
    ///
    /// The array is `version`, `environment`, `database_id`, `evidence_sha256`,
    /// `grant_revision`. `state` is not in the array. Nothing signs this digest.
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        input_digest(
            SHARE_CUTOVER_RECEIPT_DOMAIN,
            &(
                &self.version,
                self.environment,
                &self.database_id,
                &self.evidence_sha256,
                self.grant_revision,
            ),
        )
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

/// Select the only data-route digest this database accepts.
///
/// `None` is the pre-migration proof. `Some` is the raw-body proof for both a
/// native database and a committed migration. The caller still checks the
/// realm against the service it is running.
#[must_use]
pub fn accepted_proof(state: Option<&SignedDatabase>) -> AcceptedProof {
    if state.is_some() {
        AcceptedProof::SignedBody
    } else {
        AcceptedProof::Legacy
    }
}

/// Body the status route may return. It is not permission to return log bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessBody {
    /// Serialize [`OpenDatabase`]. Do not start signed sync.
    Open,
    /// Serialize the stored [`NativeDatabase`] or [`CutoverReceipt`].
    Signed,
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

/// What a publisher cutover may do to the stored row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyCutover {
    /// Write the row only if the service's own head and member revision still match.
    Store,
    /// The same receipt is already stored. Do not write again.
    AlreadyStored,
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

/// Authorize a publisher cutover. The body signature and the grant stay separate.
///
/// `Store` still needs the service's conditional write. A saved copy is not that write.
pub fn authorize_legacy_cutover(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
    receipt: &CutoverReceipt,
) -> Result<LegacyCutover, ProtocolError> {
    require_post(target.method, target.path, MUTATION_LOG_CUTOVER_PATH)?;
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    if token.member_list_revision != current.revision {
        return Err(ProtocolError::unauthorized("share.node_read.revision"));
    }
    if authorize(current, &token.node_public_key)? != ShareAccess::Publisher {
        return Err(ProtocolError::unauthorized("share.cutover.publisher"));
    }
    receipt.validate()?;
    if receipt.database_id != token.database_id || receipt.environment != target.environment {
        return Err(ProtocolError::unauthorized("share.cutover.identity"));
    }
    if receipt.grant_revision != current.revision {
        return Err(ProtocolError::unauthorized("share.cutover.revision"));
    }
    match target.access {
        None => Ok(LegacyCutover::Store),
        Some(SignedDatabase::Native(_)) => Err(ProtocolError::invalid("share.cutover.native")),
        Some(SignedDatabase::Committed(stored)) if stored == receipt => {
            Ok(LegacyCutover::AlreadyStored)
        }
        Some(SignedDatabase::Committed(_)) => Err(ProtocolError::invalid("share.cutover.conflict")),
    }
}

/// Authorize a body-signed status read. A stale revision does not reject it.
///
/// Success is not log access.
pub fn authorize_access_read(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<AccessBody, ProtocolError> {
    require_post(target.method, target.path, MUTATION_LOG_ACCESS_PATH)?;
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    authorize(current, &token.node_public_key)?;
    match target.access {
        None => Ok(AccessBody::Open),
        Some(state) => {
            state.bind(token, target.environment)?;
            Ok(AccessBody::Signed)
        }
    }
}

/// Status read for [`LegacyNodeReadProof`]. The path must be the access path.
///
/// This is the only route that still accepts that proof. Success is not log access.
pub fn authorize_legacy_status(
    current: &MemberListReadResponse,
    proof: &LegacyNodeReadProof,
    service: ShareEnvironment,
    access: Option<&SignedDatabase>,
) -> Result<AccessBody, ProtocolError> {
    if proof.method != ShareHttpMethod::Post || proof.path.as_str() != MUTATION_LOG_ACCESS_PATH {
        return Err(ProtocolError::invalid("share.node_read.route"));
    }
    proof.validate()?;
    if proof.stream != current.stream {
        return Err(ProtocolError::unauthorized("share.node_read.scope"));
    }
    authorize(current, &proof.node_public_key)?;
    match access {
        None => Ok(AccessBody::Open),
        Some(state) => {
            if state.database_id() != &proof.stream.stream_id || state.environment() != service {
                return Err(ProtocolError::unauthorized("share.cutover.identity"));
            }
            if let SignedDatabase::Committed(receipt) = state {
                receipt.validate()?;
            }
            Ok(AccessBody::Signed)
        }
    }
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
