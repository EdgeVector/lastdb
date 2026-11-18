//! Successor rules and route authorization for a publisher-signed member list.
//!
//! The signed document is the whole member set. A revoked key stays on that
//! set until a later revision marks it active and sets `regrant`.

use super::succession::{self, member_role};
use super::{
    account_key_matches, MemberListReadResponse, MemberListRevision, MemberListUpdate,
    NativeDatabase, NodeReadToken, ShareEnvironment, ShareHttpMethod, ShareMemberRole, ShareRead,
    ShareSettings, MUTATION_LOG_DELIVERY_OBJECT_PATH, MUTATION_LOG_INBOX_LIST_PATH,
    MUTATION_LOG_INBOX_SETTLE_PATH, MUTATION_LOG_SNAPSHOT_BYTES_PATH, SHARE_MEMBER_GET_PATH,
    SHARE_MEMBER_LIST_READ_PATH, SHARE_SETTINGS_GET_PATH, SHARE_SETTINGS_SET_PATH,
};
use crate::{Digest, ErrorCode, ProtocolError};

const MUTATION_LOG_CREATE_DATABASE_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/create-database";
const MUTATION_LOG_ACCESS_PATH: &str = "/api/cloud/backups/v2/mutation-log/access";

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
    /// Stored native database. Absent while the database is open.
    pub access: Option<&'a NativeDatabase>,
}

/// What a node key may do with the current member list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShareAccess {
    /// Account key for this database. It is not a member row.
    Publisher,
    /// Active member with no schema writes granted.
    Reader,
    /// Active member with a contributor grant. Whole-database proposal-log append
    /// access depends on active membership for both roles.
    Contributor,
}

/// Body of a member-list read after the current grant allows the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberListReadView {
    /// Publisher named the stored revision. Return the stored member document.
    Document,
    /// Authorized caller. Return [`MemberListRevision`] and no members.
    RevisionOnly,
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
    if let Some(state) = target.access {
        state.bind(token, target.environment)
    } else if token.member_list_revision.get() != 0 {
        Err(ProtocolError::invalid("share.create.revision"))
    } else {
        Ok(())
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

pub(super) fn validate_successor(
    previous: Option<&MemberListUpdate>,
    next: &MemberListUpdate,
) -> Result<(), ProtocolError> {
    next.validate()?;
    let Some(previous) = previous else {
        if next.revision.get() != 1 {
            return Err(ProtocolError::invalid("share.grant.revision"));
        }
        return succession::require_initial(next);
    };
    previous.validate()?;
    if next.stream != previous.stream || next.publisher_public_key != previous.publisher_public_key
    {
        return Err(ProtocolError::invalid("share.grant.identity"));
    }
    let Some(expected) = previous.revision.get().checked_add(1) else {
        return Err(ProtocolError::invalid("share.grant.revision"));
    };
    if next.revision.get() != expected {
        return Err(ProtocolError::invalid("share.grant.revision"));
    }
    succession::walk(&previous.members, &next.members)
}

/// Resolve the current grant for `node`.
///
/// The account key is the publisher even when no list is stored. A revoked
/// row and a missing row are unauthorized. One node key covers every device
/// that signs with it. Revoking the key refuses all of those devices.
/// `DeviceCursor` is not a second grant.
pub fn authorize(
    current: &MemberListReadResponse,
    node: &Digest,
) -> Result<ShareAccess, ProtocolError> {
    if let Some(update) = &current.update {
        update.validate()?;
        if update.stream != current.stream || update.revision != current.revision {
            return Err(ProtocolError::invalid("share.member_list.read.identity"));
        }
    } else if current.revision.get() != 0 {
        return Err(ProtocolError::invalid("share.member_list.read.update"));
    }
    if account_key_matches(node, &current.stream.scope.account_id).is_ok() {
        return Ok(ShareAccess::Publisher);
    }
    let Some(update) = &current.update else {
        return Err(ProtocolError::unauthorized("share.grant.absent"));
    };
    match member_role(update, node)? {
        ShareMemberRole::Read => Ok(ShareAccess::Reader),
        ShareMemberRole::Submit => Ok(ShareAccess::Contributor),
    }
}

fn bind_request_scope(
    token: &NodeReadToken,
    current: &MemberListReadResponse,
) -> Result<(), ProtocolError> {
    if token.database_id != token.stream.stream_id || token.stream != current.stream {
        return Err(ProtocolError::unauthorized("share.node_read.scope"));
    }
    Ok(())
}

/// Authorize a mutation-log request against the current grant.
///
/// The signature check and the nonce spend happen before this call. They are
/// not the grant check. The database must already be native.
/// The signed revision must be the stored revision. The member-list read does
/// not use this function.
pub fn authorize_request(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<ShareAccess, ProtocolError> {
    require_signed(token, &target)?;
    if !is_mutation_log_path(target.path) {
        return Err(ProtocolError::invalid("share.node_read.route"));
    }
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    if token.member_list_revision != current.revision {
        return Err(ProtocolError::unauthorized("share.node_read.revision"));
    }
    let access = authorize(current, &token.node_public_key)?;
    if matches!(
        target.path,
        MUTATION_LOG_INBOX_LIST_PATH | MUTATION_LOG_INBOX_SETTLE_PATH
    ) && access != ShareAccess::Publisher
    {
        return Err(ProtocolError::unauthorized("share.inbox.publisher"));
    }
    Ok(access)
}

/// Choose the member-list read body.
///
/// The database must already be native. A stale revision does not
/// reject this route. Only a publisher who named the stored revision receives
/// the member document. Every other authorized node receives the revision
/// alone. An unauthorized node receives neither.
pub fn member_list_read_view(
    current: &MemberListReadResponse,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<MemberListReadView, ProtocolError> {
    require_signed(token, &target)?;
    if target.method != ShareHttpMethod::Post || target.path != SHARE_MEMBER_LIST_READ_PATH {
        return Err(ProtocolError::invalid("share.node_read.route"));
    }
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    let access = authorize(current, &token.node_public_key)?;
    if access == ShareAccess::Publisher && token.member_list_revision == current.revision {
        return Ok(MemberListReadView::Document);
    }
    Ok(MemberListReadView::RevisionOnly)
}

/// Authorize `POST` [`SHARE_SETTINGS_SET_PATH`].
///
/// This sits beside member set ([`super::SHARE_MEMBER_SET_PATH`]): the statement's
/// publisher signature is the authorization, and this crate does not verify
/// it. Verify the signature over the digest hex before this call. There is
/// no [`NodeReadToken`] grant. A member signature is not this check.
///
/// `previous` is absent when the database has no settings yet.
pub fn authorize_settings_set(
    previous: Option<&ShareSettings>,
    next: &ShareSettings,
    method: ShareHttpMethod,
    path: &str,
) -> Result<(), ProtocolError> {
    require_post(method, path, SHARE_SETTINGS_SET_PATH)?;
    next.validate_successor_of(previous)
}

/// Authorize `POST` [`SHARE_SETTINGS_GET_PATH`].
///
/// The native-database requirement sits beside [`member_list_read_view`].
/// A stale member-list revision does not reject the call, which sits beside
/// [`authorize_access_read`]. The publisher and any active member may read.
/// When `read` is `anyone`, a caller [`authorize`] refuses for membership may
/// also read. An invalid roster is still refused. The caller still presents
/// a `LastDBNode` token bound to this database. The signature check and the
/// nonce spend happen before this call.
pub fn authorize_settings_get(
    current: &MemberListReadResponse,
    settings: Option<&ShareSettings>,
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
) -> Result<(), ProtocolError> {
    require_member_route(token, target, current, SHARE_SETTINGS_GET_PATH)?;
    bind_settings(settings, current, target.environment)?;
    match authorize(current, &token.node_public_key) {
        Ok(_) => Ok(()),
        Err(error) if error.code == ErrorCode::Unauthorized && settings_anyone(settings) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Authorize `POST` [`SHARE_MEMBER_GET_PATH`] for one member key.
///
/// The publisher may name any member. Any other active member may name only
/// its own key. That is the rule on [`super::MemberGetRequest`], beside
/// [`member_list_read_view`]. A revoked key stays refused.
pub fn authorize_member_get(
    current: &MemberListReadResponse,
    settings: Option<&ShareSettings>,
    token: &NodeReadToken,
    member: &Digest,
    target: SignedRequestTarget<'_>,
) -> Result<(), ProtocolError> {
    require_member_route(token, target, current, SHARE_MEMBER_GET_PATH)?;
    bind_settings(settings, current, target.environment)?;
    let access = authorize(current, &token.node_public_key)?;
    if access == ShareAccess::Publisher || token.node_public_key == *member {
        return Ok(());
    }
    Err(ProtocolError::unauthorized("share.member.other"))
}

fn require_member_route(
    token: &NodeReadToken,
    target: SignedRequestTarget<'_>,
    current: &MemberListReadResponse,
    path: &str,
) -> Result<(), ProtocolError> {
    require_post(target.method, target.path, path)?;
    token.require_target(target.environment, target.method, target.path)?;
    bind_request_scope(token, current)?;
    require_signed(token, &target)
}

fn bind_settings(
    settings: Option<&ShareSettings>,
    current: &MemberListReadResponse,
    environment: ShareEnvironment,
) -> Result<(), ProtocolError> {
    let Some(settings) = settings else {
        return Ok(());
    };
    settings.validate()?;
    if settings.stream != current.stream || settings.environment != environment {
        return Err(ProtocolError::invalid("share.settings.identity"));
    }
    Ok(())
}

fn settings_anyone(settings: Option<&ShareSettings>) -> bool {
    settings.is_some_and(|settings| settings.read == ShareRead::Anyone)
}

fn require_signed(
    token: &NodeReadToken,
    target: &SignedRequestTarget<'_>,
) -> Result<(), ProtocolError> {
    let Some(state) = target.access else {
        return Err(ProtocolError::unauthorized("share.cutover.required"));
    };
    state.bind(token, target.environment)
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

fn is_mutation_log_path(path: &str) -> bool {
    matches!(
        path,
        "/api/cloud/backups/v2/mutation-log/append"
            | "/api/cloud/backups/v2/mutation-log/head"
            | "/api/cloud/backups/v2/mutation-log/list"
            | "/api/cloud/backups/v2/mutation-log/settle-submissions"
            | MUTATION_LOG_INBOX_LIST_PATH
            | MUTATION_LOG_INBOX_SETTLE_PATH
            | MUTATION_LOG_SNAPSHOT_BYTES_PATH
            | MUTATION_LOG_DELIVERY_OBJECT_PATH
    )
}

/// Revision object for [`MemberListReadView::RevisionOnly`]. It names no members.
#[must_use]
pub fn member_list_revision(current: &MemberListReadResponse) -> MemberListRevision {
    MemberListRevision {
        version: current.version,
        database_id: current.stream.stream_id.clone(),
        revision: current.revision,
    }
}
