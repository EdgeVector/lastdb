//! Route authorization helpers for signed share requests.
//!
//! Membership is decided from each member's own signed entry on the service.
//! These helpers cover native create. They do not rebuild a whole-list
//! grant document.

use super::{
    account_key_matches, NativeDatabase, NodeReadToken, ShareEnvironment, ShareHttpMethod,
};
use crate::ProtocolError;

const MUTATION_LOG_CREATE_DATABASE_PATH: &str =
    "/api/cloud/backups/v2/mutation-log/create-database";

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
    } else {
        Ok(())
    }
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
