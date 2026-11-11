//! `LastDBNode` header for one member-list read. The update route does not use it.

use super::session::PublisherSession;
use base64::Engine as _;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, NodeReadToken, ShareHttpMethod, SHARE_MEMBER_LIST_READ_PATH,
    SHARE_NODE_AUTHORIZATION_SCHEME,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{DecimalU64, Id, Text};
use rand::{rngs::OsRng, RngCore};

pub(super) fn read_body(session: &PublisherSession, database_id: &str) -> Result<Vec<u8>, String> {
    let request = MemberListReadRequest {
        version: Version::V2,
        stream: stream(session, database_id)?,
    };
    request.validate().map_err(|error| error.field)?;
    serde_json::to_vec(&request).map_err(|_| "cannot encode member list read".into())
}

/// Sign `body` exactly. Do not serialize the request again before the post.
pub(super) fn authorization(
    session: &PublisherSession,
    body: &[u8],
    database_id: &str,
    revision: u64,
) -> Result<String, String> {
    let stream = stream(session, database_id)?;
    let token = NodeReadToken {
        version: Version::V2,
        environment: session.environment,
        database_id: stream.stream_id.clone(),
        stream,
        method: ShareHttpMethod::Post,
        path: Text::try_from(SHARE_MEMBER_LIST_READ_PATH.to_owned())
            .map_err(|_| "invalid member read path")?,
        issued_at_ms: issued_at()?,
        nonce: nonce()?,
        member_list_revision: DecimalU64::new(revision),
        node_public_key: session.publisher.clone(),
    };
    let digest = token.digest(body).map_err(|error| error.field)?;
    let signature = session.signer.sign(digest.as_str().as_bytes()).to_bytes();
    let token_json = serde_json::to_vec(&token).map_err(|_| "cannot encode member read proof")?;
    Ok(format!(
        "{SHARE_NODE_AUTHORIZATION_SCHEME} {}.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token_json),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature)
    ))
}

fn stream(session: &PublisherSession, database_id: &str) -> Result<Stream, String> {
    Ok(Stream {
        scope: session.scope.clone(),
        stream_id: Id::try_from(database_id.to_owned())
            .map_err(|_| "database registry is unavailable")?,
    })
}

fn issued_at() -> Result<DecimalU64, String> {
    let now = fold_db::clock::unix_millis();
    if now == 0 {
        return Err("invalid clock for member read".into());
    }
    Ok(DecimalU64::new(now))
}

fn nonce() -> Result<Text<64>, String> {
    let mut raw = [0u8; 32];
    OsRng.fill_bytes(&mut raw);
    Text::try_from(hex::encode(raw)).map_err(|_| "cannot encode member read nonce".into())
}
