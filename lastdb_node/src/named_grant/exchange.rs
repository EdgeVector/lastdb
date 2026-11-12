//! Member routes. The API key is not attached. Success is the ack JSON.

use super::pending;
use super::proof;
use super::session::PublisherSession;
use super::successor::SignedUpdate;
use super::MemberAck;
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadResponse, MemberListRevision, SHARE_MEMBER_LIST_READ_PATH,
    SHARE_MEMBER_LIST_UPDATE_PATH,
};
use cloud_backup_protocol::{DecimalU64, ErrorCode, ProtocolError};
use reqwest::Url;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);

pub(super) async fn read_document(
    session: &PublisherSession,
    database_id: &str,
) -> Result<MemberListReadResponse, String> {
    match read_at(session, database_id, 0).await? {
        ReadBody::Document(document) => Ok(document),
        ReadBody::Revision(revision) => read_named(session, database_id, revision).await,
    }
}

pub(super) async fn commit(
    session: &PublisherSession,
    home: &Path,
    database_id: &str,
    signed: &SignedUpdate,
) -> Result<MemberAck, String> {
    finish(session, home, database_id, &signed.body, signed.revision).await
}

pub(super) async fn post_saved(
    session: &PublisherSession,
    home: &Path,
    database_id: &str,
    body: &[u8],
) -> Result<MemberAck, String> {
    let revision = signed_revision(body)?;
    finish(session, home, database_id, body, revision).await
}

async fn read_named(
    session: &PublisherSession,
    database_id: &str,
    revision: MemberListRevision,
) -> Result<MemberListReadResponse, String> {
    if revision.database_id.as_str() != database_id {
        return Err("share.grant.identity".into());
    }
    match read_at(session, database_id, revision.revision.get()).await? {
        ReadBody::Document(document) => Ok(document),
        ReadBody::Revision(_) => Err("member list read was not acknowledged".into()),
    }
}

async fn read_at(
    session: &PublisherSession,
    database_id: &str,
    revision: u64,
) -> Result<ReadBody, String> {
    let body = proof::read_body(session, database_id)?;
    let authorization = proof::authorization(session, &body, database_id, revision)?;
    let reply = post(
        session,
        SHARE_MEMBER_LIST_READ_PATH,
        &body,
        Some(authorization),
        "member list read was not acknowledged",
    )
    .await?;
    parse_read(&reply)
}

async fn finish(
    session: &PublisherSession,
    home: &Path,
    database_id: &str,
    body: &[u8],
    revision: u64,
) -> Result<MemberAck, String> {
    let reply = match post(
        session,
        SHARE_MEMBER_LIST_UPDATE_PATH,
        body,
        None,
        "member update was not acknowledged",
    )
    .await
    {
        Ok(reply) => reply,
        Err(error) => return Err(error),
    };
    match classify_update(&reply, revision) {
        UpdateClass::Ack(revision) => acknowledged(home, database_id, revision),
        UpdateClass::Refused(field) => {
            clear_pending(home, database_id)?;
            Err(field)
        }
        UpdateClass::Unknown => Err("member update was not acknowledged".into()),
    }
}

fn acknowledged(home: &Path, database_id: &str, revision: u64) -> Result<MemberAck, String> {
    clear_pending(home, database_id)?;
    Ok(MemberAck {
        database_id: database_id.to_owned(),
        revision,
        unchanged: false,
        local: false,
    })
}

fn clear_pending(home: &Path, database_id: &str) -> Result<(), String> {
    pending::remove(home, database_id)
        .map_err(|_| "pending member update could not be cleared".into())
}

async fn post(
    session: &PublisherSession,
    path: &str,
    body: &[u8],
    authorization: Option<String>,
    failure: &'static str,
) -> Result<Reply, String> {
    let failed = || failure.to_owned();
    let mut request = session
        .http
        .post(endpoint(&session.origin, path)?)
        .timeout(TIMEOUT)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_vec());
    if let Some(authorization) = authorization {
        request = request.header(reqwest::header::AUTHORIZATION, authorization);
    }
    let response = request.send().await.map_err(|_| failed())?;
    let status = response.status().as_u16();
    let bytes = read_limited(response).await.map_err(|_| failed())?;
    Ok(Reply { status, bytes })
}

async fn read_limited(response: reqwest::Response) -> Result<Vec<u8>, ()> {
    let mut body = Vec::new();
    let mut response = response;
    loop {
        let Some(chunk) = response.chunk().await.map_err(|_| ())? else {
            return Ok(body);
        };
        if body.len().saturating_add(chunk.len()) > CONTROL_JSON_MAX_BYTES as usize {
            return Err(());
        }
        body.extend_from_slice(&chunk);
    }
}

fn endpoint(origin: &Url, path: &str) -> Result<Url, String> {
    let mut url = origin.clone();
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn parse_read(reply: &Reply) -> Result<ReadBody, String> {
    if (200..300).contains(&reply.status) {
        if let Ok(document) = serde_json::from_slice::<MemberListReadResponse>(&reply.bytes) {
            return Ok(ReadBody::Document(document));
        }
        if let Ok(revision) = serde_json::from_slice::<MemberListRevision>(&reply.bytes) {
            return Ok(ReadBody::Revision(revision));
        }
        return Err("member list read was not acknowledged".into());
    }
    Err(failure_field(&reply.bytes)
        .unwrap_or_else(|| "member list read was not acknowledged".into()))
}

fn classify_update(reply: &Reply, expected: u64) -> UpdateClass {
    if (200..300).contains(&reply.status) {
        return match serde_json::from_slice::<AckBody>(&reply.bytes) {
            Ok(ack) if ack.committed && ack.revision.get() == expected => {
                UpdateClass::Ack(expected)
            }
            _ => UpdateClass::Unknown,
        };
    }
    match failure(&reply.bytes) {
        Some(error) if definitive(error.code) => UpdateClass::Refused(error.field),
        _ => UpdateClass::Unknown,
    }
}

fn failure_field(bytes: &[u8]) -> Option<String> {
    failure(bytes).map(|error| error.field)
}

fn failure(bytes: &[u8]) -> Option<ProtocolError> {
    serde_json::from_slice(bytes).ok()
}

fn definitive(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::InvalidInput
            | ErrorCode::Unauthorized
            | ErrorCode::NotFound
            | ErrorCode::ResourceLimit
            | ErrorCode::IdentityConflict
    )
}

fn signed_revision(body: &[u8]) -> Result<u64, String> {
    let update: cloud_backup_protocol::v2::share_access::MemberListUpdate =
        serde_json::from_slice(body).map_err(|_| "pending member update is unreadable")?;
    Ok(update.revision.get())
}

enum ReadBody {
    Document(MemberListReadResponse),
    Revision(MemberListRevision),
}

enum UpdateClass {
    Ack(u64),
    Refused(String),
    Unknown,
}

struct Reply {
    status: u16,
    bytes: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AckBody {
    revision: DecimalU64,
    committed: bool,
}
