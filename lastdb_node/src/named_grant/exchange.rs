//! Signed member entry requests to the storage service.

use super::{entries, PublisherSession};
use crate::cloud_backup_client::{authorization_post, read_bounded_bytes, PathPostTarget};
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::v2::share_access::{
    MemberEntry, MemberGetRequest, MemberGetResponse, MemberSetRequest, MemberSetResponse,
    MEMBER_SET_MAX, SHARE_MEMBER_GET_PATH, SHARE_MEMBER_SET_PATH,
};
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{Digest, Id, ProtocolError};
use reqwest::Url;
use serde::de::DeserializeOwned;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);
const UNCONFIRMED: &str = "member service did not answer";
/// Entry bytes in one write. The request limit is 256 KiB.
const SET_BYTES: usize = 200 * 1024;

/// Why a member call did not land. Callers collapse this to text.
pub(super) enum CloudRefusal {
    Unconfirmed,
    Payment,
    Service(ProtocolError),
}

/// The stored entry of one member and the database's member write count.
pub(super) async fn get(
    session: &PublisherSession,
    database_id: &str,
    node: &Digest,
) -> Result<(u64, Option<MemberEntry>), String> {
    let request = MemberGetRequest {
        version: Version::V2,
        stream: entries::stream(session, database_id)?,
        node_public_key: node.clone(),
    };
    let reply: MemberGetResponse = signed(session, database_id, SHARE_MEMBER_GET_PATH, &request)
        .await
        .map_err(member_text)?;
    reply.validate_for(&request).map_err(|error| error.field)?;
    if let Some(entry) = &reply.entry {
        entries::verify(session, database_id, entry)?;
    }
    Ok((reply.revision.get(), reply.entry))
}

/// Write signed entries. Returns the member write count after the last one.
///
/// A request carries at most [`MEMBER_SET_MAX`] entries and is atomic. More
/// entries go as several requests, in key order.
pub(super) async fn set(
    session: &PublisherSession,
    database_id: &str,
    mut signed_entries: Vec<MemberEntry>,
) -> Result<u64, String> {
    signed_entries.sort_by(|left, right| left.node_public_key.cmp(&right.node_public_key));
    let mut revision = 0;
    let mut chunk: Vec<MemberEntry> = Vec::new();
    let mut bytes = 0usize;
    for entry in signed_entries {
        let size = serde_json::to_vec(&entry)
            .map_err(|_| "cannot encode member update")?
            .len();
        // A request has a count limit and a size limit. An entry with a long
        // write scope is large, so both bound a chunk.
        if !chunk.is_empty() && (chunk.len() == MEMBER_SET_MAX || bytes + size > SET_BYTES) {
            revision = send(session, database_id, std::mem::take(&mut chunk)).await?;
            bytes = 0;
        }
        bytes += size;
        chunk.push(entry);
    }
    if !chunk.is_empty() {
        revision = send(session, database_id, chunk).await?;
    }
    Ok(revision)
}

async fn send(
    session: &PublisherSession,
    database_id: &str,
    entries: Vec<MemberEntry>,
) -> Result<u64, String> {
    let request = MemberSetRequest {
        version: Version::V2,
        stream: entries::stream(session, database_id)?,
        entries,
    };
    request.validate().map_err(|error| error.field)?;
    let body = serde_json::to_vec(&request).map_err(|_| "cannot encode member update")?;
    let reply: MemberSetResponse = post(session, SHARE_MEMBER_SET_PATH, &body, None)
        .await
        .map_err(member_text)?;
    Ok(reply.revision.get())
}

async fn signed<Request: serde::Serialize, Reply: DeserializeOwned>(
    session: &PublisherSession,
    database_id: &str,
    path: &'static str,
    request: &Request,
) -> Result<Reply, CloudRefusal> {
    let body = serde_json::to_vec(request).map_err(|_| CloudRefusal::Unconfirmed)?;
    let database = Id::try_from(database_id.to_owned()).map_err(|_| CloudRefusal::Unconfirmed)?;
    let authorization = authorization_post(
        &session.signer,
        PathPostTarget {
            scope: &session.scope,
            database_id: &database,
            environment: session.environment,
            path,
        },
        &body,
        fold_db::clock::unix_millis(),
    )
    .map_err(|_| CloudRefusal::Unconfirmed)?;
    post(session, path, &body, Some(authorization)).await
}

async fn post<Reply: DeserializeOwned>(
    session: &PublisherSession,
    path: &str,
    body: &[u8],
    authorization: Option<String>,
) -> Result<Reply, CloudRefusal> {
    let mut request = session
        .http
        .post(endpoint(&session.origin, path))
        .timeout(TIMEOUT)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_vec());
    if let Some(authorization) = authorization {
        request = request.header(reqwest::header::AUTHORIZATION, authorization);
    }
    let response = request
        .send()
        .await
        .map_err(|_| CloudRefusal::Unconfirmed)?;
    let status = response.status().as_u16();
    let bytes = read_bounded_bytes(response, CONTROL_JSON_MAX_BYTES)
        .await
        .map_err(|_| CloudRefusal::Unconfirmed)?;
    if (200..300).contains(&status) {
        return serde_json::from_slice(&bytes).map_err(|_| CloudRefusal::Unconfirmed);
    }
    if crate::cloud::is_payment_subscription_error(status, &bytes) {
        return Err(CloudRefusal::Payment);
    }
    match serde_json::from_slice::<ProtocolError>(&bytes) {
        Ok(error) => Err(CloudRefusal::Service(error)),
        Err(_) => Err(CloudRefusal::Unconfirmed),
    }
}

fn member_text(error: CloudRefusal) -> String {
    match error {
        CloudRefusal::Unconfirmed => UNCONFIRMED.into(),
        CloudRefusal::Payment => crate::cloud::PAYMENT_REQUIRED_MESSAGE.to_owned(),
        CloudRefusal::Service(error) => refusal(&error),
    }
}

/// Keep the field of a refusal a caller acts on. A stale sequence is a
/// revision conflict: another write for that member arrived first.
fn refusal(error: &ProtocolError) -> String {
    match error.field.as_str() {
        "share.member.sequence" => "share.grant.revision".into(),
        "share.grant.identity" | "share.grant.personal" | "mutation_log.transaction" => {
            error.field.clone()
        }
        _ => crate::cloud::format_protocol_error(error),
    }
}

fn endpoint(origin: &Url, path: &str) -> Url {
    let mut url = origin.clone();
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    url
}
