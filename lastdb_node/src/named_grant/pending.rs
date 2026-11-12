//! Exact signed update saved before the post. One file per database.

use super::exchange;
use super::files;
use super::session::PublisherSession;
use super::{MemberAck, MemberIntent};
use base64::Engine as _;
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::v2::share_access::{
    MemberListUpdate, ShareMemberRole, ShareMemberState,
};
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const DIRECTORY: &str = "share-members";
const FILE_LIMIT: u64 = CONTROL_JSON_MAX_BYTES * 2;

pub(super) struct Saved {
    body: Vec<u8>,
    record: PendingRecord,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingRecord {
    node_public_key: Digest,
    action: String,
    regrant: bool,
    body: String,
}

pub(super) fn load(home: &Path, database_id: &str) -> Result<Option<Saved>, String> {
    let Some(bytes) = files::read_at(&path(home, database_id)?, FILE_LIMIT)? else {
        return Ok(None);
    };
    let record: PendingRecord =
        serde_json::from_slice(&bytes).map_err(|_| "pending member update is unreadable")?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(record.body.as_bytes())
        .map_err(|_| "pending member update is unreadable")?;
    if body.len() as u64 > CONTROL_JSON_MAX_BYTES
        || serde_json::from_slice::<MemberListUpdate>(&body).is_err()
    {
        return Err("pending member update is unreadable".into());
    }
    Ok(Some(Saved { body, record }))
}

pub(super) fn store(
    home: &Path,
    database_id: &str,
    node: &Digest,
    intent: MemberIntent,
    body: &[u8],
) -> Result<(), String> {
    if body.len() as u64 > CONTROL_JSON_MAX_BYTES {
        return Err("mutation_log.transaction".into());
    }
    let (action, regrant) = action(intent);
    let record = PendingRecord {
        node_public_key: node.clone(),
        action: action.to_owned(),
        regrant,
        body: base64::engine::general_purpose::STANDARD.encode(body),
    };
    let encoded = serde_json::to_vec(&record).map_err(|_| "cannot store pending member update")?;
    files::replace(&home.join(DIRECTORY), &file_name(database_id)?, &encoded)
}

pub(super) async fn retry(
    home: &Path,
    session: &PublisherSession,
    database_id: &str,
    node: &Digest,
    intent: MemberIntent,
    saved: Saved,
) -> Result<MemberAck, String> {
    if !matches_intent(&saved, node, intent) {
        return Err("a pending member update must be acknowledged first".into());
    }
    exchange::post_saved(session, home, database_id, &saved.body).await
}

pub(super) fn remove(home: &Path, database_id: &str) -> Result<(), String> {
    files::remove_file(&path(home, database_id)?)
}

fn matches_intent(saved: &Saved, node: &Digest, intent: MemberIntent) -> bool {
    let (action, regrant) = action(intent);
    if &saved.record.node_public_key != node
        || saved.record.action != action
        || saved.record.regrant != regrant
    {
        return false;
    }
    let Ok(update) = serde_json::from_slice::<MemberListUpdate>(&saved.body) else {
        return false;
    };
    row_matches(&update, node, intent)
}

fn row_matches(update: &MemberListUpdate, node: &Digest, intent: MemberIntent) -> bool {
    let Some(member) = update
        .members
        .iter()
        .find(|member| &member.node_public_key == node)
    else {
        return false;
    };
    match intent {
        MemberIntent::Read { regrant } => {
            member.role == ShareMemberRole::Read
                && member.state == ShareMemberState::Active
                && member.regrant == regrant
        }
        MemberIntent::Submit { regrant } => {
            member.role == ShareMemberRole::Submit
                && member.state == ShareMemberState::Active
                && member.regrant == regrant
        }
        MemberIntent::Revoke => member.state == ShareMemberState::Revoked && !member.regrant,
    }
}

fn action(intent: MemberIntent) -> (&'static str, bool) {
    match intent {
        MemberIntent::Read { regrant } => ("read", regrant),
        MemberIntent::Submit { regrant } => ("submit", regrant),
        MemberIntent::Revoke => ("revoke", false),
    }
}

fn path(home: &Path, database_id: &str) -> Result<PathBuf, String> {
    Ok(home.join(DIRECTORY).join(file_name(database_id)?))
}

fn file_name(database_id: &str) -> Result<String, String> {
    let id = cloud_backup_protocol::Id::try_from(database_id.to_owned())
        .map_err(|_| "database registry is unavailable")?;
    Ok(format!("{}.json", id.as_str()))
}
