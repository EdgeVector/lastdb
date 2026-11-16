//! Exact signed update saved before the post. One file per database.

use super::{exchange, MemberAck, MemberIntent, PublisherSession};
use crate::profile::private_fs;
use base64::Engine as _;
use cloud_backup_protocol::v2::limits::CONTROL_JSON_MAX_BYTES;
use cloud_backup_protocol::v2::share_access::{
    MemberListUpdate, ShareMember, ShareMemberRole, ShareMemberState,
};
use cloud_backup_protocol::{Digest, Id};
use serde::{Deserialize, Serialize};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const DIRECTORY: &str = "share-members";
const FILE_LIMIT: usize = (CONTROL_JSON_MAX_BYTES * 2) as usize;

pub(super) struct Saved {
    pub body: Vec<u8>,
    record: PendingRecord,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    node_public_key: Option<Digest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    regrant: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_id: Option<Id>,
    body: String,
}

pub(super) fn lock(home: &Path, database_id: &str) -> Result<std::fs::File, String> {
    let directory = home.join(DIRECTORY);
    ensure_dir(&directory)?;
    let dir = crate::offline_home::Directory::open(&directory).map_err(|e| e.to_string())?;
    let file = dir
        .open_lock(&format!("{database_id}.lock"))
        .map_err(|e| e.to_string())?;
    file.try_lock()
        .map_err(|_| "a member update is in progress")?;
    Ok(file)
}

pub(super) fn load(home: &Path, database_id: &str) -> Result<Option<Saved>, String> {
    let Some(bytes) = private_fs::read_file_limited(&path(home, database_id)?, FILE_LIMIT, false)
        .map_err(|_| "cannot read a private file".to_owned())?
    else {
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
    let (action, regrant) = action(intent);
    let record = PendingRecord {
        node_public_key: Some(node.clone()),
        action: Some(action.to_owned()),
        regrant: Some(regrant),
        request_id: None,
        body: encode(body)?,
    };
    save(home, database_id, &record)
}

pub(super) fn store_config(
    home: &Path,
    database_id: &str,
    request_id: Id,
    body: &[u8],
) -> Result<(), String> {
    let record = PendingRecord {
        node_public_key: None,
        action: None,
        regrant: None,
        request_id: Some(request_id),
        body: encode(body)?,
    };
    save(home, database_id, &record)
}

fn encode(body: &[u8]) -> Result<String, String> {
    if body.len() as u64 > CONTROL_JSON_MAX_BYTES {
        return Err("mutation_log.transaction".into());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(body))
}

fn save(home: &Path, database_id: &str, record: &PendingRecord) -> Result<(), String> {
    let encoded = serde_json::to_vec(record).map_err(|_| "cannot store pending member update")?;
    let directory = home.join(DIRECTORY);
    ensure_dir(&directory)?;
    let name = file_name(database_id)?;
    private_fs::write_private(&directory.join(name), &encoded)
        .map_err(|_| "cannot publish a private file".into())
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
    private_fs::remove_private(&path(home, database_id)?)
        .map_err(|_| "cannot remove a private file".into())
}

fn ensure_dir(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|_| "cannot create a private directory")?;
    let metadata = fs::symlink_metadata(path).map_err(|_| "cannot inspect a private directory")?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("private directory must be a real directory".into());
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|_| "cannot protect a private directory")
}

fn matches_intent(saved: &Saved, node: &Digest, intent: MemberIntent) -> bool {
    let (action, regrant) = action(intent);
    if saved.record.node_public_key.as_ref() != Some(node)
        || saved.record.action.as_deref() != Some(action)
        || saved.record.regrant != Some(regrant)
    {
        return false;
    }
    let Ok(update) = serde_json::from_slice::<MemberListUpdate>(&saved.body) else {
        return false;
    };
    row_matches(&update, node, intent)
}

impl Saved {
    pub(super) fn matches_config(
        &self,
        request_id: &Id,
        expected: u64,
        members: &[ShareMember],
    ) -> bool {
        self.record.request_id.as_ref() == Some(request_id)
            && serde_json::from_slice::<MemberListUpdate>(&self.body).is_ok_and(|update| {
                update.revision.get().checked_sub(1) == Some(expected) && update.members == members
            })
    }
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
