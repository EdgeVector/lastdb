//! Node-key grants stored on this machine when no Exemem account is saved.
//!
//! The file is `<home>/databases/<id>/contributors.json`, beside the store.
//! A saved Exemem session does not use this file.

use super::{MemberAck, MemberIntent};
use cloud_backup_protocol::Digest;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

const FORMAT: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    node_key: String,
    role: String,
    status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    format: u32,
    revision: u64,
    members: Vec<Member>,
}

pub(super) fn update(
    home: &Path,
    database_id: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    let mut file = load(home, database_id)?;
    let changed = apply(&mut file, node.as_str(), intent)?;
    if changed {
        file.revision = file
            .revision
            .checked_add(1)
            .ok_or("contributor grant revision overflow")?;
        write(home, database_id, &file)?;
    }
    Ok(MemberAck {
        database_id: database_id.to_owned(),
        revision: file.revision,
        unchanged: !changed,
        local: true,
    })
}

pub fn list(home: &Path, selector: &str) -> Result<serde_json::Value, String> {
    let entry = crate::host::owned::find_shareable(home, selector)?;
    let file = load(home, &entry.id)?;
    Ok(serde_json::json!({
        "database_id": entry.id,
        "database_name": entry.name,
        "revision": file.revision,
        "members": file.members.iter().map(|member| {
            serde_json::json!({
                "node_key": member.node_key,
                "role": member.role,
                "status": member.status,
            })
        }).collect::<Vec<_>>(),
    }))
}

fn apply(file: &mut File, node_key: &str, intent: MemberIntent) -> Result<bool, String> {
    let position = file
        .members
        .iter()
        .position(|member| member.node_key == node_key);
    match intent {
        MemberIntent::Revoke => revoke(file, position),
        MemberIntent::Submit { regrant } => grant(file, position, node_key, "contributor", regrant),
        MemberIntent::Read { regrant } => grant(file, position, node_key, "reader", regrant),
    }
}

fn grant(
    file: &mut File,
    position: Option<usize>,
    node_key: &str,
    role: &str,
    regrant: bool,
) -> Result<bool, String> {
    let Some(position) = position else {
        file.members.push(Member {
            node_key: node_key.to_owned(),
            role: role.to_owned(),
            status: "granted".to_owned(),
        });
        return Ok(true);
    };
    let member = &mut file.members[position];
    if member.status == "revoked" && !regrant {
        return Err("contributor grant is revoked".into());
    }
    if member.status == "granted" && member.role == role {
        return Ok(false);
    }
    member.role = role.to_owned();
    member.status = "granted".to_owned();
    Ok(true)
}

fn revoke(file: &mut File, position: Option<usize>) -> Result<bool, String> {
    let Some(position) = position else {
        return Err("contributor grant is absent".into());
    };
    let member = &mut file.members[position];
    if member.status == "revoked" {
        return Ok(false);
    }
    member.status = "revoked".to_owned();
    Ok(true)
}

fn load(home: &Path, database_id: &str) -> Result<File, String> {
    let path = members_path(home, database_id)?;
    if !path.is_file() {
        return Ok(File {
            format: FORMAT,
            revision: 0,
            members: Vec::new(),
        });
    }
    let bytes =
        std::fs::read(&path).map_err(|error| format!("read contributor grants: {error}"))?;
    let file: File = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid contributor grants: {error}"))?;
    if file.format != FORMAT {
        return Err("unsupported contributor grant format".into());
    }
    Ok(file)
}

fn write(home: &Path, database_id: &str, file: &File) -> Result<(), String> {
    let path = members_path(home, database_id)?;
    let dir = path
        .parent()
        .ok_or("contributor grant path has no directory")?;
    std::fs::create_dir_all(dir)
        .map_err(|error| format!("create contributor grant dir: {error}"))?;
    let temporary = dir.join(format!("contributors.json.tmp.{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(file)
        .map_err(|error| format!("encode contributor grants: {error}"))?;
    let write_result = (|| {
        let mut handle = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)
            .map_err(|error| format!("write contributor grants: {error}"))?;
        handle
            .write_all(&bytes)
            .map_err(|error| format!("write contributor grants: {error}"))?;
        handle
            .sync_all()
            .map_err(|error| format!("sync contributor grants: {error}"))?;
        std::fs::rename(&temporary, &path)
            .map_err(|error| format!("rename contributor grants: {error}"))?;
        std::fs::File::open(dir)
            .and_then(|handle| handle.sync_all())
            .map_err(|error| format!("sync contributor grant dir: {error}"))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write_result
}

fn members_path(home: &Path, database_id: &str) -> Result<PathBuf, String> {
    if database_id.is_empty()
        || database_id.contains('/')
        || database_id.contains('\\')
        || database_id.contains("..")
    {
        return Err("database registry is unavailable".into());
    }
    Ok(home
        .join("databases")
        .join(database_id)
        .join("contributors.json"))
}
