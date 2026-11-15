use super::{parse_json_response, post_json_with_timeout, DatabaseTarget, ShareCommand};
use base64::Engine as _;
use cloud_backup_protocol::Digest;
use lastdb_node::share_invitation::{self, ShareContact, MAX_SEALED_INVITATION_BYTES};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[path = "share_cmds/named.rs"]
mod named;

pub(super) fn run(
    data_dir: Option<PathBuf>,
    db: Option<&str>,
    action: ShareCommand,
) -> Result<(), String> {
    match action {
        ShareCommand::Accept {
            invitation,
            publisher,
            name,
        } => accept(data_dir, db, &invitation, &publisher, name.as_deref()),
        ShareCommand::Invite {
            recipient,
            recipient_key,
            out,
            expires_in_hours,
            regrant,
        } => named::invite(named::InviteOptions {
            data_dir,
            db,
            recipient: &recipient,
            recipient_key,
            out: &out,
            expires_in_hours,
            regrant,
        }),
        ShareCommand::Contributors(action) => named::contributors(data_dir, db, action),
        action => run_offline(data_dir, action),
    }
}

fn accept(
    data_dir: Option<PathBuf>,
    database_id: Option<&str>,
    invitation: &Path,
    publisher: &str,
    name: Option<&str>,
) -> Result<(), String> {
    let sealed = read_file(invitation, MAX_SEALED_INVITATION_BYTES)?;
    let mut body = serde_json::json!({
        "invitation": base64::engine::general_purpose::STANDARD.encode(sealed),
        "publisher": publisher,
        "name": name,
    });
    if let Some(database_id) = database_id {
        body["database_id"] = serde_json::json!(database_id);
    }
    let target = DatabaseTarget::open(data_dir, None)?;
    let raw = post_json_with_timeout(
        &target.socket,
        &target.path("/api/databases/accept"),
        &body,
        Duration::from_secs(10),
    )?;
    println!("{}", parse_json_response(&raw, "database registration")?);
    Ok(())
}

fn run_offline(data_dir: Option<PathBuf>, action: ShareCommand) -> Result<(), String> {
    let data_dir = data_dir
        .map(lastdb_node::profile::paths::expand_tilde_path)
        .transpose()?;
    let identity_home = match data_dir {
        Some(dir) if dir.is_absolute() => dir
            .canonicalize()
            .map_err(|_| "share identity home must exist".to_owned())?,
        _ => return Err("share requires an explicit absolute --data-dir identity home".into()),
    };
    match action {
        ShareCommand::Contact { out } => contact(&identity_home, &out),
        ShareCommand::VerifyContact {
            contact,
            recipient_key,
        } => {
            let verified = read_contact(&contact, recipient_key)?;
            println!(
                "{}",
                serde_json::json!({"contactCode": verified.encode(), "nodeKey": verified.identity_public_key()})
            );
            Ok(())
        }
        _ => unreachable!("online share commands are routed to the node"),
    }
}

fn read_contact(path: &Path, recipient_key: String) -> Result<ShareContact, String> {
    let bytes = read_file(path, 512)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| "invalid share contact file")?;
    let contact = ShareContact::parse(text.trim()).map_err(|e| e.to_string())?;
    let expected = Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
    if &expected != contact.identity_public_key() {
        return Err("contact does not match the trusted recipient".into());
    }
    Ok(contact)
}

fn contact(home: &Path, out: &Path) -> Result<(), String> {
    let recipient = share_invitation::recipient(home)?.contact();
    write_file(out, recipient.encode().as_bytes())?;
    println!(
        "{}",
        serde_json::json!({"contact": out, "recipient_public_key": recipient.identity_public_key()})
    );
    Ok(())
}

fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    lastdb_node::profile::private_fs::read_file_limited(path, limit, false)
        .ok()
        .flatten()
        .ok_or_else(|| "cannot read share input file".into())
}

fn write_file(out: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let directory = File::open(parent).map_err(|_| "cannot open share output directory")?;
    let temporary = out.with_file_name(format!(".lastdb-share-{}.pending", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .map_err(|_| "cannot create protected share output")?;
    let result = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::hard_link(&temporary, out));
    let cleanup = std::fs::remove_file(&temporary);
    result.map_err(|_| "cannot publish share output to a new file")?;
    cleanup
        .and_then(|()| directory.sync_all())
        .map_err(|_| "cannot sync share output".into())
}
