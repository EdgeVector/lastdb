//! Share contacts and local open-link mint/accept over the owner socket.
//!
//! Sealed invitations and ExeMem share/members are deleted. Access is the
//! signed grant chain; People wraps the open link in a hand-off consent code.

use super::{
    build_post_request, parse_json_response, post_json_with_timeout, request_with_timeout,
    require_reachable, write_new_private_file, DatabaseTarget, ShareCommand,
};
use cloud_backup_protocol::Digest;
use lastdb_node::lastdb_uds::handler_timeout;
use lastdb_node::share_invitation::{self, ShareContact};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(super) fn run(
    data_dir: Option<PathBuf>,
    db: Option<&str>,
    action: ShareCommand,
) -> Result<(), String> {
    match action {
        ShareCommand::Accept { link, name } => {
            if db.is_some() {
                return Err("--db does not apply to an open read link".into());
            }
            post_accept(data_dir, &json!({ "link": link, "name": name }))
        }
        ShareCommand::Link { database } => mint_link(data_dir, &database),
        ShareCommand::Lease {
            database,
            length,
            clear,
        } => lease(data_dir, &database, length.as_deref(), clear),
        action => run_offline(data_dir, action),
    }
}

fn post_accept(data_dir: Option<PathBuf>, body: &Value) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, None)?;
    require_reachable(&target.socket)?;
    let path = target.path("/api/databases/accept");
    let raw = post_json_with_timeout(&target.socket, path.as_ref(), body, Duration::from_secs(10))?;
    println!("{}", parse_json_response(&raw, "database registration")?);
    Ok(())
}

fn mint_link(data_dir: Option<PathBuf>, database: &str) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, Some(database))?;
    require_reachable(&target.socket)?;
    let path = target.path("/api/share/link");
    let raw = request_with_timeout(
        &target.socket,
        &build_post_request(path.as_ref(), "", &[]),
        handler_timeout(),
    )?;
    println!("{}", owner_json(&raw, "open read link")?);
    Ok(())
}

/// Read or set the lease of one database on the owner socket.
fn lease(
    data_dir: Option<PathBuf>,
    database: &str,
    length: Option<&str>,
    clear: bool,
) -> Result<(), String> {
    let body = match (length, clear) {
        (Some(text), _) => json!({ "length_ms": parse_length(text)? }),
        (None, true) => json!({ "length_ms": null }),
        (None, false) => json!({}),
    };
    let target = DatabaseTarget::open(data_dir, Some(database))?;
    require_reachable(&target.socket)?;
    let path = target.path("/api/lease");
    let raw = post_json_with_timeout(&target.socket, path.as_ref(), &body, handler_timeout())?;
    println!("{}", owner_json(&raw, "lease")?);
    Ok(())
}

/// `30s`, `5m`, `24h` or `7d` in milliseconds.
fn parse_length(text: &str) -> Result<u64, String> {
    let invalid = || format!("invalid lease length {text:?}; use a number and s, m, h or d");
    let unit = text.chars().last().ok_or_else(invalid)?;
    let scale: u64 = match unit {
        's' => 1_000,
        'm' => 60_000,
        'h' => 3_600_000,
        'd' => 86_400_000,
        _ => return Err(invalid()),
    };
    let count: u64 = text[..text.len() - 1].parse().map_err(|_| invalid())?;
    count.checked_mul(scale).ok_or_else(invalid)
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
                json!({"contactCode": verified.encode(), "nodeKey": verified.identity_public_key()})
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
    write_new_private_file(out, recipient.encode().as_bytes(), "share output")?;
    println!(
        "{}",
        json!({"contact": out, "recipient_public_key": recipient.identity_public_key()})
    );
    Ok(())
}

fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    lastdb_node::profile::private_fs::read_file_limited(path, limit, false)
        .ok()
        .flatten()
        .ok_or_else(|| "cannot read share input file".into())
}

fn owner_json(raw: &str, label: &str) -> Result<Value, String> {
    parse_json_response(raw, label)
}
