//! Manage members and invitations on a signed named database over the owner socket.

use super::{read_contact, write_file};
use base64::Engine as _;
use cloud_backup_protocol::Digest;
use lastdb_node::lastdb_uds::{handler_timeout, valid_selector};
use lastdb_node::share_invitation::ShareContact;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(super) struct InviteOptions<'a> {
    pub data_dir: Option<PathBuf>,
    pub db: Option<&'a str>,
    pub recipient: &'a Path,
    pub recipient_key: String,
    pub out: &'a Path,
    pub expires_in_hours: u32,
    pub regrant: bool,
}

struct MemberAck {
    database_id: String,
    revision: u64,
    unchanged: bool,
}

pub(super) fn invite(args: InviteOptions<'_>) -> Result<(), String> {
    let selector = args.db.ok_or("share invite requires --db")?;
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let socket = owner_socket(args.data_dir)?;
    let (ack, sealed) = issue_over_uds(
        &socket,
        selector,
        "read",
        &contact,
        args.regrant,
        args.expires_in_hours,
    )?;
    write_file(args.out, &sealed)?;
    print_member("invitation", Some(args.out), &ack);
    Ok(())
}

pub(super) fn grant(args: InviteOptions<'_>) -> Result<(), String> {
    let selector = args.db.ok_or("contributor grant requires --db")?;
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let socket = owner_socket(args.data_dir)?;
    let (ack, sealed) = issue_over_uds(
        &socket,
        selector,
        "submit",
        &contact,
        args.regrant,
        args.expires_in_hours,
    )?;
    write_file(args.out, &sealed)?;
    print_member("grant", Some(args.out), &ack);
    Ok(())
}

pub(super) fn revoke(
    data_dir: Option<PathBuf>,
    db: Option<&str>,
    recipient_key: String,
) -> Result<(), String> {
    let selector = db.ok_or("contributor revoke requires --db")?;
    let node = Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
    let socket = owner_socket(data_dir)?;
    let body = json!({ "action": "revoke", "recipientKey": node });
    let reply = post_member(&socket, selector, &body)?;
    let ack = parse_ack(&reply)?;
    print_member("revoked", None, &ack);
    Ok(())
}

fn owner_socket(data_dir: Option<PathBuf>) -> Result<PathBuf, String> {
    let (_home, socket) = crate::resolve_client_home_and_socket(data_dir)?;
    crate::require_reachable(&socket)?;
    Ok(socket)
}

fn issue_over_uds(
    socket: &Path,
    selector: &str,
    action: &str,
    contact: &ShareContact,
    regrant: bool,
    expires_in_hours: u32,
) -> Result<(MemberAck, Vec<u8>), String> {
    let body = json!({
        "action": action,
        "contactCode": contact.encode(),
        "recipientKey": contact.identity_public_key(),
        "regrant": regrant,
        "expiresInHours": expires_in_hours,
    });
    let reply = post_member(socket, selector, &body)?;
    let ack = parse_ack(&reply)?;
    let encoded = reply["sealed"]
        .as_str()
        .ok_or("share member response missing sealed invitation")?;
    let sealed = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| "invalid encoded invitation".to_owned())?;
    Ok((ack, sealed))
}

fn post_member(socket: &Path, selector: &str, body: &Value) -> Result<Value, String> {
    if !valid_selector(selector) {
        return Err("invalid database ID or name".into());
    }
    let path = format!("/api/databases/{selector}/share/members");
    let raw = crate::post_json_with_timeout(socket, &path, body, handler_timeout())?;
    crate::parse_json_response(&raw, "share member update").map_err(|fallback| {
        raw.split_once("\r\n\r\n")
            .and_then(|(_, b)| serde_json::from_str::<Value>(b.trim()).ok())
            .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned))
            .unwrap_or(fallback)
    })
}

fn parse_ack(reply: &Value) -> Result<MemberAck, String> {
    let database_id = reply["database_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or("share member response missing database_id")?
        .to_owned();
    let revision = reply["revision"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| reply["revision"].as_u64())
        .ok_or("share member response missing revision")?;
    let unchanged = reply["unchanged"].as_bool().unwrap_or(false);
    Ok(MemberAck {
        database_id,
        revision,
        unchanged,
    })
}

fn print_member(label: &str, path: Option<&Path>, ack: &MemberAck) {
    let mut value = json!({
        "database_id": ack.database_id,
        "revision": ack.revision.to_string(),
        "committed": true,
    });
    value[label] = match path {
        Some(path) => json!(path),
        None => json!(true),
    };
    if ack.unchanged {
        value["unchanged"] = json!(true);
    }
    println!("{value}");
}
