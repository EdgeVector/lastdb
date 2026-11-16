//! Share contacts, invitations, and named-database member management over the owner socket.

use super::{
    parse_json_or_error, parse_json_response, post_json_with_timeout, require_reachable,
    resolve_client_home_and_socket, write_new_private_file, DatabaseTarget, ShareCommand,
    ShareContributorCommand,
};
use base64::Engine as _;
use cloud_backup_protocol::Digest;
use lastdb_node::lastdb_uds::{handler_timeout, valid_selector};
use lastdb_node::share_invitation::{self, ShareContact, MAX_SEALED_INVITATION_BYTES};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

struct InviteOptions<'a> {
    data_dir: Option<PathBuf>,
    db: Option<&'a str>,
    recipient: &'a Path,
    recipient_key: String,
    out: &'a Path,
    expires_in_hours: u32,
    regrant: bool,
}

struct MemberAck {
    database_id: String,
    revision: u64,
    unchanged: bool,
}

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
        } => invite(InviteOptions {
            data_dir,
            db,
            recipient: &recipient,
            recipient_key,
            out: &out,
            expires_in_hours,
            regrant,
        }),
        ShareCommand::Contributors(action) => contributors(data_dir, db, action),
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
    let mut body = json!({
        "invitation": base64::engine::general_purpose::STANDARD.encode(sealed),
        "publisher": publisher,
        "name": name,
    });
    if let Some(database_id) = database_id {
        body["database_id"] = json!(database_id);
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

fn contributors(
    data_dir: Option<PathBuf>,
    db: Option<&str>,
    action: ShareContributorCommand,
) -> Result<(), String> {
    match action {
        ShareContributorCommand::Grant {
            recipient,
            recipient_key,
            out,
            regrant,
            expires_in_hours,
        } => grant(InviteOptions {
            data_dir,
            db,
            recipient: &recipient,
            recipient_key,
            out: &out,
            regrant,
            expires_in_hours: expires_in_hours.unwrap_or(24),
        }),
        ShareContributorCommand::Revoke { recipient_key } => revoke(data_dir, db, recipient_key),
    }
}

fn invite(args: InviteOptions<'_>) -> Result<(), String> {
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
    write_new_private_file(args.out, &sealed, "share output")?;
    print_member("invitation", Some(args.out), &ack);
    Ok(())
}

fn grant(args: InviteOptions<'_>) -> Result<(), String> {
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
    write_new_private_file(args.out, &sealed, "share output")?;
    print_member("grant", Some(args.out), &ack);
    Ok(())
}

fn revoke(
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
    let (_home, socket) = resolve_client_home_and_socket(data_dir)?;
    require_reachable(&socket)?;
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
    let raw = post_json_with_timeout(socket, &path, body, handler_timeout())?;
    parse_json_or_error(&raw, "share member update")
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
