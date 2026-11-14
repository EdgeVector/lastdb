//! Manage members and invitations on a signed named database.

use super::{read_contact, runtime, write_file};
use base64::Engine as _;
use cloud_backup_protocol::Digest;
use lastdb_node::lastdb_uds::{handler_timeout, socket_path, valid_selector};
use lastdb_node::named_grant::{self, MemberAck, MemberIntent};
use lastdb_node::share_invitation::ShareContact;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(super) struct InviteOptions<'a> {
    pub db: Option<&'a str>,
    pub identity_home: &'a Path,
    pub recipient: &'a Path,
    pub recipient_key: String,
    pub out: &'a Path,
    pub expires_in_hours: u32,
    pub regrant: bool,
}

pub(super) fn invite(args: InviteOptions<'_>) -> Result<(), String> {
    let selector = args.db.ok_or("share invite requires --db")?;
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let (ack, sealed) = if let Some(socket) = live_socket(args.identity_home) {
        issue_over_uds(
            &socket,
            selector,
            "read",
            &contact,
            args.regrant,
            args.expires_in_hours,
        )?
    } else {
        let ack = member_update(
            args.identity_home,
            selector,
            contact.identity_public_key(),
            MemberIntent::Read {
                regrant: args.regrant,
            },
        )?;
        let sealed = seal(args.identity_home, &ack, &contact, args.expires_in_hours)?;
        (ack, sealed)
    };
    write_file(args.out, &sealed)?;
    print_member("invitation", Some(args.out), &ack);
    Ok(())
}

pub(super) fn grant(args: InviteOptions<'_>) -> Result<(), String> {
    let selector = args.db.ok_or("contributor grant requires --db")?;
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let (ack, sealed) = if let Some(socket) = live_socket(args.identity_home) {
        issue_over_uds(
            &socket,
            selector,
            "submit",
            &contact,
            args.regrant,
            args.expires_in_hours,
        )?
    } else {
        let ack = member_update(
            args.identity_home,
            selector,
            contact.identity_public_key(),
            MemberIntent::Submit {
                regrant: args.regrant,
            },
        )?;
        let sealed = seal(args.identity_home, &ack, &contact, args.expires_in_hours)?;
        (ack, sealed)
    };
    write_file(args.out, &sealed)?;
    print_member("grant", Some(args.out), &ack);
    Ok(())
}

pub(super) fn revoke(
    identity_home: &Path,
    db: Option<&str>,
    recipient_key: String,
) -> Result<(), String> {
    let selector = db.ok_or("contributor revoke requires --db")?;
    let node = Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
    let ack = if let Some(socket) = live_socket(identity_home) {
        let body = json!({ "action": "revoke", "recipientKey": node });
        let reply = post_member(&socket, selector, &body)?;
        parse_ack(&reply)?
    } else {
        member_update(identity_home, selector, &node, MemberIntent::Revoke)?
    };
    print_member("revoked", None, &ack);
    Ok(())
}

fn live_socket(identity_home: &Path) -> Option<PathBuf> {
    let mut candidates = vec![socket_path(&identity_home.join("data"))];
    if let Ok(rest) = identity_home.strip_prefix("/private/tmp") {
        candidates.push(socket_path(&Path::new("/tmp").join(rest).join("data")));
    }
    if let Ok((env_home, env_sock)) = crate::resolve_client_home_and_socket(None) {
        if env_home == identity_home
            || env_home.canonicalize().ok().as_deref() == Some(identity_home)
        {
            candidates.push(env_sock);
        }
    }
    candidates
        .into_iter()
        .find(|sock| crate::probe_health(sock).is_ok())
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

fn member_update(
    home: &Path,
    selector: &str,
    node: &Digest,
    intent: MemberIntent,
) -> Result<MemberAck, String> {
    runtime()?.block_on(named_grant::update_member(home, selector, node, intent))
}

fn seal(
    home: &Path,
    ack: &MemberAck,
    contact: &ShareContact,
    expires_in_hours: u32,
) -> Result<Vec<u8>, String> {
    runtime()?
        .block_on(named_grant::seal_named_invitation(
            home,
            &ack.database_id,
            contact,
            expires_in_hours,
        ))
        .map_err(|error| {
            format!(
                "member list committed at revision {}; {error}",
                ack.revision
            )
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
