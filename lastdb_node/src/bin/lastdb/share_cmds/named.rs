//! Manage members and invitations on a signed named database.

use super::{read_contact, runtime, write_file};
use cloud_backup_protocol::Digest;
use lastdb_node::named_grant::{self, MemberAck, MemberIntent};
use lastdb_node::share_invitation::ShareContact;
use serde_json::json;
use std::path::Path;

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
    let ack = member_update(
        args.identity_home,
        selector,
        contact.identity_public_key(),
        MemberIntent::Read {
            regrant: args.regrant,
        },
    )?;
    let sealed = seal(args.identity_home, &ack, &contact, args.expires_in_hours)?;
    write_file(args.out, &sealed)?;
    print_member("invitation", Some(args.out), &ack);
    Ok(())
}

pub(super) fn grant(args: InviteOptions<'_>) -> Result<(), String> {
    let selector = args.db.ok_or("contributor grant requires --db")?;
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let ack = member_update(
        args.identity_home,
        selector,
        contact.identity_public_key(),
        MemberIntent::Submit {
            regrant: args.regrant,
        },
    )?;
    let sealed = seal(args.identity_home, &ack, &contact, args.expires_in_hours)?;
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
    let ack = member_update(identity_home, selector, &node, MemberIntent::Revoke)?;
    print_member("revoked", None, &ack);
    Ok(())
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
