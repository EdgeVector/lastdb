//! Named-database member commands. Publication commands stay on `--home`.

use super::{read_contact, read_file, runtime, write_file};
use cloud_backup_protocol::{Digest, Id};
use lastdb_node::named_grant::{self, MemberAck, MemberIntent};
use lastdb_node::share_contribution::{self, GrantRequest, WriteRule};
use lastdb_node::share_invitation::ShareContact;
use lastdb_node::share_publication;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::path::Path;

const MAX_INPUT_BYTES: u64 = 64 * 1024;
const GRANT_FLAGS: &str =
    "contributor grant requires --home, --rules, --operation-id, and --expected-version";

/// Parent `--db` or publication `--home`, plus the invite flags.
pub(super) struct InviteOptions<'a> {
    pub db: Option<&'a str>,
    pub identity_home: &'a Path,
    pub home: Option<&'a Path>,
    pub recipient: &'a Path,
    pub recipient_key: String,
    pub out: &'a Path,
    pub expires_in_hours: u32,
    pub regrant: bool,
}

/// Parent `--db` or publication `--home`, plus the contributor grant flags.
pub(super) struct GrantOptions<'a> {
    pub identity_home: &'a Path,
    pub db: Option<&'a str>,
    pub home: Option<&'a Path>,
    pub recipient: &'a Path,
    pub recipient_key: String,
    pub rules: Option<&'a Path>,
    pub operation_id: Option<String>,
    pub expected_version: Option<u64>,
    pub out: &'a Path,
    pub regrant: bool,
    pub expires_in_hours: Option<u32>,
}

pub(super) fn invite(args: InviteOptions<'_>) -> Result<(), String> {
    if args.db.is_some() && args.home.is_some() {
        return Err("share invite accepts --db or --home, not both".into());
    }
    let contact = read_contact(args.recipient, args.recipient_key)?;
    if let Some(selector) = args.db {
        return named_invite(
            args.identity_home,
            selector,
            &contact,
            args.out,
            args.expires_in_hours,
            args.regrant,
        );
    }
    legacy_invite(
        args.identity_home,
        args.home,
        &contact,
        args.out,
        args.expires_in_hours,
        args.regrant,
    )
}

pub(super) fn grant(args: GrantOptions<'_>) -> Result<(), String> {
    if args.db.is_some() && args.home.is_some() {
        return Err("contributor grant accepts --db or --home, not both".into());
    }
    if let Some(selector) = args.db {
        return named_grant(selector, args);
    }
    legacy_grant(args)
}

pub(super) fn revoke(
    identity_home: &Path,
    db: Option<&str>,
    home: Option<&Path>,
    recipient_key: String,
    expected_version: Option<u64>,
) -> Result<(), String> {
    if db.is_some() && home.is_some() {
        return Err("contributor revoke accepts --db or --home, not both".into());
    }
    if let Some(selector) = db {
        return named_revoke(
            identity_home,
            selector,
            recipient_key,
            home,
            expected_version,
        );
    }
    legacy_revoke(identity_home, home, recipient_key, expected_version)
}

fn named_invite(
    home: &Path,
    selector: &str,
    contact: &ShareContact,
    out: &Path,
    expires_in_hours: u32,
    regrant: bool,
) -> Result<(), String> {
    let ack = member_update(
        home,
        selector,
        contact.identity_public_key(),
        MemberIntent::Read { regrant },
    )?;
    let sealed = seal(home, &ack, contact, expires_in_hours)?;
    write_file(out, &sealed)?;
    print_member("invitation", Some(out), &ack);
    Ok(())
}

fn legacy_invite(
    identity_home: &Path,
    home: Option<&Path>,
    contact: &ShareContact,
    out: &Path,
    expires_in_hours: u32,
    regrant: bool,
) -> Result<(), String> {
    if regrant {
        return Err("--regrant applies only to a named database".into());
    }
    let home = home.ok_or("share invite requires --home")?;
    let sealed = runtime()?.block_on(share_publication::invite(
        home,
        identity_home,
        contact,
        expires_in_hours,
    ))?;
    write_file(out, &sealed)?;
    println!("{}", json!({"invitation": out}));
    Ok(())
}

fn named_grant(selector: &str, args: GrantOptions<'_>) -> Result<(), String> {
    if args.rules.is_some() || args.operation_id.is_some() || args.expected_version.is_some() {
        return Err("named database grant does not accept publication flags".into());
    }
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let ack = member_update(
        args.identity_home,
        selector,
        contact.identity_public_key(),
        MemberIntent::Submit {
            regrant: args.regrant,
        },
    )?;
    let sealed = seal(
        args.identity_home,
        &ack,
        &contact,
        args.expires_in_hours.unwrap_or(24),
    )?;
    write_file(args.out, &sealed)?;
    print_member("grant", Some(args.out), &ack);
    Ok(())
}

fn legacy_grant(args: GrantOptions<'_>) -> Result<(), String> {
    if args.regrant {
        return Err("--regrant applies only to a named database".into());
    }
    if args.expires_in_hours.is_some() {
        return Err("--expires-in-hours applies only to a named database grant".into());
    }
    let (home, rules, operation_id, expected_version) = legacy_grant_args(
        args.home,
        args.rules,
        args.operation_id,
        args.expected_version,
    )?;
    let contact = read_contact(args.recipient, args.recipient_key)?;
    let rules: Vec<WriteRule> = read_json(rules, "invalid share write rules")?;
    let sealed = runtime()?.block_on(share_contribution::grant(GrantRequest {
        home,
        owner_home: args.identity_home,
        contact: &contact,
        rules,
        operation_id,
        expected_version,
    }))?;
    write_file(args.out, &sealed)?;
    println!("{}", json!({ "grant": args.out }));
    Ok(())
}

fn legacy_grant_args<'a>(
    home: Option<&'a Path>,
    rules: Option<&'a Path>,
    operation_id: Option<String>,
    expected_version: Option<u64>,
) -> Result<(&'a Path, &'a Path, Id, u64), String> {
    let home = home.ok_or(GRANT_FLAGS)?;
    let rules = rules.ok_or(GRANT_FLAGS)?;
    let operation_id = operation_id.ok_or(GRANT_FLAGS)?;
    let expected_version = expected_version.ok_or(GRANT_FLAGS)?;
    let operation_id = Id::try_from(operation_id).map_err(|_| "invalid operation UUID")?;
    Ok((home, rules, operation_id, expected_version))
}

fn named_revoke(
    home: &Path,
    selector: &str,
    recipient_key: String,
    publication: Option<&Path>,
    expected_version: Option<u64>,
) -> Result<(), String> {
    if publication.is_some() || expected_version.is_some() {
        return Err("named database revoke does not accept publication flags".into());
    }
    let node = Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
    let ack = member_update(home, selector, &node, MemberIntent::Revoke)?;
    print_member("revoked", None, &ack);
    Ok(())
}

fn legacy_revoke(
    identity_home: &Path,
    home: Option<&Path>,
    recipient_key: String,
    expected_version: Option<u64>,
) -> Result<(), String> {
    let home = home.ok_or("contributor revoke requires --home and --expected-version")?;
    let expected_version =
        expected_version.ok_or("contributor revoke requires --home and --expected-version")?;
    let writer = Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
    share_contribution::revoke(home, identity_home, &writer, expected_version)?;
    println!("{}", json!({"revoked": true}));
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

fn read_json<T: DeserializeOwned>(path: &Path, description: &'static str) -> Result<T, String> {
    let bytes = read_file(path, MAX_INPUT_BYTES)?;
    serde_json::from_slice(&bytes).map_err(|_| description.into())
}
