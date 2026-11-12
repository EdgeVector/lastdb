use super::{
    cloud_cmds::FreshHome, parse_json_response, post_json_with_timeout, DatabaseTarget,
    ShareCommand, ShareContributorCommand, ShareFilesCommand,
};
use base64::Engine as _;
use cloud_backup_protocol::Digest;
use lastdb_node::cloud_backup_client::byte_backup::ShareEnvironment;
use lastdb_node::share_invitation::{ShareContact, MAX_SEALED_INVITATION_BYTES};
use lastdb_node::share_publication;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[path = "share_cmds/contributors.rs"]
mod contributors;
#[path = "share_cmds/files/mod.rs"]
mod files;
#[path = "share_cmds/named.rs"]
mod named;
#[path = "share_cmds/queries/mod.rs"]
mod queries;

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
        ShareCommand::Files(ShareFilesCommand::Receive {
            invitation,
            publisher,
            name,
            cache: None,
            ..
        }) => accept(data_dir, None, &invitation, &publisher, name.as_deref()),
        ShareCommand::Files(action) if db.is_some() => files::run_selected(data_dir, db, action),
        ShareCommand::Contributors(ShareContributorCommand::Accept { db, grant }) => {
            let sealed = read_file(&grant, 64 * 1024)?;
            let body = serde_json::json!({
                "grant": base64::engine::general_purpose::STANDARD.encode(sealed),
            });
            database_request(data_dir, Some(&db), "/api/contributors", &body)
        }
        ShareCommand::Queries(action) => queries::run(data_dir, action),
        action => run_offline(data_dir, db, action),
    }
}

fn accept(
    data_dir: Option<PathBuf>,
    database_id: Option<&str>,
    invitation: &Path,
    publisher: &str,
    name: Option<&str>,
) -> Result<(), String> {
    let sealed = read_file(invitation, MAX_SEALED_INVITATION_BYTES as u64)?;
    let mut body = serde_json::json!({
        "invitation": base64::engine::general_purpose::STANDARD.encode(sealed),
        "publisher": publisher,
        "name": name,
    });
    if let Some(database_id) = database_id {
        body["database_id"] = serde_json::json!(database_id);
    }
    database_request(data_dir, None, "/api/databases/accept", &body)
}

fn database_request(
    data_dir: Option<PathBuf>,
    selector: Option<&str>,
    path: &str,
    body: &serde_json::Value,
) -> Result<(), String> {
    let target = DatabaseTarget::open(data_dir, selector)?;
    let raw = post_json_with_timeout(
        &target.socket,
        &target.path(path),
        body,
        Duration::from_secs(10),
    )?;
    println!("{}", parse_json_response(&raw, "database registration")?);
    Ok(())
}

fn run_offline(
    data_dir: Option<PathBuf>,
    db: Option<&str>,
    action: ShareCommand,
) -> Result<(), String> {
    let identity_home = data_dir
        .filter(|p| p.is_absolute())
        .ok_or("share requires an explicit absolute --data-dir identity home")?;
    let identity_home = identity_home
        .canonicalize()
        .map_err(|_| "share identity home must exist")?;
    match action {
        ShareCommand::Create {
            into,
            env,
            write_mode,
            spec,
            spec_version,
        } => {
            let write_mode = match write_mode.as_str() {
                "publisher-only" => share_publication::WriteMode::PublisherOnly,
                "publisher-validated" => share_publication::WriteMode::PublisherValidated,
                _ => return Err("invalid share write mode".into()),
            };
            create(
                &identity_home,
                &into,
                &env,
                write_mode,
                spec.as_deref(),
                spec_version,
            )
        }
        ShareCommand::Contact { out } => contact(&identity_home, &out),
        ShareCommand::Invite {
            home,
            recipient,
            recipient_key,
            out,
            expires_in_hours,
            regrant,
        } => named::invite(
            db,
            &identity_home,
            home.as_deref(),
            &recipient,
            recipient_key,
            &out,
            expires_in_hours,
            regrant,
        ),
        ShareCommand::Accept { .. } => unreachable!("accept is routed to the node"),
        ShareCommand::Contributors(action) => contributors::run(&identity_home, db, action),
        ShareCommand::Files(action) => files::run(&identity_home, action),
        ShareCommand::Queries(_) => unreachable!("queries are routed to the node"),
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

fn create(
    owner: &Path,
    into: &Path,
    env: &str,
    write_mode: share_publication::WriteMode,
    spec_name: Option<&str>,
    spec_version: Option<u32>,
) -> Result<(), String> {
    lastdb_node::offline_home::refuse_primary(into)?;
    if into.starts_with(owner) {
        return Err("publication home must be separate from the identity home".into());
    }
    let target = FreshHome::preflight_empty(into)?;
    let environment = match env {
        "dev" => ShareEnvironment::Dev,
        "prod" => ShareEnvironment::Prod,
        _ => return Err("invalid share environment".into()),
    };
    let prepared =
        share_publication::prepare(owner, environment, write_mode, spec_name, spec_version)?;
    let marker =
        serde_json::to_vec(&prepared.marker).map_err(|_| "cannot encode publication marker")?;
    let mut target = target.create()?;
    target.write(
        lastdb_node::identity::IDENTITY_KEY_FILE,
        prepared.seed.as_ref(),
    )?;
    target.write(share_publication::MARKER_FILE, &marker)?;
    target.commit()?;
    let mut printed = serde_json::json!({
        "home": into, "publication_id": prepared.marker.id, "publisher_public_key": prepared.marker.publisher,
        "environment": prepared.marker.environment, "write_mode": prepared.marker.write_mode,
    });
    if let Some(spec) = &prepared.marker.spec {
        printed["spec"] =
            serde_json::to_value(spec).map_err(|_| "cannot encode publication spec")?;
    }
    println!("{printed}");
    Ok(())
}

fn contact(home: &Path, out: &Path) -> Result<(), String> {
    let recipient = share_publication::recipient(home)?.contact();
    write_file(out, recipient.encode().as_bytes())?;
    println!(
        "{}",
        serde_json::json!({"contact": out, "recipient_public_key": recipient.identity_public_key()})
    );
    Ok(())
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "cannot construct share runtime".into())
}

fn read_file(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "cannot open share input file")?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect share input file")?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err("share input must be a bounded regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read share input file")?;
    if bytes.len() as u64 > limit {
        return Err("share input exceeds its size limit".into());
    }
    Ok(bytes)
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
