use super::{admin_cmds::guest_stage, cloud_cmds::FreshHome, ShareCommand};
use cloud_backup_client::byte_backup::ShareEnvironment;
use cloud_backup_protocol::{Digest, Id};
use lastdb_node::share_invitation::{ShareContact, MAX_SEALED_INVITATION_BYTES};
use lastdb_node::share_publication;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

pub(super) fn run(data_dir: Option<PathBuf>, action: ShareCommand) -> Result<(), String> {
    let identity_home = data_dir
        .filter(|p| p.is_absolute())
        .ok_or("share requires an explicit absolute --data-dir identity home")?;
    let identity_home = identity_home
        .canonicalize()
        .map_err(|_| "share identity home must exist")?;
    match action {
        ShareCommand::Create { into, env } => create(&identity_home, &into, &env),
        ShareCommand::Publish { home, request_id } => publish(&identity_home, &home, request_id),
        ShareCommand::Contact { out } => contact(&identity_home, &out),
        ShareCommand::Invite {
            home,
            recipient,
            recipient_key,
            out,
            expires_in_hours,
        } => {
            let bytes = read_file(&recipient, 512)?;
            let text = std::str::from_utf8(&bytes).map_err(|_| "invalid share contact file")?;
            let contact = ShareContact::parse(text.trim()).map_err(|e| e.to_string())?;
            let expected =
                Digest::try_from(recipient_key).map_err(|_| "invalid recipient public key")?;
            if &expected != contact.identity_public_key() {
                return Err("contact does not match the trusted recipient".into());
            }
            let sealed = runtime()?.block_on(share_publication::invite(
                &home,
                &identity_home,
                &contact,
                expires_in_hours,
            ))?;
            write_file(&out, &sealed)?;
            println!("{}", serde_json::json!({"invitation": out}));
            Ok(())
        }
        ShareCommand::Accept {
            invitation,
            publisher,
            selection,
        } => {
            let publisher =
                Digest::try_from(publisher).map_err(|_| "invalid publisher public key")?;
            let sealed = read_file(&invitation, MAX_SEALED_INVITATION_BYTES as u64)?;
            let recipient = share_publication::recipient(&identity_home)?;
            let opened = recipient
                .open(&sealed, &publisher)
                .map_err(|e| e.to_string())?;
            guest_stage::stage(
                selection,
                opened.permission,
                publisher,
                opened.publication_key,
            )
        }
    }
}

fn create(owner: &Path, into: &Path, env: &str) -> Result<(), String> {
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
    let prepared = share_publication::prepare(owner, environment)?;
    let marker =
        serde_json::to_vec(&prepared.marker).map_err(|_| "cannot encode publication marker")?;
    let mut target = target.create()?;
    target.write(lastdb_identity::IDENTITY_KEY_FILE, prepared.seed.as_ref())?;
    target.write(share_publication::MARKER_FILE, &marker)?;
    target.commit()?;
    println!(
        "{}",
        serde_json::json!({
            "home": into, "publication_id": prepared.marker.id, "publisher_public_key": prepared.marker.publisher,
            "environment": prepared.marker.environment,
        })
    );
    Ok(())
}

fn publish(owner: &Path, home: &Path, request_id: String) -> Result<(), String> {
    let request_id =
        Id::try_from(request_id).map_err(|_| "invalid share publication request ID")?;
    let report = runtime()?
        .block_on(lastdb_node::byte_cloud_backup::backup_publication_stopped(
            home, owner, request_id,
        ))
        .map_err(|e| e.to_string())?;
    let accepted = report.accepted_backup;
    println!(
        "{}",
        serde_json::json!({
            "publication_id": accepted.job().stream.stream_id,
            "commit_id": accepted.job().commit_id,
            "commit_sha256": accepted.commit_sha256,
            "ordinal": accepted.ordinal,
        })
    );
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
