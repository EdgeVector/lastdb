use super::{
    download, session, NativeFileProof, NativeInventory, RecoveryKeys, RestoreError, RestoreReport,
    Result, VerifiedNativeContext,
};
use cloud_backup_protocol::bytes_sha256;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u8,
    session_id: uuid::Uuid,
    report: RestoreReport,
}

pub(super) fn published_context(
    session: &VerifiedNativeContext<'_>,
) -> Result<Option<RestoreReport>> {
    let destination = session.destination;
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(meta) if !meta.is_dir() => return Err(RestoreError::Invalid("published destination")),
        Ok(_) => {}
    }
    let receipt: Receipt = session::read_json(&destination.join(session.receipt_name), 64 * 1024)?;
    if receipt.version != session.receipt_version
        || receipt.session_id != session.session_id
        || receipt.report.commit_sha256 != session.commit_sha256.as_str()
        || receipt.report.destination != *destination
        || !receipt.report.file_dependencies_verified
    {
        return Err(RestoreError::Invalid("published receipt"));
    }
    // A prior call can lose either post-rename directory barrier. The receipt
    // proves the selected data, but success also requires both barriers again.
    File::open(session.root)?.sync_all()?;
    File::open(
        destination
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?,
    )?
    .sync_all()?;
    session::atomic_json(&session.root.join("result.json"), &receipt.report)?;
    Ok(Some(receipt.report))
}

pub(super) fn finish_context(
    session: &VerifiedNativeContext<'_>,
    keys: &RecoveryKeys,
    inventory: NativeInventory,
    atoms: u64,
    entry_for: impl Fn(&str) -> Result<NativeFileProof>,
) -> Result<RestoreReport> {
    let physical = session.physical();
    let native = laststore::disk_pack::backup::validate_snapshot(&physical)
        .map_err(|_| RestoreError::Invalid("native snapshot validation"))?;
    let expected = session.native_root;
    let current = download::bytes(&physical.join("CURRENT"), 4096)?;
    if native.home_id.to_string() != expected.home_id.as_str()
        || bytes_sha256(&current) != expected.current_sha256
        || hex::encode(native.current.home_digest) != expected.home_digest.as_str()
        || native.current.generation != expected.generation.get()
        || native.current.frontier != expected.frontier.get()
        || native.current.required_features != expected.required_features.get()
        || native.files.len() as u64 != inventory.files
    {
        return Err(RestoreError::Invalid("native accepted boundary"));
    }
    for file in native.files {
        let path = file
            .path
            .to_str()
            .ok_or(RestoreError::Invalid("native inventory path"))?;
        let entry = entry_for(path)?;
        if entry.path != path || entry.bytes != file.bytes {
            return Err(RestoreError::Invalid("exact native inventory"));
        }
        download::verify_file(&physical.join(&file.path), &entry.sha256, file.bytes)?;
    }
    let home = session.root.join("home");
    let _native_lock =
        fold_db::storage::disk_pack::prepare_restored_node(&home.join("data"), &keys.content)
            .map_err(|_| RestoreError::Invalid("native node wrapper"))?;
    install_identity(&home, keys)?;
    let (config, paused) = crate::cloud::cloud_sync_paths(&home);
    if config.try_exists()? || paused.try_exists()? {
        return Err(RestoreError::Invalid("restore cloud configuration"));
    }
    let report = RestoreReport {
        commit_sha256: session.commit_sha256.as_str().into(),
        native_files: inventory.files,
        native_bytes: inventory.bytes,
        verified_atoms: atoms,
        file_dependencies_verified: true,
        verified_external_files: inventory.dependencies,
        destination: session.destination.to_path_buf(),
    };
    session::atomic_json(
        &home.join(session.receipt_name),
        &Receipt {
            version: session.receipt_version,
            session_id: session.session_id,
            report: report.clone(),
        },
    )?;
    sync_owned_tree(&home, 0)?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        &home,
        rustix::fs::CWD,
        session.destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)?;
    File::open(session.root)?.sync_all()?;
    File::open(
        session
            .root
            .parent()
            .ok_or(RestoreError::Invalid("destination parent"))?,
    )?
    .sync_all()?;
    session::atomic_json(&session.root.join("result.json"), &report)?;
    Ok(report)
}

fn install_identity(home: &Path, keys: &RecoveryKeys) -> Result<()> {
    let path = home.join(lastdb_identity::IDENTITY_KEY_FILE);
    if path.try_exists()? {
        if !fs::symlink_metadata(&path)?.is_file() {
            return Err(RestoreError::Invalid("restore identity"));
        }
        let mut file = session::owner_file(&path, false)?;
        let mut seed = zeroize::Zeroizing::new(Vec::new());
        Read::by_ref(&mut file).take(33).read_to_end(&mut seed)?;
        if seed.len() > 32 || !keys.seed.starts_with(&seed) {
            return Err(RestoreError::Invalid("restore identity"));
        }
        if seed.len() < 32 {
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&*keys.seed)?;
        }
    } else {
        lastdb_identity::write_seed(home, &keys.seed)
            .map_err(|_| RestoreError::Invalid("restore identity flush"))?;
    }
    File::open(path)?.sync_all()?;
    File::open(home)?.sync_all()?;
    Ok(())
}

fn sync_owned_tree(root: &Path, depth: usize) -> Result<()> {
    if depth > 12 {
        return Err(RestoreError::Limit("destination depth"));
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            sync_owned_tree(&entry.path(), depth + 1)?;
        } else if !kind.is_file() {
            return Err(RestoreError::Invalid("destination member type"));
        }
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
