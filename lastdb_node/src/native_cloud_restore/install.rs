use super::{
    session, NativeInventory, RecoveryKeys, RestoreError, RestoreReport, Result,
    VerifiedNativeContext,
};
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

pub(super) fn finish_run_context(
    session: &VerifiedNativeContext<'_>,
    keys: &RecoveryKeys,
    inventory: NativeInventory,
    proof: &fold_db::storage::disk_pack::run_restore::RunRestoreGuard,
) -> Result<RestoreReport> {
    proof
        .validate_context(&session.data_root(), session.native_root)
        .map_err(|_| RestoreError::Invalid("verified run publication"))?;
    let (files, bytes) = proof
        .totals()
        .map_err(|_| RestoreError::Invalid("verified run totals"))?;
    if files != inventory.files || bytes != inventory.bytes || inventory.dependencies != 0 {
        return Err(RestoreError::Invalid("verified run inventory"));
    }
    publish_context(session, keys, inventory, proof.verified_records())
}

fn publish_context(
    session: &VerifiedNativeContext<'_>,
    keys: &RecoveryKeys,
    inventory: NativeInventory,
    records: u64,
) -> Result<RestoreReport> {
    let home = session.root.join("home");
    install_identity(&home, keys)?;
    if crate::cloud::cloud_credentials_path(&home).try_exists()? {
        return Err(RestoreError::Invalid("restore cloud configuration"));
    }
    let report = RestoreReport {
        commit_sha256: session.commit_sha256.as_str().into(),
        native_files: inventory.files,
        native_bytes: inventory.bytes,
        verified_records: records,
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
