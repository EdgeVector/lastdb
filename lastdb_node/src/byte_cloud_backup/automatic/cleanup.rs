//! Retire only the previous source-released automatic client's transfer metadata.
use super::*;
use std::fs::{self, File};

pub(super) fn retire(host: &crate::host::Host, id: &Id) -> Result<()> {
    if !super::reserved(id) {
        return Err(BackupError::Identity);
    }
    let root = Store::base(&host.home).join("jobs").join(id.as_str());
    let previous = store::state(&host.home, id)?;
    if previous.is_none() && Store::active(&host.home)?.as_ref() != Some(id) {
        return remove_directory(&root);
    }
    let saved = previous.ok_or(BackupError::Missing)?;
    if saved.automatic_revision.is_none()
        || !saved.terminal()
        || Store::active(&host.home)?.as_ref() == Some(id)
    {
        return Err(BackupError::Identity);
    }
    // The blob plan has no page limit, so it goes by exact names before the
    // bounded listing of the remaining files.
    super::super::blobs::remove_plan(&root)?;
    File::open(&root)?.sync_all()?;
    let mut files = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if files.len() >= 32768 {
            return Err(BackupError::Limit);
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or(BackupError::Invalid)?;
        if !allowed(name) {
            return Err(BackupError::Invalid);
        }
        super::super::store::size(&entry.path())?;
        if ["state.json", "terminal.json"].contains(&name) {
            continue;
        }
        files.push(entry.path());
    }
    for path in files {
        super::super::store::remove_owned(&path)?;
    }
    File::open(&root)?.sync_all()?;
    store::remove_owned(&root.join("terminal.json"))?;
    File::open(&root)?.sync_all()?;
    store::remove_owned(&root.join("state.json"))?;
    File::open(&root)?.sync_all()?;
    remove_directory(&root)
}
/// Remove the job directory of a job that the retired version 2 policy
/// named. Only file names that a backup job writes are removed; a directory
/// that still holds another name stays. An absent directory is already
/// removed, so a repeat after a crash is harmless.
pub(super) fn remove_previous_job(home: &std::path::Path, id: &Id) -> Result<()> {
    if !super::reserved(id) || uuid::Uuid::parse_str(id.as_str()).is_err() {
        return Ok(());
    }
    let root = Store::base(home).join("jobs").join(id.as_str());
    match fs::symlink_metadata(&root) {
        Ok(info) => {
            use std::os::unix::fs::MetadataExt;
            if !info.is_dir()
                || info.uid() != unsafe { libc::geteuid() }
                || info.mode() & 0o077 != 0
            {
                return Err(BackupError::Invalid);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if files.len() >= 32768 {
            return Err(BackupError::Limit);
        }
        let name = entry.file_name();
        if name.to_str().is_some_and(allowed) {
            files.push(entry.path());
        }
    }
    for path in files {
        store::remove_owned(&path)?;
    }
    File::open(&root)?.sync_all()?;
    match fs::remove_dir(&root) {
        Ok(()) => File::open(root.parent().ok_or(BackupError::Invalid)?)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
fn remove_directory(root: &std::path::Path) -> Result<()> {
    match fs::remove_dir(root) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    File::open(root.parent().ok_or(BackupError::Invalid)?)?.sync_all()?;
    Ok(())
}
fn allowed(name: &str) -> bool {
    if [
        "state.json",
        "state.pending",
        "terminal.json",
        "terminal.pending",
        "recovery.json",
        "recovery.pending",
        "pause.json",
        "pause.pending",
        "log-plan.json",
        "log-plan.pending",
        "inventory-index.json",
        "inventory-index.pending",
        "close.json",
        "close.pending",
        "blobs.json",
        "blobs.pending",
    ]
    .contains(&name)
    {
        return true;
    }
    [
        "log-object-",
        "log-file-",
        "log-payload-",
        "inventory-page-",
        "blob-upload-",
        "blob-uploaded-",
        "blob-release-",
    ]
    .into_iter()
    .any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|tail| tail.split_once('.'))
            .is_some_and(|(digits, extension)| {
                !digits.is_empty()
                    && digits.len() <= 8
                    && digits.bytes().all(|b| b.is_ascii_digit())
                    && ["json", "pending", "enc"].contains(&extension)
            })
    })
}
