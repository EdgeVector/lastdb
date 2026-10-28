//! Retire only the previous source-released automatic client's transfer metadata.
use super::*;
use std::fs::{self, File};

pub(super) fn retire(host: &crate::host::Host, id: &Id) -> Result<()> {
    let root = Store::base(&host.home).join("jobs").join(id.as_str());
    let previous = store::state(&host.home, id)?;
    let expires = super::reserved(id);
    if previous.is_none() && expires && Store::active(&host.home)?.as_ref() != Some(id) {
        return remove_directory(&root);
    }
    let saved = previous.ok_or(BackupError::Missing)?;
    if saved.automatic_revision.is_none()
        || !saved.terminal()
        || Store::active(&host.home)?.as_ref() == Some(id)
    {
        return Err(BackupError::Identity);
    }
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
    // Legacy UUID jobs retain their terminal; new reserved IDs expire locally.
    for path in files {
        super::super::store::remove_owned(&path)?;
    }
    File::open(&root)?.sync_all()?;
    if expires {
        store::remove_owned(&root.join("terminal.json"))?;
        File::open(&root)?.sync_all()?;
        store::remove_owned(&root.join("state.json"))?;
        File::open(&root)?.sync_all()?;
        remove_directory(&root)?;
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
