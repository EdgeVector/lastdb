//! Retire automatic backup job metadata, previous policy baselines, and local replacements.
use super::*;
use crate::byte_cloud_backup::{model::LocalAction, source::Source, store::AUTOMATIC_FILE as FILE};
use fold_db::storage::disk_pack::PackRuntime;
use serde::Deserialize;
use std::fs::{self, File};
use std::path::Path;

pub(in crate::byte_cloud_backup) const PREVIOUS_VERSION: u32 = 2;

#[derive(Deserialize)]
pub(super) struct Version {
    pub version: u32,
}

#[derive(Deserialize)]
struct Previous {
    account: cloud_backup_protocol::AccountId,
    source_home: Id,
    endpoint: String,
    credential_sha256: cloud_backup_protocol::Digest,
    state: PreviousState,
    #[serde(default)]
    pending: Option<PreviousPending>,
    #[serde(default)]
    cleanup: Option<Id>,
}

#[derive(Deserialize)]
struct PreviousState {
    enabled: bool,
    revision: u64,
    log_interval_seconds: u64,
    snapshot_interval_seconds: u64,
    #[serde(default)]
    last_completed_request_id: Option<Id>,
}

#[derive(Deserialize)]
struct PreviousPending {
    id: Id,
}

pub(super) fn retire(host: &crate::host::Host, id: &Id) -> Result<()> {
    retire_at(&Store::base(&host.home), id)
}

pub(in crate::byte_cloud_backup) fn retire_at(base: &Path, id: &Id) -> Result<()> {
    if !super::reserved(id) {
        return Err(BackupError::Identity);
    }
    let root = base.join("jobs").join(id.as_str());
    let previous = store::state_at(base, id)?;
    let active = Store::active_at(base)?;
    if previous.is_none() && active.as_ref() != Some(id) {
        return remove_directory(&root);
    }
    let saved = previous.ok_or(BackupError::Missing)?;
    if saved.automatic_revision.is_none() || !saved.terminal() || active.as_ref() == Some(id) {
        return Err(BackupError::Identity);
    }
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

/// Replace a version 2 policy file with the same setting and no baseline.
pub(super) fn retire_previous_policy(home: &Path, runtime: &PackRuntime) -> Result<()> {
    let base = Store::base(home);
    let Some(previous) = store::read::<Previous>(&base.join(FILE))? else {
        return Ok(());
    };
    let dropped = previous
        .pending
        .as_ref()
        .map(|pending| pending.id.as_str().to_owned());
    if !runtime.is_dedicated_shared_home() {
        runtime.retire_previous_backup_capture(dropped.as_deref())?;
        runtime.retire_previous_mutation_log()?;
        let jobs = previous
            .pending
            .as_ref()
            .map(|pending| &pending.id)
            .into_iter()
            .chain(previous.cleanup.as_ref())
            .chain(previous.state.last_completed_request_id.as_ref());
        for id in jobs {
            remove_previous_job(home, id)?;
        }
        for name in ["recovery.pending", "active.pending"] {
            store::remove_owned(&base.join(name))?;
        }
    }
    for name in ["recovery.json", "active.json"] {
        store::remove_owned(&base.join(name))?;
    }
    let mut state = Policy::empty_state(
        previous.state.log_interval_seconds,
        previous.state.snapshot_interval_seconds,
    );
    state.enabled = previous.state.enabled;
    state.revision = previous
        .state
        .revision
        .checked_add(1)
        .ok_or(BackupError::Limit)?;
    state.snapshot_reason = Some("backup format upgrade".to_owned());
    let policy = Policy::new(
        previous.account,
        previous.source_home,
        previous.endpoint,
        previous.credential_sha256,
        state,
    );
    Store::write_configuration(home, FILE, &policy)
}

/// Recover a local-only log intent after native recovery requires a new snapshot.
pub(super) fn retire_replacement(
    host: &crate::host::Host,
    policy: &Policy,
    pending: &Pending,
) -> Result<bool> {
    if !matches!(pending.work, Work::MutationLog { .. }) {
        return Ok(false);
    }
    let previous = store::state(&host.home, &pending.id)?;
    if let Some(saved) = &previous {
        policy.validate_job(saved)?;
        if saved.terminal() || saved.begin_submitted {
            return Ok(false);
        }
    }
    let runtime = native(host)?;
    let journal = runtime.mutation_log_status()?;
    let Work::MutationLog { context } = &pending.work else {
        return Err(BackupError::Invalid);
    };
    let already_replacing = previous.as_ref().is_some_and(|s| s.local_action.is_some());
    if !already_replacing
        && !journal.snapshot_required
        && journal.anchor.as_ref() == Some(&context.anchor)
    {
        return Ok(false);
    }
    let _permit = Store::lock(&host.home)?;
    super::super::control::ensure_previous(&host.home, &pending.id)?;
    if runtime.mutation_log_capture_materialized(&pending.id)? {
        return if already_replacing {
            Err(BackupError::Identity)
        } else {
            Ok(false)
        };
    }
    let source = Source::from_host(host)?;
    let client = super::super::connection::client_for_host(host)?;
    let mut saved = previous.unwrap_or_else(|| {
        let mut saved = source.saved(pending.id.clone(), client.canonical_endpoint());
        saved.work = pending.work.clone();
        saved.automatic_revision = Some(pending.revision);
        saved
    });
    policy.validate_job(&saved)?;
    source.validate(&saved)?;
    let store = Store::open(&host.home, &pending.id)?;
    saved.local_action = Some(LocalAction::ReplaceWithSnapshot);
    store.save(&saved)?;
    store.select(&pending.id)?;
    source.cancel_replacement(&saved)?;
    saved.phase = Phase::Closed;
    saved.retained_source_bytes = Some(0);
    saved.blocked_reason = None;
    store.save(&saved)?;
    Ok(true)
}

fn remove_previous_job(home: &Path, id: &Id) -> Result<()> {
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

fn remove_directory(root: &Path) -> Result<()> {
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
