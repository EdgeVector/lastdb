use super::*;
use crate::byte_cloud_backup::store::AUTOMATIC_FILE as FILE;
use cloud_backup_protocol::{AccountId, Digest};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::Path;

const MAX_INTERVAL: u64 = 7 * 86400;
const VERSION: u32 = 3;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomaticBackupStatus {
    pub enabled: bool,
    pub revision: u64,
    pub log_interval_seconds: u64,
    pub snapshot_interval_seconds: u64,
    pub next_log_at: u64,
    pub next_snapshot_at: u64,
    pub pending_request_id: Option<Id>,
    pub attempts: u32,
    pub retry_at: u64,
    pub last_completed_request_id: Option<Id>,
    pub last_completed_at: Option<u64>,
    pub last_snapshot_at: Option<u64>,
    pub last_log_at: Option<u64>,
    pub last_backed_up_frontiers: Option<fold_db::storage::disk_pack::mutation_log::Frontiers>,
    pub snapshot_reason: Option<String>,
    pub paused: Option<PauseState>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub id: Id,
    pub revision: u64,
    pub work: Work,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    version: u32,
    account: AccountId,
    source_home: Id,
    endpoint: String,
    credential_sha256: Digest,
    pub state: AutomaticBackupStatus,
    pub pending: Option<Pending>,
    pub cleanup: Option<Id>,
}

impl Policy {
    pub fn save(&self, host: &crate::host::Host) -> Result<()> {
        self.validate(host)?;
        Store::write_configuration_at(&Store::base(&host.home), FILE, self)
    }

    fn validate(&self, host: &crate::host::Host) -> Result<()> {
        let runtime = native(host)?;
        let st = &self.state;
        if self.version != VERSION
            || self.account.as_str() != host.user_hash
            || self.source_home.as_str() != runtime.native_home_id().to_string()
            || self.endpoint.len() > 4096
            || st.revision == 0
            || !(60..=MAX_INTERVAL).contains(&st.log_interval_seconds)
            || !(60..=MAX_INTERVAL).contains(&st.snapshot_interval_seconds)
            || self.pending.as_ref().map(|p| &p.id) != st.pending_request_id.as_ref()
        {
            return Err(BackupError::Identity);
        }
        if let Some(p) = &self.pending {
            p.work.validate(None)?;
            if p.revision == 0 || p.revision > st.revision {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }

    pub fn require_connection(&self, host: &crate::host::Host) -> Result<()> {
        let (acc, src, ep, dig) = super::super::connection::automatic_binding(host)?;
        let expected = (
            &self.account,
            &self.source_home,
            &self.endpoint,
            &self.credential_sha256,
        );
        (expected == (&acc, &src, &ep, &dig))
            .then_some(())
            .ok_or(BackupError::Identity)
    }

    pub fn validate_job(&self, saved: &Saved) -> Result<()> {
        let p = self.pending.as_ref().ok_or(BackupError::Invalid)?;
        let ok = saved.account == self.account
            && saved.source_home == self.source_home
            && saved.endpoint == self.endpoint
            && saved.request_id == p.id
            && saved.automatic_revision == Some(p.revision)
            && saved.work == p.work;
        ok.then_some(()).ok_or(BackupError::Identity)
    }
}

pub(super) fn load(host: &crate::host::Host) -> Result<Option<Policy>> {
    let policy: Option<Policy> = store::read(&Store::base(&host.home).join(FILE))?;
    if let Some(p) = &policy {
        p.validate(host)?;
    }
    Ok(policy)
}

pub(in crate::byte_cloud_backup) fn status(
    host: &crate::host::Host,
) -> Result<Option<AutomaticBackupStatus>> {
    Ok(load(host)?.map(|p| p.state))
}

pub(in crate::byte_cloud_backup) fn configure(
    host: &crate::host::Host,
    enabled: bool,
    log: u64,
    snapshot: u64,
) -> Result<()> {
    let old = load(host)?;
    if !enabled {
        if let Some(mut p) = old {
            p.state.enabled = false;
            p.state.revision = p.state.revision.checked_add(1).ok_or(BackupError::Limit)?;
            p.save(host)?;
            native(host)?.configure_mutation_log(false)?;
        }
        host.byte_backup.wake();
        return Ok(());
    }
    if !(60..=MAX_INTERVAL).contains(&log) || !(60..=MAX_INTERVAL).contains(&snapshot) {
        return Err(BackupError::Invalid);
    }
    let (account, source_home, endpoint, credential_sha256) =
        super::super::connection::automatic_binding(host)?;
    if let Some(existing) = old.as_ref().filter(|p| p.pending.is_some()) {
        existing.require_connection(host)?;
    }
    let (mut state, pending, cleanup) = match old {
        Some(p) => (p.state, p.pending, p.cleanup),
        None => (AutomaticBackupStatus::default(), None, None),
    };
    state.enabled = true;
    state.revision = state.revision.checked_add(1).ok_or(BackupError::Limit)?;
    (state.log_interval_seconds, state.snapshot_interval_seconds) = (log, snapshot);
    (state.next_log_at, state.retry_at, state.paused) = (fold_db::clock::unix_secs(), 0, None);
    state.next_snapshot_at = state
        .last_snapshot_at
        .map_or(0, |at| at.saturating_add(snapshot));
    let policy = Policy {
        version: VERSION,
        account,
        source_home,
        endpoint,
        credential_sha256,
        state,
        pending,
        cleanup,
    };
    policy.save(host)?;
    if let Some(p) = &policy.pending {
        Store::open(&host.home, &p.id)?.write("pause.json", &Option::<PauseState>::None)?;
        host.byte_backup.acknowledge_completion(&p.id);
    }
    native(host)?.configure_mutation_log(true)?;
    host.byte_backup.wake();
    Ok(())
}

pub(in crate::byte_cloud_backup) fn retire_at(base: &Path, id: &Id) -> Result<()> {
    if !super::reserved(id) {
        return Err(BackupError::Identity);
    }
    let root = base.join("jobs").join(id.as_str());
    let (previous, active) = (store::state_at(base, id)?, Store::active_at(base)?);
    if previous.is_none() && active.as_ref() != Some(id) {
        return remove_directory(&root);
    }
    let saved = previous.ok_or(BackupError::Missing)?;
    if saved.automatic_revision.is_none() || !saved.terminal() || active.as_ref() == Some(id) {
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
        allowed(name).then_some(()).ok_or(BackupError::Invalid)?;
        store::size(&entry.path())?;
        if !matches!(name, "state.json" | "terminal.json") {
            files.push(entry.path());
        }
    }
    for path in files {
        store::remove_owned(&path)?;
    }
    File::open(&root)?.sync_all()?;
    for name in ["terminal.json", "state.json"] {
        store::remove_owned(&root.join(name))?;
        File::open(&root)?.sync_all()?;
    }
    remove_directory(&root)
}

fn remove_directory(root: &Path) -> Result<()> {
    if let Err(e) = fs::remove_dir(root) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(e.into());
        }
    }
    Ok(File::open(root.parent().ok_or(BackupError::Invalid)?)?.sync_all()?)
}

const EXACT_FILES: &[&str] = &[
    "state",
    "terminal",
    "recovery",
    "pause",
    "log-plan",
    "inventory-index",
    "close",
    "blobs",
];
const NUMBERED_PREFIXES: &[&str] = &[
    "log-object-",
    "log-file-",
    "log-payload-",
    "inventory-page-",
    "blob-upload-",
    "blob-uploaded-",
    "blob-release-",
];

fn allowed(name: &str) -> bool {
    if matches!(name.rsplit_once('.'), Some((s, "json" | "pending")) if EXACT_FILES.contains(&s)) {
        return true;
    }
    NUMBERED_PREFIXES.iter().any(|p| {
        name.strip_prefix(p)
            .and_then(|t| t.split_once('.'))
            .is_some_and(|(d, ext)| {
                (1..=8).contains(&d.len())
                    && d.bytes().all(|b| b.is_ascii_digit())
                    && matches!(ext, "json" | "pending" | "enc")
            })
    })
}
