use super::*;
use cloud_backup_protocol::{AccountId, Digest};
use serde::{Deserialize, Serialize};

const FILE: &str = "automatic.json";
const MAX_INTERVAL: u64 = 7 * 86400;

#[derive(Clone, Debug, Serialize, Deserialize)]
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
        Store::write_configuration(&host.home, FILE, self)
    }
    fn validate(&self, host: &crate::host::Host) -> Result<()> {
        let runtime = native(host)?;
        if self.version != 2
            || self.account.as_str() != host.user_hash
            || self.source_home.as_str() != runtime.native_home_id().to_string()
            || self.endpoint.len() > 4096
            || self.state.revision == 0
            || !(60..=MAX_INTERVAL).contains(&self.state.log_interval_seconds)
            || !(60..=MAX_INTERVAL).contains(&self.state.snapshot_interval_seconds)
            || self.pending.as_ref().map(|p| &p.id) != self.state.pending_request_id.as_ref()
        {
            return Err(BackupError::Identity);
        }
        if let Some(pending) = &self.pending {
            pending.work.validate(None)?;
            if pending.revision == 0 || pending.revision > self.state.revision {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }
    pub fn require_connection(&self, host: &crate::host::Host) -> Result<()> {
        let binding = super::super::connection::automatic_binding(host)?;
        if binding
            != (
                self.account.clone(),
                self.source_home.clone(),
                self.endpoint.clone(),
                self.credential_sha256.clone(),
            )
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
    pub fn validate_job(&self, saved: &Saved) -> Result<()> {
        let pending = self.pending.as_ref().ok_or(BackupError::Invalid)?;
        if saved.account != self.account
            || saved.source_home != self.source_home
            || saved.endpoint != self.endpoint
            || saved.request_id != pending.id
            || saved.automatic_revision != Some(pending.revision)
            || saved.work != pending.work
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
}
pub(super) fn load(host: &crate::host::Host) -> Result<Option<Policy>> {
    let policy: Option<Policy> = store::read(&Store::base(&host.home).join(FILE))?;
    if let Some(policy) = &policy {
        policy.validate(host)?;
    }
    Ok(policy)
}
pub(in crate::byte_cloud_backup) fn status(
    host: &crate::host::Host,
) -> Result<Option<AutomaticBackupStatus>> {
    Ok(load(host)?.map(|policy| policy.state))
}
pub(in crate::byte_cloud_backup) fn configure(
    host: &crate::host::Host,
    enabled: bool,
    log: u64,
    snapshot: u64,
) -> Result<()> {
    let old = load(host)?;
    if !enabled {
        if let Some(mut policy) = old {
            policy.state.enabled = false;
            policy.state.revision = policy
                .state
                .revision
                .checked_add(1)
                .ok_or(BackupError::Limit)?;
            policy.save(host)?;
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
    if old.as_ref().is_some_and(|p| p.pending.is_some()) {
        old.as_ref()
            .ok_or(BackupError::Invalid)?
            .require_connection(host)?;
    }
    let mut policy = old.unwrap_or(Policy {
        version: 2,
        account: account.clone(),
        source_home: source_home.clone(),
        endpoint: endpoint.clone(),
        credential_sha256: credential_sha256.clone(),
        state: AutomaticBackupStatus {
            enabled: false,
            revision: 0,
            log_interval_seconds: log,
            snapshot_interval_seconds: snapshot,
            next_log_at: 0,
            next_snapshot_at: 0,
            pending_request_id: None,
            attempts: 0,
            retry_at: 0,
            last_completed_request_id: None,
            last_completed_at: None,
            last_snapshot_at: None,
            last_log_at: None,
            last_backed_up_frontiers: None,
            snapshot_reason: None,
            paused: None,
        },
        pending: None,
        cleanup: None,
    });
    policy.account = account;
    policy.source_home = source_home;
    policy.endpoint = endpoint;
    policy.credential_sha256 = credential_sha256;
    policy.state.enabled = true;
    policy.state.revision = policy
        .state
        .revision
        .checked_add(1)
        .ok_or(BackupError::Limit)?;
    policy.state.log_interval_seconds = log;
    policy.state.snapshot_interval_seconds = snapshot;
    policy.state.next_log_at = fold_db::clock::unix_secs();
    policy.state.retry_at = 0;
    policy.state.next_snapshot_at = policy
        .state
        .last_snapshot_at
        .map_or(0, |at| at.saturating_add(snapshot));
    policy.state.paused = None;
    if let Some(pending) = &policy.pending {
        Store::open(&host.home, &pending.id)?.write("pause.json", &Option::<PauseState>::None)?;
        host.byte_backup.acknowledge_completion(&pending.id);
    }
    policy.save(host)?;
    native(host)?.configure_mutation_log(true)?;
    host.byte_backup.wake();
    Ok(())
}
