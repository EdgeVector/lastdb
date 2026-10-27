use super::*;
use cloud_backup_protocol::{AccountId, Digest};
use serde::{Deserialize, Serialize};

const FILE: &str = "automatic.json";
const MIN_INTERVAL: u64 = 60;
const MAX_INTERVAL: u64 = 7 * 24 * 60 * 60;

/// Public local scheduling facts. No key, recovery phrase or provider grant is present.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomaticBackupStatus {
    pub enabled: bool,
    pub revision: u64,
    pub interval_seconds: u64,
    pub next_run_at: u64,
    pub pending_request_id: Option<Id>,
    pub pending_frontier: Option<u64>,
    pub attempts: u32,
    pub last_completed_request_id: Option<Id>,
    pub last_completed_at: Option<u64>,
    pub last_backed_up_frontier: Option<u64>,
    pub paused: Option<PauseState>,
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
}

impl Policy {
    pub fn save(&self, host: &crate::host::Host) -> Result<()> {
        self.validate(host)?;
        Store::write_configuration(&host.home, FILE, self)
    }

    fn validate(&self, host: &crate::host::Host) -> Result<()> {
        let runtime = host
            .db
            .db_ops()
            .namespaced_store()
            .disk_pack_runtime()
            .ok_or(BackupError::Source)?;
        if self.version != 1
            || self.account.as_str() != host.user_hash
            || self.source_home.as_str() != runtime.native_home_id().to_string()
            || self.endpoint.len() > 4096
            || self.state.revision == 0
            || !(MIN_INTERVAL..=MAX_INTERVAL).contains(&self.state.interval_seconds)
            || self.state.pending_request_id.is_some() != self.state.pending_frontier.is_some()
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }

    pub fn require_connection(&self, host: &crate::host::Host) -> Result<()> {
        let (account, source, endpoint, digest) =
            super::super::connection::automatic_binding(host)?;
        if (account, source, endpoint, digest)
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
        if saved.account != self.account
            || saved.source_home != self.source_home
            || saved.endpoint != self.endpoint
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

/// Called under the transition lock. Disable never needs valid network credentials.
pub(in crate::byte_cloud_backup) fn configure(
    host: &crate::host::Host,
    enabled: bool,
    interval: u64,
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
        }
        host.byte_backup.wake();
        return Ok(());
    }
    if !(MIN_INTERVAL..=MAX_INTERVAL).contains(&interval) {
        return Err(BackupError::Invalid);
    }
    let (account, source_home, endpoint, credential_sha256) =
        super::super::connection::automatic_binding(host)?;
    let state = old.map(|policy| policy.state);
    let mut state = state.unwrap_or(AutomaticBackupStatus {
        enabled: false,
        revision: 0,
        interval_seconds: interval,
        next_run_at: 0,
        pending_request_id: None,
        pending_frontier: None,
        attempts: 0,
        last_completed_request_id: None,
        last_completed_at: None,
        last_backed_up_frontier: None,
        paused: None,
    });
    state.enabled = true;
    state.revision = state.revision.checked_add(1).ok_or(BackupError::Limit)?;
    state.interval_seconds = interval;
    state.next_run_at = fold_db::clock::unix_secs();
    if let Some(id) = &state.pending_request_id {
        let store = Store::open(&host.home, id)?;
        store.write("pause.json", &Option::<PauseState>::None)?;
    }
    state.paused = None;
    let policy = Policy {
        version: 1,
        account,
        source_home,
        endpoint,
        credential_sha256,
        state,
    };
    policy.save(host)?;
    host.byte_backup.wake();
    Ok(())
}
