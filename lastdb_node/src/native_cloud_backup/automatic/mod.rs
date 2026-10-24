//! Per-home explicit consent. The existing worker owns all backup effects.
mod controller;
mod local;
mod model;
mod plan;
mod preflight;

use super::{
    store::{self, Store},
    BackupError, Disposition, NativeBackupStatus, PauseReason, PauseState, Result, RunOrigin,
};
use crate::host::Host;
use cloud_backup_protocol::Id;
use model::State;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};

pub use controller::spawn;
pub use model::Binding as AutoBinding;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub enum AutoRequest {
    #[serde(rename = "auto_on")]
    On,
    #[serde(rename = "auto_off")]
    Off,
    #[serde(rename = "auto_status")]
    Status,
}

#[derive(Serialize)]
pub struct AutoStatus {
    pub enabled: bool,
    pub revision: u64,
    pub binding: Option<AutoBinding>,
    pub scheduled_request_id: Option<Id>,
    pub pause: Option<PauseState>,
    pub next_retry_at_ms: Option<u64>,
    pub current_frontier: u64,
    pub accepted_frontier: Option<u64>,
    pub backup: NativeBackupStatus,
}

pub async fn request(host: &Host, action: AutoRequest) -> Result<AutoStatus> {
    match action {
        AutoRequest::Status => {}
        AutoRequest::Off => {
            let _transition = host.native_backup.transition()?;
            disable_for_close(&host.home)?;
            host.native_backup.wake();
        }
        AutoRequest::On => enable(host).await?,
    }
    status(host)
}

async fn enable(host: &Host) -> Result<()> {
    let expected_revision = {
        let _transition = host.native_backup.transition()?;
        host.native_backup.check_admission()?;
        State::load(&host.home)?.revision
    };
    let (binding, client) = preflight::check(host).await?;
    let accepted = preflight::reconcile_head(host, &binding, &client).await?;
    let _transition = host.native_backup.transition()?;
    host.native_backup.check_admission()?;
    let mut state = State::load(&host.home)?;
    if state.revision != expected_revision {
        return Err(BackupError::Disabled);
    }
    let active = Store::active(&host.home)?;
    let adopted = plan::prefer_active(host, &mut state, &binding, false)?;
    if let Some(accepted) = accepted {
        let pending = state
            .scheduled
            .as_ref()
            .or(active.as_ref())
            .filter(|id| *id == &accepted.job.commit_id)
            .map(|id| local::saved(&host.home, id))
            .transpose()?
            .flatten()
            .is_some_and(|saved| saved.retained_source_bytes != Some(0));
        if local::accepted(&host.home)?.as_ref() == Some(&accepted) && !pending {
            local::bind_accepted_endpoint(host, &binding, &accepted)?;
        } else {
            let id = state
                .scheduled
                .as_ref()
                .or(active.as_ref())
                .ok_or(BackupError::Identity)?;
            local::pending_head(&host.home, &binding, &accepted, id)?;
        }
    }
    let mut retry_saved = false;
    let mut selection_cleared = false;
    if state
        .binding
        .as_ref()
        .is_some_and(|saved| saved != &binding)
    {
        if let Some(id) = state.scheduled.as_ref().or(active.as_ref()) {
            if local::saved(&host.home, id)?
                .is_some_and(|saved| saved.retained_source_bytes != Some(0))
            {
                return Err(BackupError::Identity);
            }
        }
    }
    if let Some(id) = state.scheduled.as_ref().or(active.as_ref()) {
        if let Some(saved) = local::saved(&host.home, id)? {
            if local::terminal(&host.home, &saved)? {
                if state.scheduled.as_ref() == Some(id) {
                    state.scheduled = None;
                    state.selection_at_schedule = None;
                    state.clear_pause();
                    selection_cleared = true;
                }
            } else {
                local::compatible(&saved, &binding)?;
                retry_saved = saved.pause.is_some();
                state.scheduled = Some(id.clone());
                state.selection_at_schedule = None;
            }
        }
    }
    let changed = !state.enabled
        || state.binding.as_ref() != Some(&binding)
        || state.pause.is_some()
        || retry_saved
        || adopted
        || selection_cleared;
    if !changed {
        state.save(&host.home)?;
        host.native_backup.wake();
        return Ok(());
    }
    state.advance()?;
    state.enabled = true;
    state.binding = Some(binding);
    state.clear_pause();
    state.save(&host.home)?;
    host.native_backup.wake();
    Ok(())
}

pub(super) fn check_origin(home: &Path, origin: &RunOrigin) -> Result<()> {
    if let RunOrigin::Auto { consent_revision } = origin {
        let state = State::load(home)?;
        if !state.enabled || state.revision != *consent_revision {
            return Err(BackupError::Disabled);
        }
    }
    Ok(())
}

pub(super) fn disable_for_close(home: &Path) -> Result<()> {
    let mut state = State::load(home)?;
    state.advance()?;
    state.enabled = false;
    state.awaiting_completion = false;
    state.expected = None;
    state.save(home)
}

fn status(host: &Host) -> Result<AutoStatus> {
    let state = State::load(&host.home)?;
    let runtime = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or(BackupError::Source)?;
    let current = runtime.native_backup_boundary()?;
    let accepted_frontier = match &state.binding {
        Some(binding) => local::coverage(&host.home, binding)?,
        None => None,
    };
    Ok(AutoStatus {
        enabled: state.enabled,
        revision: state.revision,
        binding: state.binding,
        scheduled_request_id: state.scheduled,
        pause: state.pause,
        next_retry_at_ms: state.retry_at_ms,
        current_frontier: current.frontier,
        accepted_frontier,
        backup: super::status::status(&host.home, &host.native_backup)?,
    })
}
