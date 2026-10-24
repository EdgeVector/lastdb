use super::super::{error::PauseState, store, BackupError, Result};
use cloud_backup_protocol::{CloudScope, Id};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub scope: CloudScope,
    pub source_home: Id,
    pub endpoint: String,
}
impl Binding {
    fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        if self.endpoint.is_empty() || self.endpoint.len() > 4096 {
            return Err(BackupError::Invalid);
        }
        let url = reqwest::Url::parse(&self.endpoint).map_err(|_| BackupError::Invalid)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != self.endpoint
            || !url.path().ends_with("/api/cloud/native-backup/v1/")
        {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }
}

/// Consent and retry intent only. AcceptedBackup remains the coverage authority.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub format: u8,
    pub revision: u64,
    pub enabled: bool,
    pub binding: Option<Binding>,
    pub scheduled: Option<Id>,
    pub selection_at_schedule: Option<Id>,
    pub pause: Option<PauseState>,
    pub retry_attempt: u8,
    pub retry_at_ms: Option<u64>,
    pub awaiting_completion: bool,
    pub expected: Option<Expected>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Expected {
    pub runtime_id: Id,
    pub after_token: u64,
    pub token: Option<u64>,
    pub auto_revision: Option<u64>,
}
impl State {
    pub fn load(home: &Path) -> Result<Self> {
        let state: Self = store::read(&store::Store::base(home).join("automatic.json"))?
            .unwrap_or_else(|| Self {
                format: 1,
                ..Self::default()
            });
        state.validate()?;
        Ok(state)
    }
    pub fn save(&self, home: &Path) -> Result<()> {
        self.validate()?;
        store::write_automatic(home, self)
    }
    pub fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(BackupError::Limit)?;
        Ok(())
    }
    pub fn clear_pause(&mut self) {
        self.pause = None;
        self.retry_attempt = 0;
        self.retry_at_ms = None;
        self.awaiting_completion = false;
        self.expected = None;
    }
    fn validate(&self) -> Result<()> {
        if self.format != 1
            || (self.enabled && (self.revision == 0 || self.binding.is_none()))
            || (self.awaiting_completion && (self.scheduled.is_none() || self.expected.is_none()))
            || (self.selection_at_schedule.is_some() && self.scheduled.is_none())
            || (self.retry_at_ms.is_some() && self.pause.is_none())
            || self.expected.as_ref().is_some_and(|expected| {
                expected
                    .auto_revision
                    .is_some_and(|revision| revision != self.revision)
                    || expected
                        .token
                        .is_some_and(|token| token <= expected.after_token)
            })
        {
            return Err(BackupError::Invalid);
        }
        if let Some(binding) = &self.binding {
            binding.validate()?;
        }
        if let Some(pause) = self.pause {
            pause.validate()?;
        }
        if self.retry_at_ms.is_some()
            != self
                .pause
                .is_some_and(|pause| pause.disposition == super::super::Disposition::Retry)
        {
            return Err(BackupError::Invalid);
        }
        Ok(())
    }
}

pub(super) fn now_ms() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| BackupError::Invalid)?
        .as_millis()
        .try_into()
        .map_err(|_| BackupError::Limit)
}
