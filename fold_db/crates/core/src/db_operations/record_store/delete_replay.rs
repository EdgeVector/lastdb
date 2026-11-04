//! The global replay cutoff of a home: a write older than it is refused. Only
//! stopped-home maintenance (`lastdb db recount --delete-cutoff-unix-secs`)
//! moves it, and only up.

use super::RecordStore;
use crate::storage::error::StorageResult;
use crate::storage::format::control::{
    validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY as CUTOFF_KEY,
};
use crate::storage::StorageError;
use serde::{Deserialize, Serialize};

/// What a raise of the replay cutoff did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayCutoffReport {
    pub executed: bool,
    pub cutoff_before_nanos: u64,
    pub cutoff_after_nanos: u64,
    pub operations: u64,
    pub batches: u64,
}

fn invalid(text: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("delete replay cutoff: {text}"))
}

impl RecordStore {
    /// The floor is global to this home and changes only through stopped-home
    /// maintenance.
    pub(crate) async fn minimum_replay_time(&self) -> StorageResult<u64> {
        if let Some(cutoff) = self.serving_replay_cutoff.get() {
            return Ok(*cutoff);
        }
        self.read_minimum_replay_time().await
    }

    /// The serving process cannot advance this boundary. The offline raise uses
    /// a separate instance without this cache and always reads its disk state.
    pub(crate) async fn freeze_replay_cutoff(&self) -> StorageResult<()> {
        self.runtime.begin_serving();
        let cutoff = self.read_minimum_replay_time().await?;
        if let Err(cutoff) = self.serving_replay_cutoff.set(cutoff) {
            if self.serving_replay_cutoff.get() != Some(&cutoff) {
                return Err(invalid("serving replay floor changed after startup"));
            }
        }
        Ok(())
    }

    async fn read_minimum_replay_time(&self) -> StorageResult<u64> {
        let values = self
            .main_store
            .inner()
            .get_many(vec![
                crate::storage::format::CURRENT_KEY_FORMAT_KEY
                    .as_bytes()
                    .to_vec(),
                CUTOFF_KEY.as_bytes().to_vec(),
            ])
            .await?;
        if values.len() != 2 {
            return Err(invalid("incomplete fence read"));
        }
        validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())
    }

    /// Explicit offline maintenance only (`lastdb db recount`). The caller
    /// holds the stopped-home lock. The cutoff never moves down or into the
    /// future.
    pub async fn raise_replay_cutoff(
        &self,
        cutoff: u64,
        execute: bool,
    ) -> StorageResult<ReplayCutoffReport> {
        if self.serving_replay_cutoff.get().is_some() {
            return Err(invalid(
                "a raise requires a stopped-home maintenance instance",
            ));
        }
        let previous = self.minimum_replay_time().await?;
        crate::storage::format::records::validate_cutoff_raise(previous, cutoff)?;
        let mut report = ReplayCutoffReport {
            executed: execute,
            cutoff_before_nanos: previous,
            cutoff_after_nanos: cutoff,
            ..Default::default()
        };
        if previous == cutoff {
            return Ok(report);
        }
        report.operations = 1;
        report.batches = 1;
        if execute {
            self.format.raise_replay_cutoff(cutoff).await?;
        }
        Ok(report)
    }
}
