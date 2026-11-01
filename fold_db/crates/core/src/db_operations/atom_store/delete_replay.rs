//! The global replay cutoff of a home: a write older than it is refused. Only
//! stopped-home maintenance (`lastdb db recount --delete-cutoff-unix-secs`)
//! moves it, and only up.

use super::AtomStore;
use crate::records::key_format::{
    validate_frozen_replay_floor, Cutoff, DELETE_REPLAY_CUTOFF_KEY as CUTOFF_KEY,
};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
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

impl AtomStore {
    /// The floor is global to this home and changes only through stopped-home
    /// maintenance.
    pub(crate) async fn minimum_replay_time(&self) -> StorageResult<u64> {
        if let Some(cutoff) = self.serving_replay_cutoff.get() {
            return Ok(*cutoff);
        }
        self.read_minimum_replay_time().await
    }

    /// The serving process cannot advance this boundary. Offline recount uses
    /// a separate instance without this cache and always reads its disk state.
    pub(crate) async fn freeze_replay_cutoff(&self) -> StorageResult<()> {
        if let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
        {
            runtime.begin_serving().await?;
        }
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
                Self::CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                CUTOFF_KEY.as_bytes().to_vec(),
            ])
            .await?;
        if values.len() != 2 {
            return Err(invalid("incomplete fence read"));
        }
        validate_frozen_replay_floor(values[0].as_deref(), values[1].as_deref())
    }

    /// Explicit offline maintenance only. The caller holds the stopped-home
    /// lock. The cutoff never moves down or into the future.
    pub(crate) async fn raise_replay_cutoff(
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(invalid)?
            .as_nanos();
        if cutoff == 0 || cutoff < previous || (cutoff > previous && u128::from(cutoff) > now) {
            return Err(invalid(
                "the cutoff must be positive, cannot decrease, and cannot move into the future",
            ));
        }
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
            let floor = serde_json::to_vec(&Cutoff {
                v: 1,
                minimum_written_at_nanos: cutoff,
            })
            .map_err(invalid)?;
            self.main_store
                .inner()
                .batch_mutate(vec![KvMutation::put(CUTOFF_KEY.as_bytes(), floor)])
                .await?;
            self.main_store.inner().flush().await?;
        }
        Ok(report)
    }
}
