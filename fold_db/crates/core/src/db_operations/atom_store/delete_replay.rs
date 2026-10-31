//! Explicit offline delete-barrier retirement behind a durable replay fence.

use super::{AtomStore, RangeWalk};
use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier};
use crate::records::key_format::{
    validate_frozen_replay_floor, Cutoff, DELETE_REPLAY_CUTOFF_KEY as CUTOFF_KEY,
};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletePruneReport {
    pub executed: bool,
    pub cutoff_before_nanos: u64,
    pub cutoff_after_nanos: u64,
    pub examined: u64,
    pub pruned: u64,
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
    /// lock and includes every requested scope. Fence publication completes
    /// before the first barrier delete; interruption can leave extra barriers.
    pub(crate) async fn prune_delete_barriers(
        &self,
        scopes: &[Option<String>],
        cutoff: u64,
        execute: bool,
    ) -> StorageResult<DeletePruneReport> {
        if self.serving_replay_cutoff.get().is_some() {
            return Err(invalid(
                "cleanup requires a stopped-home maintenance instance",
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
        let mut report = DeletePruneReport {
            executed: execute,
            cutoff_before_nanos: previous,
            cutoff_after_nanos: cutoff,
            ..Default::default()
        };
        if previous != cutoff {
            report.operations += 1;
            report.batches += 1;
        }
        if execute && previous != cutoff {
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
        for scope in scopes {
            self.prune_delete_scope(scope.as_deref(), cutoff, execute, &mut report)
                .await?;
        }
        Ok(report)
    }

    async fn prune_delete_scope(
        &self,
        scope: Option<&str>,
        cutoff: u64,
        execute: bool,
        report: &mut DeletePruneReport,
    ) -> StorageResult<()> {
        let prefix = build_storage_key(scope, "rdel:v2:");
        let end = FilterUtils::create_prefix_end(&prefix).map_err(invalid)?;
        let mut walk = RangeWalk::new(prefix.as_bytes(), end.as_bytes(), 256);
        while !walk.done() {
            let page = self.walk_step(&mut walk).await.map_err(invalid)?;
            let mut deletes = Vec::new();
            for (key, value) in page.rows {
                let barrier: DeleteBarrier = serde_json::from_slice(&value).map_err(invalid)?;
                if delete_barrier_key(barrier.mk_key.as_bytes()).as_bytes() != key.as_slice() {
                    return Err(invalid(
                        "a barrier disagrees with its key; the durable fence remains in force",
                    ));
                }
                report.examined += 1;
                if barrier.written_at <= cutoff {
                    deletes.push(KvMutation::delete(key));
                }
            }
            report.pruned += deletes.len() as u64;
            report.operations += deletes.len() as u64;
            report.batches += u64::from(!deletes.is_empty());
            if execute && !deletes.is_empty() {
                self.main_store.inner().batch_mutate(deletes).await?;
            }
        }
        Ok(())
    }
}
