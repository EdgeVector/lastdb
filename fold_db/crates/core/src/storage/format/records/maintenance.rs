//! Retained offline replay-floor and in-request audit writes.

use super::{RecordDeleteLedgerEntry, RecordFormat};
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::error::StorageResult;
use crate::storage::format::{control, DELETE_AUDIT_FAMILY, REPLAY_CUTOFF_VERSION};
use crate::storage::{KvMutation, StorageError};

pub(crate) fn validate_cutoff_raise(previous: u64, cutoff: u64) -> StorageResult<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(control::invalid)?
        .as_nanos();
    if cutoff == 0 || cutoff < previous || (cutoff > previous && u128::from(cutoff) > now) {
        return Err(control::invalid(
            "the cutoff must be positive, cannot decrease, and cannot move into the future",
        ));
    }
    Ok(())
}

impl RecordFormat {
    /// The one offline floor writer rechecks its monotonicity under the gate.
    pub(crate) async fn raise_replay_cutoff(&self, cutoff: u64) -> StorageResult<()> {
        transaction::scope(self.runtime.clone(), async {
            if self
                .runtime
                .serving
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(control::invalid(
                    "a raise requires a stopped-home maintenance instance",
                ));
            }
            let rows = self
                .main_store
                .inner()
                .get_many(vec![
                    control::CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                    control::DELETE_REPLAY_CUTOFF_KEY.as_bytes().to_vec(),
                ])
                .await?;
            if rows.len() != 2 {
                return Err(control::invalid("incomplete fence read"));
            }
            let previous =
                control::validate_frozen_replay_floor(rows[0].as_deref(), rows[1].as_deref())?;
            validate_cutoff_raise(previous, cutoff)?;
            let floor = serde_json::to_vec(&control::Cutoff {
                v: REPLAY_CUTOFF_VERSION,
                minimum_written_at_nanos: cutoff,
            })
            .map_err(control::invalid)?;
            self.main_store
                .inner()
                .batch_mutate(vec![KvMutation::put(
                    control::DELETE_REPLAY_CUTOFF_KEY.as_bytes(),
                    floor,
                )])
                .await?;
            Ok::<_, StorageError>(())
        })
        .await?;
        self.flush().await
    }

    pub(crate) async fn record_delete_audit(
        &self,
        entry: RecordDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        self.require_scope()?;
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let key = lastdb_model::kind_partition::anchored(
            DELETE_AUDIT_FAMILY,
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let bytes = serde_json::to_vec(&entry)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?;
        self.main_store
            .inner()
            .put(key.as_bytes(), bytes)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("stage erase audit: {error}")))
    }
}
