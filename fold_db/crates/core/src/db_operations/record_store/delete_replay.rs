//! The global replay cutoff of a home: a write older than it is refused.

use super::RecordStore;
use crate::storage::error::StorageResult;
use crate::storage::format::control::{
    validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY as CUTOFF_KEY,
};
use crate::storage::StorageError;

fn invalid(text: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("delete replay cutoff: {text}"))
}

impl RecordStore {
    pub(crate) async fn minimum_replay_time(&self) -> StorageResult<u64> {
        if let Some(cutoff) = self.serving_replay_cutoff.get() {
            return Ok(*cutoff);
        }
        self.read_minimum_replay_time().await
    }

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
}
