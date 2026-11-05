//! The author position belongs to the same durable request as the records.

use super::{EncodedMetadata, Metadata};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::portable_mutation::{Position, Stamp, StreamId, StreamProgress};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAuthor {
    stream: StreamId,
    progress: StreamProgress,
}

pub(in crate::storage) fn initial_local_author(home: uuid::Uuid) -> StorageResult<Vec<u8>> {
    let state = LocalAuthor {
        stream: StreamId::try_from(home.to_string()).map_err(invalid)?,
        progress: StreamProgress::default(),
    };
    serde_json::to_vec(&state).map_err(invalid)
}

pub(in crate::storage) fn validate_local_author(bytes: Option<&[u8]>) -> StorageResult<()> {
    LocalAuthor::decode(bytes).map(|_| ())
}

impl LocalAuthor {
    fn decode(bytes: Option<&[u8]>) -> StorageResult<Self> {
        let bytes = bytes.ok_or_else(|| invalid("local stream state is missing"))?;
        let state: Self = serde_json::from_slice(bytes).map_err(invalid)?;
        state.progress.validate().map_err(invalid)?;
        Ok(state)
    }

    fn author(&mut self, count: usize, now_ms: u64) -> StorageResult<Vec<Stamp>> {
        let count = u64::try_from(count).map_err(invalid)?;
        let start = self.progress.applied_through;
        let end = start
            .checked_add(count)
            .ok_or_else(|| invalid("local stream sequence exhausted"))?;
        let authored_at_ms = now_ms.max(self.progress.applied_through_ms.unwrap_or_default());
        let stamps = (start..end)
            .map(|previous| Stamp {
                authored_at_ms,
                position: Position {
                    source_stream: self.stream.clone(),
                    sequence: previous + 1,
                },
            })
            .collect();
        self.progress.applied_through = end;
        self.progress.applied_through_ms = Some(authored_at_ms);
        Ok(stamps)
    }
}

impl Metadata {
    pub(crate) async fn stage_local_stamps(&self, count: usize) -> StorageResult<Vec<Stamp>> {
        self.require_scope()?;
        if count == 0 {
            return Ok(Vec::new());
        }
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(invalid)?
                .as_millis(),
        )
        .map_err(invalid)?;
        let bytes = self.metadata_store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?;
        let mut state = LocalAuthor::decode(bytes.as_deref())?;
        let stamps = state.author(count, now_ms)?;
        self.stage(EncodedMetadata {
            key: LOCAL_AUTHOR_KEY.into(),
            bytes: serde_json::to_vec(&state).map_err(invalid)?,
        })
        .await?;
        Ok(stamps)
    }
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("portable author state: {error}"))
}
