//! The author position belongs to the same durable request as the records.

use super::{EncodedMetadata, Metadata};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::portable_mutation::{HomeReplayState, Position, Stamp, StreamId, StreamProgress};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";
const REPLAY_STATE_KEY: &str = "portable-replay-state";
const PROGRESS_PREFIX: &str = "portable-progress:";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAuthor {
    stream: StreamId,
    progress: StreamProgress,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clock_floor_ms: Option<u64>,
}

pub(in crate::storage) fn initial_local_author(home: uuid::Uuid) -> StorageResult<Vec<u8>> {
    let state = LocalAuthor {
        stream: StreamId::try_from(home.to_string()).map_err(invalid)?,
        progress: StreamProgress::default(),
        clock_floor_ms: None,
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
        let authored_at_ms = now_ms
            .max(self.progress.applied_through_ms.unwrap_or_default())
            .max(self.clock_floor_ms.unwrap_or_default());
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

    pub(crate) async fn stage_portable_restore(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        fresh_stream: &StreamId,
    ) -> StorageResult<()> {
        self.require_scope()?;
        if progress.len() > 4094 || progress.contains_key(fresh_stream) {
            return Err(invalid("invalid restored stream directory"));
        }
        let author = LocalAuthor {
            stream: fresh_stream.clone(),
            progress: StreamProgress::default(),
            clock_floor_ms: cutoff
                .map(|ms| {
                    ms.checked_add(1)
                        .ok_or_else(|| invalid("delete boundary exhausted"))
                })
                .transpose()?,
        };
        let state = HomeReplayState {
            local_stream_id: fresh_stream.clone(),
            participating_streams: progress
                .keys()
                .cloned()
                .chain([fresh_stream.clone()])
                .collect(),
            delete_reclaimed_through_ms: cutoff,
        };
        let mut rows = Vec::with_capacity(progress.len() + 2);
        for (stream, cursor) in progress {
            cursor.validate().map_err(invalid)?;
            if cursor.uploaded_through.is_some() {
                return Err(invalid("restored peer cursor has local upload state"));
            }
            rows.push(EncodedMetadata {
                key: format!("{PROGRESS_PREFIX}{stream}"),
                bytes: serde_json::to_vec(cursor).map_err(invalid)?,
            });
        }
        rows.push(EncodedMetadata {
            key: REPLAY_STATE_KEY.into(),
            bytes: serde_json::to_vec(&state).map_err(invalid)?,
        });
        rows.push(EncodedMetadata {
            key: LOCAL_AUTHOR_KEY.into(),
            bytes: serde_json::to_vec(&author).map_err(invalid)?,
        });
        self.stage_batch(rows).await
    }

    pub(crate) async fn validate_portable_restore_checkpoint(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
    ) -> StorageResult<()> {
        if progress.len() > 4094 {
            return Err(invalid("invalid restored stream directory"));
        }
        let keys = std::iter::once(REPLAY_STATE_KEY.as_bytes().to_vec())
            .chain(
                progress
                    .keys()
                    .map(|stream| format!("{PROGRESS_PREFIX}{stream}").into_bytes()),
            )
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() != progress.len() + 1 {
            return Err(invalid("incomplete restored replay state"));
        }
        let Some(bytes) = rows[0].as_deref() else {
            if progress.is_empty() && cutoff.is_none() {
                return Ok(());
            }
            return Err(invalid("checkpoint replay state is missing"));
        };
        let state: HomeReplayState = serde_json::from_slice(bytes).map_err(invalid)?;
        let participants = state
            .participating_streams
            .iter()
            .filter(|stream| **stream != state.local_stream_id || progress.contains_key(*stream));
        if state.delete_reclaimed_through_ms != cutoff || participants.ne(progress.keys()) {
            return Err(invalid("checkpoint participants or delete boundary differ"));
        }
        for (expected, bytes) in progress.values().zip(&rows[1..]) {
            expected.validate().map_err(invalid)?;
            let bytes = bytes
                .as_deref()
                .ok_or_else(|| invalid("checkpoint stream cursor missing"))?;
            let actual: StreamProgress = serde_json::from_slice(bytes).map_err(invalid)?;
            actual.validate().map_err(invalid)?;
            if (actual.applied_through, actual.applied_through_ms)
                != (expected.applied_through, expected.applied_through_ms)
            {
                return Err(invalid("checkpoint stream cursor differs"));
            }
        }
        Ok(())
    }
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("portable author state: {error}"))
}
