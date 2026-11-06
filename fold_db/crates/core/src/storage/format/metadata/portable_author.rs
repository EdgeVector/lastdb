//! The author position belongs to the same durable request as the records.

use super::{EncodedMetadata, Metadata};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::shared::{LogHead, WriterStream};
use lastdb_model::portable_mutation::{HomeReplayState, Position, Stamp, StreamId, StreamProgress};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";
const REPLAY_STATE_KEY: &str = "portable-replay-state";
const PROGRESS_PREFIX: &str = "portable-progress:";
const MAX_LOCAL_AUTHOR_BYTES: usize = 8192;

mod checkpoint;
mod cleanup;
mod inbox;
pub(crate) use checkpoint::CheckpointDirectory;
mod outgoing;
pub(crate) use inbox::InboxWindow;
pub use inbox::PortablePeerProgress;
pub use outgoing::OutgoingPortablePage;

pub(crate) struct AuthoredStamp {
    pub(crate) stamp: Stamp,
    pub(crate) shared_schema: Option<JournalSchema>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalSchema {
    pub(crate) id: String,
    pub(crate) local_sha256: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SharedAuthor {
    scope: WriterStream,
    schemas: BTreeMap<String, JournalSchema>,
    progress: StreamProgress,
    uploaded_head: Option<LogHead>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAuthor {
    stream: StreamId,
    progress: StreamProgress,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clock_floor_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shared: Option<SharedAuthor>,
}

pub(in crate::storage) fn initial_local_author(home: uuid::Uuid) -> StorageResult<Vec<u8>> {
    let state = LocalAuthor {
        stream: StreamId::try_from(home.to_string()).map_err(invalid)?,
        progress: StreamProgress::default(),
        clock_floor_ms: None,
        shared: None,
    };
    serde_json::to_vec(&state).map_err(invalid)
}

pub(in crate::storage) fn validate_local_author(bytes: Option<&[u8]>) -> StorageResult<()> {
    LocalAuthor::decode(bytes).map(|_| ())
}

impl LocalAuthor {
    fn decode(bytes: Option<&[u8]>) -> StorageResult<Self> {
        let bytes = bytes.ok_or_else(|| invalid("local stream state is missing"))?;
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid("local stream state exceeds its bound"));
        }
        let state: Self = serde_json::from_slice(bytes).map_err(invalid)?;
        state.progress.validate().map_err(invalid)?;
        if let Some(shared) = &state.shared {
            shared.validate()?;
        }
        Ok(state)
    }

    fn author(&mut self, schema: &str, now_ms: u64) -> StorageResult<AuthoredStamp> {
        let now_ms = now_ms.max(self.clock_floor_ms.unwrap_or_default());
        if let Some(shared) = &mut self.shared {
            if let Some(schema_id) = shared.schemas.get(schema) {
                let stream = StreamId::try_from(shared.scope.source_stream.as_str().to_owned())
                    .map_err(invalid)?;
                return Ok(AuthoredStamp {
                    stamp: next_stamp(&stream, &mut shared.progress, now_ms)?,
                    shared_schema: Some(schema_id.clone()),
                });
            }
        }
        Ok(AuthoredStamp {
            stamp: next_stamp(&self.stream, &mut self.progress, now_ms)?,
            shared_schema: None,
        })
    }
}

fn next_stamp(
    stream: &StreamId,
    progress: &mut StreamProgress,
    now_ms: u64,
) -> StorageResult<Stamp> {
    let sequence = progress
        .applied_through
        .checked_add(1)
        .ok_or_else(|| invalid("local stream sequence exhausted"))?;
    let authored_at_ms = now_ms.max(progress.applied_through_ms.unwrap_or_default());
    progress.applied_through = sequence;
    progress.applied_through_ms = Some(authored_at_ms);
    Ok(Stamp {
        authored_at_ms,
        position: Position {
            source_stream: stream.clone(),
            sequence,
        },
    })
}

impl Metadata {
    pub(crate) async fn stage_local_stamps(
        &self,
        schemas: &[(&str, Option<Stamp>)],
    ) -> StorageResult<Vec<AuthoredStamp>> {
        self.require_scope()?;
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(invalid)?
                .as_millis(),
        )
        .map_err(invalid)?;
        let mut state = self.local_author().await?;
        let stamps: Vec<_> = schemas
            .iter()
            .map(|(schema, imported)| {
                if let Some(stamp) = imported {
                    if state
                        .shared
                        .as_ref()
                        .is_some_and(|shared| shared.schemas.contains_key(*schema))
                    {
                        return Err(invalid("legacy imports cannot enter a shared stream"));
                    }
                    return Ok(AuthoredStamp {
                        stamp: stamp.clone(),
                        shared_schema: None,
                    });
                }
                state.author(schema, now_ms)
            })
            .collect::<StorageResult<_>>()?;
        if let Some(shared) = state.shared.as_ref().filter(|_| {
            stamps
                .iter()
                .any(|stamp: &AuthoredStamp| stamp.shared_schema.is_some())
        }) {
            let mut progress = shared.progress;
            progress.uploaded_through = None;
            let cursor = EncodedMetadata {
                key: format!("{PROGRESS_PREFIX}{}", shared.scope.source_stream.as_str()),
                bytes: serde_json::to_vec(&progress).map_err(invalid)?,
            };
            self.stage_batch(vec![author_row(&state)?, cursor]).await?;
        } else if schemas.iter().any(|(_, imported)| imported.is_none()) {
            self.stage_local_author(&state).await?;
        }
        Ok(stamps)
    }

    async fn local_author(&self) -> StorageResult<LocalAuthor> {
        let bytes = self.metadata_store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?;
        LocalAuthor::decode(bytes.as_deref())
    }

    async fn stage_local_author(&self, state: &LocalAuthor) -> StorageResult<()> {
        self.stage(author_row(state)?).await
    }

    pub(crate) async fn stage_portable_restore(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
        fresh_stream: &StreamId,
    ) -> StorageResult<()> {
        self.require_scope()?;
        if progress.len() >= cloud_backup_protocol::shared::MAX_WRITERS
            || progress.contains_key(fresh_stream)
        {
            return Err(invalid("invalid restored stream directory"));
        }
        let prior = progress
            .values()
            .filter_map(|cursor| cursor.applied_through_ms)
            .chain(cutoff)
            .max();
        let author = LocalAuthor {
            stream: fresh_stream.clone(),
            progress: StreamProgress::default(),
            clock_floor_ms: prior
                .map(|ms| {
                    ms.checked_add(1)
                        .ok_or_else(|| invalid("delete boundary exhausted"))
                })
                .transpose()?,
            shared: None,
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

fn author_row(state: &LocalAuthor) -> StorageResult<EncodedMetadata> {
    let bytes = serde_json::to_vec(state).map_err(invalid)?;
    if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
        return Err(invalid("local stream state exceeds its bound"));
    }
    Ok(EncodedMetadata {
        key: LOCAL_AUTHOR_KEY.into(),
        bytes,
    })
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("portable author state: {error}"))
}
