//! The author position belongs to the same durable request as the records.

use super::{EncodedMetadata, Metadata};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::portable_mutation::{Position, Stamp, StreamId, StreamProgress};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";

const MAX_LOCAL_AUTHOR_BYTES: usize = 8192;

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
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid("local stream state exceeds its bound"));
        }
        let state: Self = serde_json::from_slice(bytes).map_err(invalid)?;
        state.progress.validate().map_err(invalid)?;
        Ok(state)
    }

    fn author(&mut self, now_ms: u64) -> StorageResult<Stamp> {
        let now_ms = now_ms.max(self.clock_floor_ms.unwrap_or_default());
        next_stamp(&self.stream, &mut self.progress, now_ms)
    }
}

fn now_ms() -> StorageResult<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(invalid)?
            .as_millis(),
    )
    .map_err(invalid)
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
    pub(crate) async fn stage_local_stamps(&self, schemas: &[&str]) -> StorageResult<Vec<Stamp>> {
        self.require_scope()?;
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let now_ms = now_ms()?;
        let mut state = self.local_author().await?;
        let stamps = schemas
            .iter()
            .map(|_| state.author(now_ms))
            .collect::<StorageResult<Vec<_>>>()?;
        self.stage_local_author(&state).await?;
        Ok(stamps)
    }

    async fn local_author(&self) -> StorageResult<LocalAuthor> {
        let bytes = self.metadata_store.get(LOCAL_AUTHOR_KEY.as_bytes()).await?;
        LocalAuthor::decode(bytes.as_deref())
    }

    async fn stage_local_author(&self, state: &LocalAuthor) -> StorageResult<()> {
        self.stage(author_row(state)?).await
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
