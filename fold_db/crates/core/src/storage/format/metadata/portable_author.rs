//! The author position belongs to the same durable request as the records.

use super::{EncodedMetadata, Metadata};
use crate::storage::error::{StorageError, StorageResult};
use cloud_backup_protocol::shared::{LogHead, WriterStream};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{HomeReplayState, Position, Stamp, StreamId, StreamProgress};
use schema_types::PortableSchemaDefinition;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";
const REPLAY_STATE_KEY: &str = "portable-replay-state";
const RESTORE_OWNER_KEY: &str = "portable-restore-owner";
const PROGRESS_PREFIX: &str = "portable-progress:";
const DEFINITION_PREFIX: &str = "portable-schema:";
const VERSIONS_PREFIX: &str = "portable-schema-versions:";
/// Replay coordinates and schema definitions travel with the shared records.
pub(in crate::storage) fn shared_metadata_key(key: &str) -> bool {
    key == REPLAY_STATE_KEY
        || key.starts_with(PROGRESS_PREFIX)
        || key.starts_with(DEFINITION_PREFIX)
        || key.starts_with(VERSIONS_PREFIX)
}

const MAX_LOCAL_AUTHOR_BYTES: usize = 8192;

mod checkpoint;
mod cleanup;
mod definitions;
mod inbox;
pub(crate) use checkpoint::CheckpointDirectory;
pub(crate) use definitions::DefinitionSet;
mod outgoing;
mod rejoin;
mod retention;
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
    /// Schema IDs whose definition this stream already carries.
    defined: BTreeSet<String>,
    progress: StreamProgress,
    uploaded_head: Option<LogHead>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uploaded_through_ms: Option<u64>,
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RestoreOwner {
    database: Id,
    device: Id,
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

    /// Author one definition entry before the schema's first record in this stream.
    fn define(
        &mut self,
        definition: &PortableSchemaDefinition,
        now_ms: u64,
    ) -> StorageResult<EncodedMetadata> {
        let now_ms = now_ms.max(self.clock_floor_ms.unwrap_or_default());
        let shared = self
            .shared
            .as_mut()
            .ok_or_else(|| invalid("shared journal is not bound"))?;
        let stream =
            StreamId::try_from(shared.scope.source_stream.as_str().to_owned()).map_err(invalid)?;
        let stamp = next_stamp(&stream, &mut shared.progress, now_ms)?;
        shared.defined.insert(definition.id().as_str().to_owned());
        definitions::define_row(&stream, stamp, definition)
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
    pub(crate) async fn stage_local_stamps(
        &self,
        schemas: &[&str],
    ) -> StorageResult<Vec<AuthoredStamp>> {
        self.require_scope()?;
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let now_ms = now_ms()?;
        let mut state = self.local_author().await?;
        let defines = self
            .first_definitions(&state, schemas)
            .await?
            .iter()
            .map(|definition| state.define(definition, now_ms))
            .collect::<StorageResult<Vec<_>>>()?;
        let stamps: Vec<_> = schemas
            .iter()
            .map(|schema| {
                // A peer's definition can install a shared schema that this
                // writer has not admitted. Its stream never defined that schema.
                if self.runtime.is_shared_schema(schema)
                    && !state
                        .shared
                        .as_ref()
                        .is_some_and(|shared| shared.schemas.contains_key(*schema))
                {
                    return Err(invalid(
                        "shared schema writes require an active writer binding that admits the schema",
                    ));
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
            let mut rows = vec![author_row(&state)?, cursor];
            rows.extend(defines);
            self.stage_batch(rows).await?;
        } else {
            self.stage_local_author(&state).await?;
        }
        Ok(stamps)
    }

    async fn writer_replay_state(
        &self,
        stream: &WriterStream,
        device: &Id,
        initialize_publisher: bool,
    ) -> StorageResult<Option<Vec<u8>>> {
        let rows = self
            .metadata_store
            .get_many(vec![
                REPLAY_STATE_KEY.as_bytes().to_vec(),
                RESTORE_OWNER_KEY.as_bytes().to_vec(),
            ])
            .await?;
        let [replay, owner]: [_; 2] = rows
            .try_into()
            .map_err(|_| invalid("incomplete writer origin read"))?;
        if initialize_publisher {
            return if replay.is_none() && owner.is_none() {
                Ok(None)
            } else {
                Err(invalid("initial publisher requires a fresh home"))
            };
        }
        let Some(owner) = owner.filter(|_| replay.is_some()) else {
            return Err(invalid(
                "restore the complete shared snapshot and mutation logs before binding",
            ));
        };
        let restored: RestoreOwner = serde_json::from_slice(&owner).map_err(invalid)?;
        if restored.database != stream.database_id || &restored.device != device {
            return Err(invalid(
                "shared restore belongs to another database or device",
            ));
        }
        Ok(replay)
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
        database: &Id,
        device: &Id,
    ) -> StorageResult<()> {
        self.require_scope()?;
        if progress.len() > cloud_backup_protocol::shared::MAX_WRITERS
            || progress.contains_key(fresh_stream)
        {
            return Err(invalid("invalid restored stream directory"));
        }
        let author = LocalAuthor {
            stream: fresh_stream.clone(),
            progress: StreamProgress::default(),
            clock_floor_ms: rejoin::restored_clock_floor(progress, cutoff)?,
            shared: None,
        };
        self.stage_restored_author(
            progress,
            cutoff,
            fresh_stream,
            &author,
            RestoreOwner {
                database: database.clone(),
                device: device.clone(),
            },
        )
        .await
    }

    pub(crate) async fn validate_portable_restore_checkpoint(
        &self,
        progress: &BTreeMap<StreamId, StreamProgress>,
        cutoff: Option<u64>,
    ) -> StorageResult<()> {
        if progress.len() > cloud_backup_protocol::shared::MAX_WRITERS {
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
