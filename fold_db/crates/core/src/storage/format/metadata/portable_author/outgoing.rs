//! The local shared mutation log is written under the native record transaction.

use super::{
    author_row, inbox, invalid, EncodedMetadata, JournalSchema, LocalAuthor, Metadata,
    SharedAuthor, REPLAY_STATE_KEY,
};
use crate::storage::error::StorageResult;
use cloud_backup_protocol::shared::{BatchReceipt, LogBatch, LogHead, WriterStream, MAX_WRITERS};
use cloud_backup_protocol::Id;
use lastdb_model::portable_mutation::{
    HomeReplayState, MutationEntry, MutationOperation, Position, SchemaIdentity, Stamp, StreamId,
    StreamProgress,
};
use schema_types::PortableSchemaDefinition;
use std::collections::{BTreeMap, BTreeSet};

const ENTRY_PREFIX: &str = "portable-entry:";
const MAX_PAGE_ENTRIES: usize = 64;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024 - 4096;

pub struct OutgoingPortablePage {
    pub stream: WriterStream,
    pub uploaded_head: Option<LogHead>,
    pub entries: Vec<MutationEntry>,
}

impl SharedAuthor {
    pub(super) fn validate(&self) -> StorageResult<()> {
        self.progress.validate().map_err(invalid)?;
        if let Some(head) = &self.uploaded_head {
            head.validate().map_err(invalid)?;
        }
        if self.schemas.is_empty()
            || self.schemas.len() > 64
            || self
                .schemas
                .values()
                .map(|schema| &schema.id)
                .collect::<BTreeSet<_>>()
                .len()
                != self.schemas.len()
            || !self
                .defined
                .iter()
                .all(|id| self.schemas.values().any(|schema| &schema.id == id))
            || self.progress.uploaded_through
                != self.uploaded_head.as_ref().map(|head| head.sequence.get())
            || self
                .progress
                .uploaded_through
                .is_some_and(|n| n > self.progress.applied_through)
            || self.uploaded_through_ms.is_some_and(|time| {
                self.uploaded_head.is_none() || Some(time) > self.progress.applied_through_ms
            })
        {
            return Err(invalid("invalid shared author state"));
        }
        for (alias, schema) in &self.schemas {
            SchemaIdentity::Local(alias.clone())
                .validate()
                .map_err(invalid)?;
            SchemaIdentity::Shared(schema.id.clone())
                .validate()
                .map_err(invalid)?;
            schema
                .id
                .parse::<schema_types::PortableSchemaId>()
                .map_err(invalid)?;
            schema
                .local_sha256
                .parse::<schema_types::PortableSchemaId>()
                .map_err(invalid)?;
        }
        Ok(())
    }
}

impl Metadata {
    pub(crate) async fn portable_writer_progress(
        &self,
    ) -> StorageResult<(WriterStream, StreamProgress)> {
        let author = self
            .local_author()
            .await?
            .shared
            .ok_or_else(|| invalid("shared writer is not bound"))?;
        Ok((author.scope, author.progress))
    }

    /// A shared schema stays while any writer or snapshot can name it.
    pub(crate) async fn shared_schema_bound(&self, name: &str) -> StorageResult<bool> {
        if self.runtime.is_shared_schema(name) {
            return Ok(true);
        }
        let state = self.local_author().await?;
        Ok(state
            .shared
            .is_some_and(|shared| shared.schemas.contains_key(name)))
    }

    /// Bind a fresh writer. Its stream defines each schema before that schema's first record.
    pub(crate) async fn bind_portable_journal(
        &self,
        stream: WriterStream,
        schemas: BTreeMap<String, JournalSchema>,
        device: &Id,
        initialize_publisher: bool,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let mut state = self.local_author().await?;
        if let Some(current) = &state.shared {
            if current.scope == stream && current.schemas == schemas {
                return Ok(());
            }
            return Err(invalid("portable journal already has a different binding"));
        }
        if state.progress.applied_through != 0 {
            return Err(invalid("a shared journal requires a fresh local author"));
        }
        let prior_stream = state.stream.clone();
        self.runtime.require_direct_shared_home()?;
        let existing = self
            .writer_replay_state(&stream, device, initialize_publisher)
            .await?;
        if prior_stream.as_str() == stream.source_stream.as_str() {
            if existing.is_none() {
                return Err(invalid("the shared writer needs a distinct source stream"));
            }
            // The restored author already owns this fresh source. Keep
            // subsequent device-only mutations on a separate sequence.
            state.stream = StreamId::try_from(uuid::Uuid::new_v4().to_string()).map_err(invalid)?;
        }
        let shared = SharedAuthor {
            scope: stream,
            schemas,
            defined: BTreeSet::new(),
            progress: StreamProgress::default(),
            uploaded_head: None,
            uploaded_through_ms: None,
        };
        shared.validate()?;
        let local_stream: StreamId = shared
            .scope
            .source_stream
            .as_str()
            .parse()
            .map_err(invalid)?;
        let mut replay = existing
            .as_deref()
            .map(|bytes| inbox::decode_state(Some(bytes)))
            .transpose()?
            .unwrap_or_else(|| HomeReplayState {
                local_stream_id: prior_stream.clone(),
                participating_streams: BTreeSet::new(),
                delete_reclaimed_through_ms: None,
            });
        if replay.local_stream_id != prior_stream {
            return Err(invalid("restored local author differs from replay state"));
        }
        if local_stream != prior_stream && replay.participating_streams.contains(&local_stream) {
            // Earlier restores recorded idle writers with zero progress.
            let row = self
                .metadata_store
                .get(inbox::progress_key(&local_stream).as_bytes())
                .await?;
            if inbox::decode_cursor(row.as_deref())? != StreamProgress::default() {
                return Err(invalid(
                    "registered writer collides with a checkpoint stream",
                ));
            }
        }
        replay.participating_streams.remove(&prior_stream);
        replay.participating_streams.insert(local_stream.clone());
        if replay.participating_streams.len() > MAX_WRITERS {
            return Err(invalid(
                "restored writer directory is at its cloud capacity",
            ));
        }
        replay.local_stream_id = local_stream;
        state.shared = Some(shared);
        self.stage_batch(vec![
            author_row(&state)?,
            EncodedMetadata {
                key: REPLAY_STATE_KEY.into(),
                bytes: serde_json::to_vec(&replay).map_err(invalid)?,
            },
        ])
        .await
    }

    /// Extend a bound writer. The stream defines each new schema before its first record.
    pub(crate) async fn admit_portable_schemas(
        &self,
        schemas: BTreeMap<String, JournalSchema>,
    ) -> StorageResult<()> {
        self.require_scope()?;
        let mut state = self.local_author().await?;
        let shared = state
            .shared
            .as_mut()
            .ok_or_else(|| invalid("shared journal is not bound"))?;
        let mut added = false;
        for (alias, schema) in schemas {
            match shared.schemas.get(&alias) {
                Some(bound) if *bound == schema => continue,
                Some(_) => return Err(invalid("a bound shared alias cannot change")),
                None => {}
            }
            shared.schemas.insert(alias, schema);
            added = true;
        }
        if !added {
            return Ok(());
        }
        shared.validate()?;
        self.stage_local_author(&state).await
    }

    pub(crate) async fn portable_bound_schemas(&self) -> StorageResult<BTreeMap<String, String>> {
        let state = self.local_author().await?;
        Ok(shared(&state)?
            .schemas
            .iter()
            .map(|(alias, schema)| (schema.id.clone(), alias.clone()))
            .collect())
    }

    pub(crate) async fn stage_outgoing(&self, entries: Vec<MutationEntry>) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let rows = entries
            .into_iter()
            .map(|entry| {
                Ok(EncodedMetadata {
                    key: entry_key(
                        &entry.stamp.position.source_stream,
                        entry.stamp.position.sequence,
                    ),
                    bytes: entry.encode().map_err(invalid)?,
                })
            })
            .collect::<StorageResult<_>>()?;
        self.stage_batch(rows).await
    }

    pub(crate) async fn portable_outgoing(&self) -> StorageResult<OutgoingPortablePage> {
        let state = self.local_author().await?;
        let shared = shared(&state)?;
        let after = shared.progress.uploaded_through.unwrap_or(0);
        let through = after
            .saturating_add(MAX_PAGE_ENTRIES as u64)
            .min(shared.progress.applied_through);
        let (entries, _) = self.read_outgoing(shared, after, through).await?;
        Ok(OutgoingPortablePage {
            stream: shared.scope.clone(),
            uploaded_head: shared.uploaded_head.clone(),
            entries,
        })
    }

    pub(crate) async fn outgoing_schema_name(
        &self,
        identity: &SchemaIdentity,
    ) -> StorageResult<String> {
        let SchemaIdentity::Shared(id) = identity else {
            return Err(invalid("portable mutation log requires a shared schema"));
        };
        let state = self.local_author().await?;
        shared(&state)?
            .schemas
            .iter()
            .find_map(|(name, bound)| (&bound.id == id).then(|| name.clone()))
            .ok_or_else(|| invalid("outgoing schema has no admitted binding"))
    }

    pub(crate) async fn stage_outgoing_receipt(
        &self,
        batch: &LogBatch,
        receipt: &BatchReceipt,
    ) -> StorageResult<Vec<MutationEntry>> {
        self.require_scope()?;
        receipt.validate_for(batch).map_err(invalid)?;
        let mut state = self.local_author().await?;
        let shared = state
            .shared
            .as_mut()
            .ok_or_else(|| invalid("shared journal is not bound"))?;
        if shared.scope != batch.stream {
            return Err(invalid("cloud receipt belongs to a different writer"));
        }
        if shared.uploaded_head.as_ref() == Some(&receipt.head) {
            return Ok(Vec::new());
        }
        if shared.uploaded_head != batch.after {
            return Err(invalid("cloud receipt has a different predecessor"));
        }
        let after = shared.progress.uploaded_through.unwrap_or(0);
        let through = receipt.head.sequence.get();
        let (entries, covered) = self.read_outgoing(shared, after, through).await?;
        if covered != through || !same_file_versions(&entries, batch) {
            return Err(invalid(
                "cloud receipt does not cover the exact local range and file versions",
            ));
        }
        let last = entries
            .last()
            .ok_or_else(|| invalid("empty accepted mutation range"))?;
        shared.progress.uploaded_through = Some(through);
        shared.uploaded_head = Some(receipt.head.clone());
        shared.uploaded_through_ms = Some(last.stamp.authored_at_ms);
        let retire_through = through.min(shared.progress.applied_through);
        let retired: Vec<_> = entries
            .into_iter()
            .take_while(|entry| entry.stamp.position.sequence <= retire_through)
            .collect();
        let keys = retired
            .iter()
            .map(|entry| {
                let position = &entry.stamp.position;
                entry_key(&position.source_stream, position.sequence).into_bytes()
            })
            .collect();
        self.stage_local_author(&state).await?;
        self.stage_deletes(keys).await?;
        Ok(retired)
    }

    /// The authored time at the latest locally verified cloud receipt.
    pub(super) async fn portable_uploaded_time(
        &self,
        shared: &SharedAuthor,
    ) -> StorageResult<Option<u64>> {
        let Some(head) = &shared.uploaded_head else {
            return Ok(None);
        };
        if let Some(time) = shared.uploaded_through_ms {
            return Ok(Some(time));
        }
        let through = head.sequence.get();
        if through == shared.progress.applied_through {
            return Ok(shared.progress.applied_through_ms);
        }
        let (entries, _) = self.read_outgoing(shared, through - 1, through).await?;
        let entry = entries
            .first()
            .ok_or_else(|| invalid("accepted outgoing entry is missing"))?;
        Ok(Some(entry.stamp.authored_at_ms))
    }

    async fn read_outgoing(
        &self,
        shared: &SharedAuthor,
        after: u64,
        through: u64,
    ) -> StorageResult<(Vec<MutationEntry>, u64)> {
        if through < after
            || through > shared.progress.applied_through
            || through - after > MAX_PAGE_ENTRIES as u64
        {
            return Err(invalid("outgoing range exceeds its admitted local prefix"));
        }
        let stream =
            StreamId::try_from(shared.scope.source_stream.as_str().to_owned()).map_err(invalid)?;
        let keys = (after..through)
            .map(|n| entry_key(&stream, n + 1).into_bytes())
            .collect();
        let rows = self.metadata_store.get_many(keys).await?;
        if rows.len() as u64 != through - after {
            return Err(invalid("outgoing range read is incomplete"));
        }
        let mut size = 0;
        let mut entries = Vec::new();
        let mut covered = after;
        for (n, row) in (after..through).zip(rows) {
            let bytes = row.ok_or_else(|| invalid("outgoing range contains a gap"))?;
            size += bytes.len() + 4;
            if size > MAX_PAGE_BYTES {
                break;
            }
            let entry = MutationEntry::decode(&bytes).map_err(invalid)?;
            if entry.stamp.position.source_stream != stream
                || entry.stamp.position.sequence != n + 1
            {
                return Err(invalid("outgoing entry identity differs from its address"));
            }
            entries.push(entry);
            covered = n + 1;
        }
        Ok((entries, covered))
    }
}

pub(super) fn entry_key(stream: &StreamId, sequence: u64) -> String {
    format!("{ENTRY_PREFIX}{stream}:{sequence:020}")
}

fn shared(state: &LocalAuthor) -> StorageResult<&SharedAuthor> {
    state
        .shared
        .as_ref()
        .ok_or_else(|| invalid("shared journal is not bound"))
}

/// The definition entry that precedes a schema's first record in a stream.
pub(super) fn define_row(
    stream: &StreamId,
    stamp: Stamp,
    definition: &PortableSchemaDefinition,
) -> StorageResult<EncodedMetadata> {
    let entry = MutationEntry {
        stamp,
        schema: SchemaIdentity::Shared(definition.id().as_str().to_owned()),
        operation: MutationOperation::DefineSchema {
            definition: Box::new(definition.clone()),
        },
    };
    entry.validate().map_err(invalid)?;
    Ok(EncodedMetadata {
        key: entry_key(stream, entry.stamp.position.sequence),
        bytes: entry.encode().map_err(invalid)?,
    })
}

fn same_file_versions(entries: &[MutationEntry], batch: &LogBatch) -> bool {
    let files: BTreeSet<&Position> = entries
        .iter()
        .filter_map(|entry| entry.operation.file_version())
        .collect();
    files
        .iter()
        .map(|position| (position.source_stream.as_str(), position.sequence))
        .eq(batch
            .file_versions
            .iter()
            .map(|file| (file.source_stream.as_str(), file.sequence.get())))
}
