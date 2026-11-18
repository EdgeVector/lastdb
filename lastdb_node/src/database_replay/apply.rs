//! Store mutations and attached file bytes for verified signed entries.

use fold_db::fold_db_core::mutation_manager::{MutationManager, ReplayStamp, ServedLocal};
use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{
    CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Query, SchemaError,
};

use crate::host::ReplayBlockReason;

use super::chain;
use super::payload::{self, FileFetch, RecordFile, SchemaChange};
use super::types::{SnapshotEntry, SnapshotFileRequest};
use super::RestoreError;

pub(super) async fn stage_file(
    store: &FoldDB,
    key: &crate::log_envelope::DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    change: &mut payload::RecordChange,
    files: &mut crate::log_sync::SnapshotFiles,
) -> Result<(), RestoreError> {
    match &change.file {
        RecordFile::Keep => {}
        RecordFile::Clear => change.mutation.file_write = Some(FileWrite::Clear),
        RecordFile::Fetch(fetch) => {
            let write = load_file(store, key, database_id, entry, fetch, files).await?;
            attach_put(&mut change.mutation, write)?;
        }
    }
    change.mutation.inline_write = change.inline.take();
    Ok(())
}

async fn load_file(
    store: &FoldDB,
    key: &crate::log_envelope::DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    fetch: &FileFetch,
    files: &mut crate::log_sync::SnapshotFiles,
) -> Result<FileWrite, RestoreError> {
    let sealed = files
        .read_blob(&SnapshotFileRequest {
            database_id: database_id.to_owned(),
            scope: fetch.object.scope.clone(),
            path: fetch.object.path.clone(),
        })
        .await?;
    let binding = chain::binding(database_id, entry);
    let plain = crate::log_envelope::open_blob(key, &binding, &sealed)
        .map_err(|_| RestoreError::File("snapshot file"))?;
    if !plain_matches(&plain, &fetch.marker) {
        return Err(RestoreError::File("snapshot file"));
    }
    let write = store
        .db_ops()
        .records()
        .seal_record_file(
            &plain,
            fetch.marker.name.clone(),
            fetch.marker.media_type.clone(),
        )
        .map_err(|_| RestoreError::Apply("snapshot file"))?;
    match &write {
        FileWrite::Put(sealed) if sealed.marker() == &fetch.marker => Ok(write),
        _ => Err(RestoreError::File("snapshot file")),
    }
}

fn attach_put(mutation: &mut Mutation, write: FileWrite) -> Result<(), RestoreError> {
    let FileWrite::Put(sealed) = &write else {
        return Err(RestoreError::File("snapshot file"));
    };
    mutation.file = Some(sealed.marker().clone());
    mutation.file_write = Some(write);
    Ok(())
}

fn plain_matches(bytes: &[u8], marker: &FileMarker) -> bool {
    marker.size_bytes == bytes.len() as u64
        && cloud_backup_protocol::bytes_sha256(bytes).as_str() == marker.sha256
}

pub(super) async fn apply_schema(store: &FoldDB, change: SchemaChange) -> Result<(), RestoreError> {
    let schemas = store.schema_manager();
    match change.definition {
        Some(definition) => schemas
            .load_schema_internal(definition)
            .await
            .map_err(|_| RestoreError::Apply("snapshot schema")),
        None => schemas
            .drop_schema(&change.name)
            .await
            .map(|_| ())
            .map_err(|_| RestoreError::Apply("snapshot schema")),
    }
}

/// The log body has no base revision. The action is the precondition.
///
/// A create holds when the key is absent. A replace, delete, or purge holds when
/// a record (or, for purge, its tombstone) is live. The caller then writes that
/// exact expectation, so the storage check is the check just made.
pub(super) async fn served_precondition(
    store: &FoldDB,
    mutation: &mut Mutation,
) -> Result<bool, RestoreError> {
    let live = live_revision(store, mutation).await?;
    let holds = match mutation.mutation_type {
        MutationType::Create => live.is_none(),
        MutationType::Replace | MutationType::Delete | MutationType::Purge => live.is_some(),
    };
    if holds {
        mutation.expected = match live {
            Some(revision) => CasExpectation::Revision { revision },
            None => CasExpectation::Absent,
        };
    }
    Ok(holds)
}

pub(super) async fn apply_served_record(
    store: &FoldDB,
    key: &crate::log_envelope::DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    files: &mut crate::log_sync::SnapshotFiles,
    mut change: payload::RecordChange,
) -> Result<(), RestoreError> {
    // The contribution names its writer when the body has one. Otherwise the entry names its writer.
    let writer = match change.mutation.contribution.as_ref() {
        Some(admission) => admission.writer.clone(),
        None => entry.writer_key.clone(),
    };
    change.mutation.writer = Some(writer);
    let schema = change.mutation.schema_name.clone();
    let record_key = change.mutation.key_value.clone();
    let mutation_id = change.mutation.uuid.clone();
    let local = store
        .mutation_manager()
        .release_local_for_served(&schema, &record_key, &mutation_id)
        .await
        .map_err(|_| RestoreError::Apply("snapshot record"))?;
    if local == ServedLocal::Echo || !served_precondition(store, &mut change.mutation).await? {
        tracing::warn!(
            target: "lastdbd::log_sync",
            %mutation_id,
            "served entry precondition did not hold"
        );
        return Ok(());
    }
    stage_file(store, key, database_id, entry, &mut change, files).await?;
    match store
        .mutation_manager()
        .write_mutations_receipt(vec![change.mutation])
        .await
    {
        Ok(_) => Ok(()),
        Err(SchemaError::CasConflict { .. }) => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %mutation_id,
                "served entry precondition did not hold"
            );
            Ok(())
        }
        Err(SchemaError::InvalidData(message)) if message.contains("alternate") => {
            tracing::warn!(
                target: "lastdbd::log_sync",
                %mutation_id,
                "served entry precondition did not hold"
            );
            Ok(())
        }
        Err(_) => Err(RestoreError::Apply("snapshot record")),
    }
}

async fn live_revision(
    store: &FoldDB,
    mutation: &Mutation,
) -> Result<Option<String>, RestoreError> {
    let query = Query::new_with_filter(
        mutation.schema_name.clone(),
        Some(key_filter(&mutation.key_value)?),
    );
    let records = store
        .query_executor()
        .query(query)
        .await
        .map_err(|_| RestoreError::Apply("snapshot record"))?;
    if records.len() > 1 {
        return Err(RestoreError::Conflict("snapshot record"));
    }
    if let Some(record) = records.into_values().next() {
        return Ok(Some(record.revision));
    }
    if mutation.mutation_type == MutationType::Purge {
        return store
            .applied_delete_revision(&mutation.schema_name, &mutation.key_value)
            .await
            .map_err(|_| RestoreError::Apply("snapshot record"));
    }
    Ok(None)
}

fn key_filter(key: &KeyValue) -> Result<HashRangeFilter, RestoreError> {
    let hash = key
        .hash
        .clone()
        .filter(|hash| !hash.is_empty())
        .ok_or(RestoreError::Entry("snapshot entry"))?;
    match &key.range {
        Some(range) if !range.is_empty() => Ok(HashRangeFilter::HashRangeKey {
            hash,
            range: range.clone(),
        }),
        _ => Ok(HashRangeFilter::HashKey(hash)),
    }
}

pub(super) struct RecordReplay<'a> {
    pub(super) mutation_id: &'a str,
    pub(super) is_local: bool,
    pub(super) schema: String,
    pub(super) key: KeyValue,
    pub(super) stamp: ReplayStamp,
    pub(super) incoming_ready: bool,
    pub(super) prior: Option<(Vec<u8>, ReplayStamp, Mutation)>,
    pub(super) blocked: Option<ReplayBlockReason>,
}

impl RecordReplay<'_> {
    pub(super) async fn attempt(
        &mut self,
        manager: &MutationManager,
        mutation: &Mutation,
        fetch: &mut Option<RecordFetch>,
    ) -> Result<(), SchemaError> {
        let (schema, key) = (&self.schema, &self.key);
        manager.observe_replay_time(self.stamp.ordering_ms).await?;
        if self.is_local
            && manager
                .clear_replay_record_after_schema_drop(schema, self.mutation_id)
                .await?
        {
            return Ok(());
        }
        let pending = manager.pending_replay_record(schema, key).await?;
        let winner = manager.replay_record_winner(schema, key).await?;
        let settles_local = self.is_local && pending.as_deref() == Some(self.mutation_id);
        let loses = winner.as_ref().is_some_and(|winner| winner >= &self.stamp);
        if settles_local && loses {
            let winner = winner.as_ref().ok_or_else(needs_preparation)?;
            self.restore_winner(manager, winner, fetch).await?;
        } else if !loses {
            // Decide the winner before fetching a file. A losing entry needs only
            // its receipt; a winning attachment must be durable before that receipt.
            if !self.incoming_ready {
                *fetch = Some(RecordFetch::Incoming);
                return Err(needs_preparation());
            }
            if pending.is_some() {
                if !manager
                    .apply_replay_candidate(mutation.clone(), &self.stamp, settles_local)
                    .await?
                {
                    return Err(self.block(ReplayBlockReason::MissingEarlierRecord));
                }
            } else {
                // Old queue entries can have no marker. Apply the accepted value
                // even for this writer, since a peer may already have changed it.
                manager
                    .apply_replay_record(mutation.clone(), &self.stamp.writer_key)
                    .await?;
            }
            manager
                .accept_replay_record(schema, key, &self.stamp)
                .await?;
        }
        if settles_local {
            manager
                .stage_deferred_replay_record(&self.schema, &self.key, None)
                .await?;
            manager
                .clear_pending_replay_record(&self.schema, &self.key, self.mutation_id)
                .await?;
        }
        Ok(())
    }

    async fn restore_winner(
        &mut self,
        manager: &MutationManager,
        winner: &ReplayStamp,
        fetch: &mut Option<RecordFetch>,
    ) -> Result<(), SchemaError> {
        let (schema, key) = (&self.schema, &self.key);
        if manager.apply_replay_baseline(schema, key, winner).await? {
            return Ok(());
        }
        // Upgrade an older client's deferred text entry on demand. New candidates
        // already carry a complete native winner, including verified file bytes.
        if let Some(saved) = manager.deferred_replay_record(schema, key).await? {
            match self.prior.take() {
                Some((loaded, stamp, mutation)) if loaded == saved && stamp == *winner => {
                    if manager
                        .apply_replay_candidate(mutation, winner, true)
                        .await?
                    {
                        return Ok(());
                    }
                }
                Some((loaded, _, _)) if loaded == saved => {
                    return Err(self.block(ReplayBlockReason::MissingEarlierRecord));
                }
                _ => {
                    *fetch = Some(RecordFetch::Prior(saved, winner.clone()));
                    return Err(needs_preparation());
                }
            }
        }
        Err(self.block(ReplayBlockReason::MissingEarlierRecord))
    }

    fn block(&mut self, reason: ReplayBlockReason) -> SchemaError {
        self.blocked = Some(reason);
        SchemaError::Blocked(reason.message().into())
    }
}

pub(super) enum RecordFetch {
    Incoming,
    Prior(Vec<u8>, ReplayStamp),
}

fn needs_preparation() -> SchemaError {
    SchemaError::InvalidData("owned replay requires materialization".into())
}
