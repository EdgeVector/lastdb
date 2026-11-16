//! Store mutations and attached file bytes for verified signed entries.

use fold_db::fold_db_core::FoldDB;
use fold_db::record_value::FileMarker;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{
    CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Query, SchemaError,
};

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

pub(super) async fn bind_expected(
    store: &FoldDB,
    mutation: &mut Mutation,
    resumed: bool,
) -> Result<bool, RestoreError> {
    if mutation.mutation_type == MutationType::Create {
        mutation.expected = CasExpectation::Absent;
        return Ok(true);
    }
    match live_revision(store, mutation).await? {
        Some(revision) => {
            mutation.expected = CasExpectation::Revision { revision };
            Ok(true)
        }
        None if resumed
            && matches!(
                mutation.mutation_type,
                MutationType::Delete | MutationType::Purge
            ) =>
        {
            Ok(false)
        }
        None => Err(RestoreError::Conflict("snapshot record")),
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

pub(super) async fn write_record(
    store: &FoldDB,
    mutation: Mutation,
    resumed: bool,
) -> Result<(), RestoreError> {
    match store
        .mutation_manager()
        .write_mutations_receipt(vec![mutation])
        .await
    {
        Ok(_) => Ok(()),
        // A resumed create can conflict because the first attempt landed and
        // the author clock changed. The caller clears only a first attempt.
        Err(SchemaError::CasConflict { .. }) if resumed => Ok(()),
        Err(SchemaError::CasConflict { .. }) => Err(RestoreError::Conflict("snapshot record")),
        Err(_) => Err(RestoreError::Apply("snapshot record")),
    }
}
