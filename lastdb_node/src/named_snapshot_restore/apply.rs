//! Apply oldest snapshot first. The journal attempt is durable before the write.
//!
//! Schema put and drop use the open store's schema manager. A record replace,
//! delete, or purge uses the revision already stored by the preceding entry.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::{
    CasExpectation, FileWrite, KeyValue, Mutation, MutationType, Query, SchemaError,
};

use super::chain::Link;
use super::decode;
use super::journal::{self, Journal};
use super::payload::{self, RecordFile, SchemaChange};
use super::types::SnapshotEntry;
use super::RestoreError;

pub(super) async fn replay(
    store: &FoldDB,
    key: &crate::log_envelope::DatabaseContentKey,
    home: &Path,
    database_id: &str,
    chain: &[Link],
    journal: &mut Journal,
    files: &mut (dyn super::SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    // The accepted body does not name a sealed-row blob. E still passes this source.
    let _ = files;
    while journal.applied_refs.len() < chain.len() {
        let index = journal.applied_refs.len();
        let link = &chain[index];
        pin_object(home, journal, link)?;
        if !object_closed(journal, link) {
            drive_object(store, key, home, database_id, link, journal).await?;
        }
        journal
            .applied_refs
            .push(link.manifest.snapshot_ref.clone());
        journal.next_part = 0;
        journal.next_entry = 0;
        journal.attempt = None;
        journal.current_ref = None;
        journal::save(home, journal)?;
    }
    Ok(())
}

async fn drive_object(
    store: &FoldDB,
    key: &crate::log_envelope::DatabaseContentKey,
    home: &Path,
    database_id: &str,
    link: &Link,
    journal: &mut Journal,
) -> Result<(), RestoreError> {
    loop {
        if object_closed(journal, link) {
            return Ok(());
        }
        let part = journal.next_part as usize;
        let entries = &link.parts[part].entries;
        if entries.is_empty() {
            bump_part(journal);
            if !object_closed(journal, link) {
                journal::save(home, journal)?;
            }
            continue;
        }
        let entry = journal.next_entry as usize;
        apply_one(store, key, home, database_id, journal, &entries[entry]).await?;
        journal.next_entry += 1;
        journal.attempt = None;
        if journal.next_entry as usize == entries.len() {
            bump_part(journal);
        }
        journal::save(home, journal)?;
    }
}

async fn apply_one(
    store: &FoldDB,
    key: &crate::log_envelope::DatabaseContentKey,
    home: &Path,
    database_id: &str,
    journal: &mut Journal,
    entry: &SnapshotEntry,
) -> Result<(), RestoreError> {
    let prepared = decode::decode_entry(key, database_id, entry)?;
    let hash = prepared.hash.clone();
    if let journal::Marker::Applied = journal::read_marker(home, database_id, &prepared.id, &hash)?
    {
        return Ok(());
    }
    let resumed = resumed(journal, &hash)?;
    if !resumed {
        journal.attempt = Some(journal::Attempt {
            part: journal.next_part,
            entry: journal.next_entry,
            payload_sha256: hash.clone(),
        });
        journal::save(home, journal)?;
    }
    match prepared.change {
        payload::Change::Schema(change) => apply_schema(store, change).await?,
        payload::Change::Record(mut change) => {
            attach(&mut change.mutation, change.file, change.inline);
            if bind_expected(store, &mut change.mutation, resumed).await? {
                write_record(store, home, journal, change.mutation, resumed).await?;
            }
        }
    }
    journal::write_marker(home, database_id, &prepared.id, &hash)
}

async fn apply_schema(store: &FoldDB, change: SchemaChange) -> Result<(), RestoreError> {
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

fn attach(
    mutation: &mut Mutation,
    file: RecordFile,
    inline: Option<fold_db::record_value::InlineWrite>,
) {
    match file {
        RecordFile::Keep => {}
        RecordFile::Clear => mutation.file_write = Some(FileWrite::Clear),
        RecordFile::Marker(marker) => mutation.file = Some(marker),
    }
    mutation.inline_write = inline;
}

async fn bind_expected(
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
    let mut query = Query::new_with_filter(
        mutation.schema_name.clone(),
        Some(key_filter(&mutation.key_value)?),
    );
    if mutation.mutation_type == MutationType::Purge {
        query.include_tombstones = true;
    }
    let records = store
        .query_executor()
        .query(query)
        .await
        .map_err(|_| RestoreError::Apply("snapshot record"))?;
    if records.len() > 1 {
        return Err(RestoreError::Conflict("snapshot record"));
    }
    Ok(records.into_values().next().map(|record| record.revision))
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

async fn write_record(
    store: &FoldDB,
    home: &Path,
    journal: &mut Journal,
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
        // the author clock changed. Clear a first-attempt conflict so a later
        // retry does not treat that conflict as success.
        Err(SchemaError::CasConflict { .. }) if resumed => Ok(()),
        Err(SchemaError::CasConflict { .. }) => {
            journal.attempt = None;
            journal::save(home, journal)?;
            Err(RestoreError::Conflict("snapshot record"))
        }
        Err(_) => Err(RestoreError::Apply("snapshot record")),
    }
}

fn resumed(journal: &Journal, hash: &str) -> Result<bool, RestoreError> {
    match &journal.attempt {
        None => Ok(false),
        Some(attempt)
            if attempt.part == journal.next_part
                && attempt.entry == journal.next_entry
                && attempt.payload_sha256 == hash =>
        {
            Ok(true)
        }
        Some(_) => Err(RestoreError::Conflict("snapshot journal")),
    }
}

fn pin_object(home: &Path, journal: &mut Journal, link: &Link) -> Result<(), RestoreError> {
    let refer = link.manifest.snapshot_ref.as_str();
    if at_boundary(journal) {
        if journal.current_ref.as_deref() == Some(refer) {
            return Ok(());
        }
        if journal.current_ref.is_some() {
            return Err(RestoreError::Conflict("snapshot journal"));
        }
        journal.current_ref = Some(refer.to_owned());
        return journal::save(home, journal);
    }
    if journal.current_ref.as_deref() == Some(refer) {
        Ok(())
    } else {
        Err(RestoreError::Conflict("snapshot journal"))
    }
}

fn at_boundary(journal: &Journal) -> bool {
    journal.next_part == 0 && journal.next_entry == 0 && journal.attempt.is_none()
}

fn object_closed(journal: &Journal, link: &Link) -> bool {
    journal.next_part as usize == link.parts.len()
        && journal.next_entry == 0
        && journal.attempt.is_none()
}

fn bump_part(journal: &mut Journal) {
    journal.next_part += 1;
    journal.next_entry = 0;
    journal.attempt = None;
}
