//! Apply oldest snapshot first. The journal attempt is durable before the write.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::{FileWrite, Mutation, SchemaError};

use crate::log_envelope::DatabaseContentKey;

use super::chain::Link;
use super::decode::{self, FileSource, PreparedFile};
use super::journal::{self, Journal};
use super::types::{SnapshotEntry, SnapshotFileSource};
use super::RestoreError;

pub(super) async fn replay(
    store: &FoldDB,
    key: &DatabaseContentKey,
    home: &Path,
    database_id: &str,
    chain: &[Link],
    journal: &mut Journal,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    while journal.applied_refs.len() < chain.len() {
        let index = journal.applied_refs.len();
        let link = &chain[index];
        pin_object(home, journal, link)?;
        if !object_closed(journal, link) {
            drive_object(store, key, home, database_id, link, journal, files).await?;
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
    key: &DatabaseContentKey,
    home: &Path,
    database_id: &str,
    link: &Link,
    journal: &mut Journal,
    files: &mut (dyn SnapshotFileSource + Send),
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
        apply_one(
            store,
            key,
            home,
            database_id,
            journal,
            &entries[entry],
            files,
        )
        .await?;
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
    key: &DatabaseContentKey,
    home: &Path,
    database_id: &str,
    journal: &mut Journal,
    entry: &SnapshotEntry,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    let mut prepared = decode::decode_entry(key, database_id, entry)?;
    let hash = prepared.hash.clone();
    let mutation_id = prepared.mutation.uuid.clone();
    if let journal::Marker::Applied = journal::read_marker(home, database_id, &mutation_id, &hash)?
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
    if let Some(file) = prepared.file.take() {
        seal_into(
            store,
            key,
            database_id,
            entry,
            &mut prepared.mutation,
            file,
            files,
        )
        .await?;
    }
    write_record(store, home, journal, prepared.mutation, resumed).await?;
    journal::write_marker(home, database_id, &mutation_id, &hash)
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

async fn seal_into(
    store: &FoldDB,
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    mutation: &mut Mutation,
    file: PreparedFile,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    let PreparedFile {
        name,
        media_type,
        size_bytes,
        sha256,
        source,
    } = file;
    let plain = file_bytes(key, database_id, entry, source, size_bytes, &sha256, files).await?;
    let sealed = store
        .db_ops()
        .records()
        .seal_record_file(&plain, name, media_type)
        .map_err(|_| RestoreError::File("file was not sealed"))?;
    let owned = sealed_marker(&sealed, &sha256, size_bytes)?;
    mutation.file = Some(owned);
    mutation.file_write = Some(sealed);
    Ok(())
}

fn sealed_marker(
    sealed: &FileWrite,
    sha256: &str,
    size_bytes: u64,
) -> Result<fold_db::record_value::FileMarker, RestoreError> {
    let FileWrite::Put(record) = sealed else {
        return Err(RestoreError::File("file was not sealed"));
    };
    let marker = record.marker();
    if marker.sha256 != sha256 || marker.size_bytes != size_bytes {
        return Err(RestoreError::File("file was not sealed"));
    }
    Ok(marker.clone())
}

async fn file_bytes(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    source: FileSource,
    size_bytes: u64,
    sha256: &str,
    files: &mut (dyn SnapshotFileSource + Send),
) -> Result<Vec<u8>, RestoreError> {
    let plain = match source {
        FileSource::Inline(bytes) => bytes,
        FileSource::Object(request) => {
            let sealed = files.read_blob(&request).await?;
            decode::open_sealed(
                key,
                database_id,
                entry,
                &sealed,
                RestoreError::File("snapshot file"),
            )?
        }
    };
    if decode::plain_matches(&plain, size_bytes, sha256) {
        Ok(plain)
    } else {
        Err(RestoreError::File("snapshot file"))
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
