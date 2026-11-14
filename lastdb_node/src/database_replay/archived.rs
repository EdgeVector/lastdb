//! Replay an archived signed snapshot with a durable per-entry journal.

use std::path::Path;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::Mutation;

use super::apply::{apply_schema, bind_expected, stage_file, write_record};
use super::chain::Link;
use super::decode;
use super::journal::{self, Journal};
use super::payload;
use super::{RestoreError, SnapshotEntry};

pub(super) struct SnapshotApplier<'a> {
    pub store: &'a FoldDB,
    pub decoder: decode::EntryDecoder<'a>,
    pub home: &'a Path,
    pub files: &'a mut (dyn super::SnapshotFileSource + Send),
    pub scope: SnapshotReplay<'a>,
}

pub(super) enum SnapshotReplay<'a> {
    PrivateOwner {
        key: &'a str,
        local_device: Option<&'a str>,
    },
    LocalPublisher {
        key: &'a str,
        device: &'a str,
    },
    Imported,
}

impl SnapshotApplier<'_> {
    pub(super) async fn replay(
        &mut self,
        chain: &[Link],
        journal: &mut Journal,
    ) -> Result<(), RestoreError> {
        while journal.applied_refs.len() < chain.len() {
            let index = journal.applied_refs.len();
            let link = &chain[index];
            pin_object(self.home, journal, link)?;
            if !object_closed(journal, link) {
                self.drive_object(link, journal).await?;
            }
            journal
                .applied_refs
                .push(link.manifest.snapshot_ref.clone());
            journal.next_part = 0;
            journal.next_entry = 0;
            journal.attempt = None;
            journal.current_ref = None;
            journal::save(self.home, journal)?;
        }
        Ok(())
    }

    async fn drive_object(
        &mut self,
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
                    journal::save(self.home, journal)?;
                }
                continue;
            }
            let entry = journal.next_entry as usize;
            let entry = &entries[entry];
            match &self.scope {
                SnapshotReplay::PrivateOwner { key, local_device } => {
                    super::owned::OwnerReplay {
                        store: self.store,
                        decoder: self.decoder,
                        owner: key,
                        local_device: *local_device,
                        files: self.files,
                    }
                    .one(entry)
                    .await?;
                }
                SnapshotReplay::LocalPublisher { key, device } => {
                    // The live publisher writes its own edits before uploading them.
                    // Its regular log path holds at entries from another writer.
                    if entry.writer_key != *key || entry.writer_device != *device {
                        return Err(RestoreError::Scope("local publisher snapshot entry"));
                    }
                }
                SnapshotReplay::Imported => {
                    apply_one(
                        self.store,
                        self.decoder,
                        self.home,
                        journal,
                        entry,
                        self.files,
                    )
                    .await?;
                }
            }
            journal.next_entry += 1;
            journal.attempt = None;
            if journal.next_entry as usize == entries.len() {
                bump_part(journal);
            }
            journal::save(self.home, journal)?;
        }
    }
}

async fn apply_one(
    store: &FoldDB,
    decoder: decode::EntryDecoder<'_>,
    home: &Path,
    journal: &mut Journal,
    entry: &SnapshotEntry,
    files: &mut (dyn super::SnapshotFileSource + Send),
) -> Result<(), RestoreError> {
    let database_id = decoder.database_id;
    let prepared = decoder.open(entry)?;
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
        payload::Change::Schema(change) => apply_schema(store, *change).await?,
        payload::Change::Record(mut change) => {
            stage_file(
                store,
                decoder.key,
                database_id,
                entry,
                change.as_mut(),
                files,
            )
            .await?;
            if bind_expected(store, &mut change.mutation, resumed).await? {
                record_outcome(store, home, journal, change.mutation, resumed).await?;
            }
        }
    }
    journal::write_marker(home, database_id, &prepared.id, &hash)
}

async fn record_outcome(
    store: &FoldDB,
    home: &Path,
    journal: &mut Journal,
    mutation: Mutation,
    resumed: bool,
) -> Result<(), RestoreError> {
    match write_record(store, mutation, resumed).await {
        Ok(()) => Ok(()),
        Err(RestoreError::Conflict(_)) if !resumed => {
            journal.attempt = None;
            journal::save(home, journal)?;
            Err(RestoreError::Conflict("snapshot record"))
        }
        Err(error) => Err(error),
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
