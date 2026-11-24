//! Apply the change of an entry of another device to the store, with the stamp of the entry.
//!
//! A record has three parts: the fields, the file and the inline file. The
//! store keeps the stamp of the newest write of each part beside the record.
//! An entry replaces the fields when its stamp is above the stamp of the fields,
//! and a part it sets or clears when its stamp is above the stamp of that part.
//! A delete is a write of all three. That rule does not depend on the order in
//! which entries arrive: an older write that arrives after a newer one, or after
//! a delete, changes nothing it is older than.
//!
//! A write made on this device gets its stamps in the transaction that commits
//! it, so this module handles only the entries of other devices. Each is applied
//! by [`remote`], and the head row of its stream is written in the same
//! transaction as the record.

use std::path::Path;

use fold_db::fold_db_core::mutation_manager::{
    ReplayAttachmentSlots, ReplayStamp, ReplayStreamHead,
};
use fold_db::schema::types::{FileWrite, Mutation, SchemaError};
use mutation_log::{EntryHash, EntryHeader, SealOpener};

use super::body::{Change, RecordChange, RecordFile};
use super::{apply_schema, files, Ctx};

/// The stamp an entry gives its record: the order value, then the node key, the
/// device id and the sequence number. This is the order of the store's stamp row.
pub(super) fn stamp_of(header: &EntryHeader) -> ReplayStamp {
    ReplayStamp {
        ordering_ms: header.lamport,
        writer_key: header.node_key.to_hex(),
        writer_device: header.device_id.to_hex(),
        writer_seq: header.writer_seq,
    }
}

pub(super) fn failed(what: &str, error: &SchemaError) -> String {
    format!("{what}: {error}")
}

/// True when a file-carrying remote record entry is already completely superseded
/// and its blob does not need to be downloaded from the relay.
pub(super) async fn superseded(
    ctx: &Ctx<'_>,
    header: &EntryHeader,
    change: &Change,
) -> Result<bool, String> {
    let Change::Record(record) = change else {
        return Ok(false);
    };
    if !matches!(record.file, RecordFile::Put(_)) {
        return Ok(false);
    }
    let stamp = stamp_of(header);
    let slots = ReplayAttachmentSlots {
        file: !matches!(record.file, RecordFile::Keep),
        inline: record.inline.is_some(),
    };
    ctx.host
        .db
        .mutation_manager()
        .is_replay_record_superseded(
            &record.mutation.schema_name,
            &record.mutation.key_value,
            &stamp,
            slots,
        )
        .await
        .map_err(|error| failed("a record of another device could not be checked", &error))
}

/// Advance a remote stream head and replay clock for an already-superseded entry.
pub(super) async fn skip_remote(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
) -> Result<(), String> {
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    manager
        .write_replay_at_head(stream, next, manager.observe_replay_time(stamp.ordering_ms))
        .await
        .map(|_| ())
        .map_err(|error| failed("a record of another device could not be applied", &error))
}

/// What [`remote`] found when applying a remote entry.
pub(super) enum ApplyOutcome {
    Applied,
    InvalidEntry(&'static str),
}

/// Apply an entry of another device. The record write, its winner row and the
/// head row of the stream are one transaction, so a restart replays nothing.
pub(super) async fn remote(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
    change: Change,
    blob: Option<&Path>,
) -> Result<ApplyOutcome, String> {
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    match change {
        Change::Record(mut record) => {
            if let Some(reason) = prepare(ctx, &mut record, header, blob)? {
                return Ok(ApplyOutcome::InvalidEntry(reason));
            }
            manager
                .write_replay_at_head(stream, next, apply_record(ctx, &stamp, record.mutation))
                .await
                .map(|_| ApplyOutcome::Applied)
                .map_err(|error| failed("a record of another device could not be applied", &error))
        }
        Change::Schema(schema) => apply_schema::remote_schema(ctx, stream, next, header, *schema)
            .await
            .map(|()| ApplyOutcome::Applied),
    }
}

/// Put the file bytes of a record write into the mutation. Returns an invalid-entry
/// reason when the sealed blob does not open or does not match the signed marker;
/// returns `Err` only for local disk or store failures.
fn prepare(
    ctx: &Ctx<'_>,
    change: &mut RecordChange,
    header: &EntryHeader,
    blob: Option<&Path>,
) -> Result<Option<&'static str>, String> {
    match &change.file {
        RecordFile::Keep => {}
        RecordFile::Clear => change.mutation.file_write = Some(FileWrite::Clear),
        RecordFile::Put(marker) => {
            let path = blob.ok_or_else(|| "a fetched record file is missing on disk".to_owned())?;
            let sealed = std::fs::read(path)
                .map_err(|error| format!("cannot read a fetched record file: {error}"))?;
            let Some(plain) = ctx.sealer.open(header, &sealed) else {
                return Ok(Some("a record file blob does not open"));
            };
            if !files::is_file_of(&plain, marker) {
                return Ok(Some("the file of a record does not match its entry"));
            }
            let write = ctx
                .host
                .db
                .db_ops()
                .records()
                .seal_record_file(&plain, marker.name.clone(), marker.media_type.clone())
                .map_err(|error| format!("a record file could not be sealed: {error}"))?;
            if !matches!(&write, FileWrite::Put(sealed) if sealed.marker() == marker) {
                return Ok(Some("the file marker of a record is not canonical"));
            }
            change.mutation.file = Some(marker.clone());
            change.mutation.file_write = Some(write);
        }
    }
    change.mutation.inline_write = change.inline.take();
    Ok(None)
}

/// The last-writer rule for one record entry of another device.
async fn apply_record(
    ctx: &Ctx<'_>,
    stamp: &ReplayStamp,
    mutation: Mutation,
) -> Result<(), SchemaError> {
    let manager = ctx.host.db.mutation_manager();
    // Raise the author clock past this entry, so a write made here after it is
    // above it.
    manager.observe_replay_time(stamp.ordering_ms).await?;
    if !schema_holds(ctx, stamp, &mutation).await? {
        return Ok(());
    }
    manager.merge_replay_record(mutation, stamp).await
}

/// True when the schema of a record entry is installed. False when a newer
/// schema drop removed it, so the entry changes nothing.
async fn schema_holds(
    ctx: &Ctx<'_>,
    stamp: &ReplayStamp,
    mutation: &Mutation,
) -> Result<bool, SchemaError> {
    if ctx
        .host
        .db
        .schema_manager()
        .resolve_schema(&mutation.schema_name)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    let dropped = ctx
        .host
        .db
        .mutation_manager()
        .replay_schema_dropped_after(&mutation.schema_name, stamp)
        .await?;
    if dropped {
        Ok(false)
    } else {
        Err(SchemaError::InvalidData(format!(
            "Schema '{}' not found",
            mutation.schema_name
        )))
    }
}

/// The hash of an entry as the head row names it.
pub(super) fn head_of(header: &EntryHeader, hash: &EntryHash) -> ReplayStreamHead {
    ReplayStreamHead {
        seq: header.writer_seq,
        hash: hash.to_hex(),
        order: header.lamport,
        frozen: false,
    }
}
