//! Apply the change of an entry to the store, with the stamp of the entry.
//!
//! A record is written when the stamp of the entry is above the stamp of the
//! last write that settled it, and never otherwise. That is the whole-record
//! last-writer rule of the store, and it does not depend on the order in which
//! entries arrive: an older write that arrives after a newer one, or after a
//! delete, changes nothing.
//!
//! The store keeps, for each record, the stamp of its winner (`winner` row),
//! the version it showed before a local write that waits (`baseline` row), and
//! a marker of that waiting write (`pending` row). A local write is pending from
//! its commit until its entry is signed, and this module settles it there. The
//! entry of another device is applied by [`remote`], and the head row of its
//! stream is written in the same transaction as the record.

use std::path::Path;

use fold_db::fold_db_core::mutation_manager::{MutationManager, ReplayStamp, ReplayStreamHead};
use fold_db::schema::types::{FileWrite, Mutation, Schema, SchemaError};
use mutation_log::{EntryHash, EntryHeader, SealOpener};
use sha2::{Digest, Sha256};

use super::body::{Change, RecordChange, RecordFile, SchemaChange};
use super::{files, Ctx};

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

fn failed(what: &str, error: &SchemaError) -> String {
    format!("{what}: {error}")
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
) -> Result<(), String> {
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    match change {
        Change::Record(mut record) => {
            prepare(ctx, &mut record, header, blob)?;
            manager
                .write_replay_at_head(
                    stream,
                    next,
                    apply_record(manager, &stamp, &record.mutation, false),
                )
                .await
                .map(|_| ())
                .map_err(|error| failed("a record of another device could not be applied", &error))
        }
        Change::Schema(schema) => remote_schema(ctx, stream, next, header, *schema).await,
    }
}

async fn remote_schema(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
    schema: SchemaChange,
) -> Result<(), String> {
    let manager = ctx.host.db.mutation_manager();
    match schema.definition {
        Some(definition) => manager
            .write_replay_at_head(stream, next, put_schema(ctx, definition))
            .await
            .map(|_| ())
            .map_err(|error| failed("a schema of another device could not be applied", &error)),
        None => drop_schema(ctx, stream, next, header, &schema.name).await,
    }
}

/// Settle a local write whose entry is signed: its marker is cleared, and a
/// write that lost to a newer one is replaced by the winner.
pub(super) async fn settle(
    ctx: &Ctx<'_>,
    header: &EntryHeader,
    change: Change,
    blob: Option<&Path>,
) -> Result<(), String> {
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    let result = match change {
        Change::Record(mut record) => {
            prepare(ctx, &mut record, header, blob)?;
            manager
                .write_replay(apply_record(manager, &stamp, &record.mutation, true))
                .await
        }
        Change::Schema(schema) => {
            manager
                .write_replay(
                    manager.clear_pending_replay_schema(&schema.name, &schema.mutation_id),
                )
                .await
        }
    };
    result.map_err(|error| failed("a local write could not be settled", &error))
}

/// Put the file bytes of a record write into the mutation. The bytes come from
/// the sealed blob, and they must be the file the body names.
fn prepare(
    ctx: &Ctx<'_>,
    change: &mut RecordChange,
    header: &EntryHeader,
    blob: Option<&Path>,
) -> Result<(), String> {
    const BAD: &str = "the file of a record does not match its entry";
    match &change.file {
        RecordFile::Keep => {}
        RecordFile::Clear => change.mutation.file_write = Some(FileWrite::Clear),
        RecordFile::Put(marker) => {
            let sealed = std::fs::read(blob.ok_or(BAD)?).map_err(|_| BAD.to_owned())?;
            let plain = ctx.sealer.open(header, &sealed).ok_or(BAD)?;
            if !files::is_file_of(&plain, marker) {
                return Err(BAD.into());
            }
            let write = ctx
                .host
                .db
                .db_ops()
                .records()
                .seal_record_file(&plain, marker.name.clone(), marker.media_type.clone())
                .map_err(|_| BAD.to_owned())?;
            if !matches!(&write, FileWrite::Put(sealed) if sealed.marker() == marker) {
                return Err(BAD.into());
            }
            change.mutation.file = Some(marker.clone());
            change.mutation.file_write = Some(write);
        }
    }
    change.mutation.inline_write = change.inline.take();
    Ok(())
}

/// The last-writer rule for one record. `local` is true for a write of this
/// device that has just been signed.
async fn apply_record(
    manager: &MutationManager,
    stamp: &ReplayStamp,
    mutation: &Mutation,
    local: bool,
) -> Result<(), SchemaError> {
    let (schema, key, id) = (
        mutation.schema_name.as_str(),
        &mutation.key_value,
        mutation.uuid.as_str(),
    );
    if !local {
        // Raise the author clock past this entry, so a write made here after it
        // is above it. Only an entry that is applied counts.
        manager.observe_replay_time(stamp.ordering_ms).await?;
    }
    let pending = manager.pending_replay_record(schema, key).await?;
    let winner = manager.replay_record_winner(schema, key).await?;
    let settles = local && pending.as_deref() == Some(id);
    let loses = winner.as_ref().is_some_and(|held| held >= stamp);
    if settles && loses {
        // A newer write won while this one waited. The winner is the record.
        let held = winner.as_ref().ok_or_else(earlier_missing)?;
        if !manager.apply_replay_baseline(schema, key, held).await? {
            return Err(earlier_missing());
        }
    } else if !loses {
        if pending.is_some() {
            // A local write waits. Keep the winner as the version the record
            // returns to if that write loses, and leave the visible record.
            if !manager
                .apply_replay_candidate(mutation.clone(), stamp, settles)
                .await?
            {
                return Err(earlier_missing());
            }
        } else {
            manager
                .apply_replay_record(mutation.clone(), &stamp.writer_key)
                .await?;
        }
        manager.accept_replay_record(schema, key, stamp).await?;
    }
    if settles {
        manager
            .stage_deferred_replay_record(schema, key, None)
            .await?;
        manager.clear_pending_replay_record(schema, key, id).await?;
    }
    Ok(())
}

fn earlier_missing() -> SchemaError {
    SchemaError::Blocked("an earlier version of the record is missing".into())
}

async fn put_schema(ctx: &Ctx<'_>, definition: Schema) -> Result<(), SchemaError> {
    let manager = ctx.host.db.mutation_manager();
    if manager
        .pending_replay_schema(&definition.name)
        .await?
        .is_none()
    {
        ctx.host
            .db
            .schema_manager()
            .load_schema_internal(definition)
            .await
    } else if manager.pending_replay_schema_matches(&definition).await? {
        Ok(())
    } else {
        Err(SchemaError::Blocked(
            "a local schema change awaits upload".into(),
        ))
    }
}

/// A schema drop is a paged removal in several transactions, so the head row
/// cannot join it. The removal keeps a receipt that makes it run once, and the
/// head row follows it. A crash between the two repeats the removal as a no-op.
async fn drop_schema(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
    name: &str,
) -> Result<(), String> {
    let mut identity = Sha256::new();
    identity.update(b"lastdb-personal-schema-drop-v1");
    identity.update(header.node_key.as_bytes());
    identity.update(header.device_id.as_bytes());
    identity.update(header.writer_seq.to_be_bytes());
    let digest = Sha256::digest(next.hash.as_bytes());
    let id: [u8; 32] = identity.finalize().into();
    let digest: [u8; 32] = digest.into();
    let done = async {
        ctx.host
            .db
            .schema_manager()
            .drop_replay_schema(name, &id, &digest)
            .await?;
        ctx.host
            .db
            .mutation_manager()
            .set_replay_stream_head(stream, next)
            .await
    };
    done.await.map_err(|error| {
        failed(
            "a schema drop of another device could not be applied",
            &error,
        )
    })
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
