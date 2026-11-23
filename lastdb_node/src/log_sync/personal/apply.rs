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

use fold_db::fold_db_core::mutation_manager::{
    ReplayAttachmentSlots, ReplayStamp, ReplayStreamHead,
};
use fold_db::schema::types::{FileWrite, Mutation, MutationType, SchemaError};
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
                    apply_record(ctx, &stamp, record.mutation, false),
                )
                .await
                .map(|_| ())
                .map_err(|error| failed("a record of another device could not be applied", &error))
        }
        Change::Schema(schema) => {
            apply_schema::remote_schema(ctx, stream, next, header, *schema).await
        }
    }
}

/// A local write whose entry is signed, ready to settle: its stamp and its
/// change, with the bytes of a file already read.
pub(super) struct Local {
    stamp: ReplayStamp,
    change: Change,
}

/// Prepare the change of a signed local write for [`settle_all`].
pub(super) fn local(
    ctx: &Ctx<'_>,
    header: &EntryHeader,
    mut change: Change,
    blob: Option<&Path>,
) -> Result<Local, String> {
    if let Change::Record(record) = &mut change {
        prepare(ctx, record, header, blob)?;
    }
    Ok(Local {
        stamp: stamp_of(header),
        change,
    })
}

/// Settle the local writes of a signed batch in one transaction: the marker of
/// each is cleared, and a write that lost to a newer one is replaced by the
/// winner. It repeats safely, so a batch that is settled again after a crash
/// changes nothing.
pub(super) async fn settle_all(ctx: &Ctx<'_>, items: Vec<Local>) -> Result<(), String> {
    let manager = ctx.host.db.mutation_manager();
    let settled = manager.write_replay(async {
        for item in items {
            match item.change {
                Change::Record(record) => {
                    apply_record(ctx, &item.stamp, record.mutation, true).await?;
                }
                Change::Schema(schema) => {
                    manager
                        .accept_replay_schema(&schema.name, &item.stamp)
                        .await?;
                    manager
                        .clear_pending_replay_schema(&schema.name, &schema.mutation_id)
                        .await?;
                }
            }
        }
        Ok::<(), SchemaError>(())
    });
    settled
        .await
        .map_err(|error| failed("a local write could not be settled", &error))
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
    ctx: &Ctx<'_>,
    stamp: &ReplayStamp,
    mutation: Mutation,
    local: bool,
) -> Result<(), SchemaError> {
    let manager = ctx.host.db.mutation_manager();
    if !local {
        // Raise the author clock past this entry, so a write made here after it
        // is above it. Only an entry that is applied counts.
        manager.observe_replay_time(stamp.ordering_ms).await?;
    }
    if !ensure_schema_or_dropped(ctx, stamp, &mutation, local).await? {
        return Ok(());
    }
    let (schema, key, id) = (
        mutation.schema_name.clone(),
        mutation.key_value.clone(),
        mutation.uuid.clone(),
    );
    let slots = mutation_slots(&mutation);
    let pending = manager.pending_replay_record(&schema, &key).await?;
    let winner = manager.replay_record_winner(&schema, &key).await?;
    let settles = local && pending.as_deref() == Some(id.as_str());
    let loses = winner.as_ref().is_some_and(|held| held >= stamp);
    if settles && loses {
        // A newer write won while this one waited. The winner is the record.
        let held = winner.as_ref().ok_or_else(earlier_missing)?;
        if !manager.apply_replay_baseline(&schema, &key, held).await? {
            return Err(earlier_missing());
        }
    } else if !loses {
        if pending.is_some() {
            // A local write waits. Keep the winner as the version the record
            // returns to if that write loses, and leave the visible record.
            if !manager
                .apply_replay_candidate(mutation, stamp, settles)
                .await?
            {
                return Err(earlier_missing());
            }
        } else {
            manager
                .apply_replay_record(mutation, &stamp.writer_key)
                .await?;
        }
        manager.accept_replay_record(&schema, &key, stamp).await?;
        manager
            .accept_replay_record_attachments(&schema, &key, stamp, slots)
            .await?;
        return clear_settled(manager, &schema, &key, &id, settles).await;
    }
    clear_settled(manager, &schema, &key, &id, settles).await?;
    if loses && (pending.is_none() || settles) {
        manager
            .apply_losing_replay_attachments(mutation, stamp)
            .await?;
    }
    Ok(())
}

/// Returns `Ok(true)` when the schema is installed, `Ok(false)` when a newer
/// schema drop already superseded this record entry.
async fn ensure_schema_or_dropped(
    ctx: &Ctx<'_>,
    stamp: &ReplayStamp,
    mutation: &Mutation,
    local: bool,
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
    let manager = ctx.host.db.mutation_manager();
    let dropped = if local {
        manager
            .clear_replay_record_after_schema_drop(&mutation.schema_name, &mutation.uuid, stamp)
            .await?
    } else {
        manager
            .replay_schema_dropped_after(&mutation.schema_name, stamp)
            .await?
    };
    dropped.then_some(false).ok_or_else(|| {
        SchemaError::InvalidData(format!("Schema '{}' not found", mutation.schema_name))
    })
}

fn mutation_slots(mutation: &Mutation) -> ReplayAttachmentSlots {
    let resets = matches!(
        mutation.mutation_type,
        MutationType::Create | MutationType::Delete | MutationType::Purge
    );
    ReplayAttachmentSlots {
        file: resets || mutation.file_write.is_some(),
        inline: resets || mutation.inline_write.is_some(),
    }
}

async fn clear_settled(
    manager: &fold_db::fold_db_core::mutation_manager::MutationManager,
    schema: &str,
    key: &fold_db::schema::types::KeyValue,
    id: &str,
    settles: bool,
) -> Result<(), SchemaError> {
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

/// The hash of an entry as the head row names it.
pub(super) fn head_of(header: &EntryHeader, hash: &EntryHash) -> ReplayStreamHead {
    ReplayStreamHead {
        seq: header.writer_seq,
        hash: hash.to_hex(),
        order: header.lamport,
        frozen: false,
    }
}
