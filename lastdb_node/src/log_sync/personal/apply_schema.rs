//! Apply remote schema puts and drops with author-clock and winner tracking.
//!
//! A schema has one stamp row: the stamp of its newest put or drop. A put or
//! drop made on this device writes that row when it commits, so an older put of
//! another device changes nothing here, and a newer one replaces the local one.

use fold_db::fold_db_core::mutation_manager::{ReplayStamp, ReplayStreamHead};
use fold_db::schema::types::{Schema, SchemaError};
use mutation_log::EntryHeader;
use sha2::{Digest, Sha256};

use super::apply::{failed, stamp_of};
use super::body::SchemaChange;
use super::Ctx;

pub(super) async fn remote_schema(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
    schema: SchemaChange,
) -> Result<(), String> {
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    match schema.definition {
        Some(definition) => manager
            .write_replay_at_head(stream, next, put_schema(ctx, &stamp, definition))
            .await
            .map(|_| ())
            .map_err(|error| failed("a schema of another device could not be applied", &error)),
        None => drop_schema(ctx, stream, next, header, &schema.name).await,
    }
}

async fn put_schema(
    ctx: &Ctx<'_>,
    stamp: &ReplayStamp,
    definition: Schema,
) -> Result<(), SchemaError> {
    let manager = ctx.host.db.mutation_manager();
    manager.observe_replay_time(stamp.ordering_ms).await?;
    let name = definition.name.clone();
    if manager
        .replay_schema_winner(&name)
        .await?
        .is_some_and(|held| &held >= stamp)
    {
        return Ok(());
    }
    ctx.host
        .db
        .schema_manager()
        .load_schema_internal(definition)
        .await?;
    manager.accept_replay_schema(&name, stamp).await?;
    Ok(())
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
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    let mut identity = Sha256::new();
    identity.update(b"lastdb-personal-schema-drop-v1");
    identity.update(header.node_key.as_bytes());
    identity.update(header.device_id.as_bytes());
    identity.update(header.writer_seq.to_be_bytes());
    let digest = Sha256::digest(next.hash.as_bytes());
    let id: [u8; 32] = identity.finalize().into();
    let digest: [u8; 32] = digest.into();
    let done = async {
        let winner = manager.read_replay_schema_winner(name).await?;
        if winner.as_ref().is_none_or(|held| &stamp > held) {
            ctx.host
                .db
                .schema_manager()
                .drop_replay_schema(name, &id, &digest, &stamp)
                .await?;
        }
        manager
            .write_replay_at_head(stream, next, async {
                manager.observe_replay_time(stamp.ordering_ms).await?;
                manager.accept_replay_drop_schema(name, &stamp).await?;
                manager.accept_replay_schema(name, &stamp).await?;
                Ok(())
            })
            .await
            .map(|_| ())
    };
    done.await.map_err(|error| {
        failed(
            "a schema drop of another device could not be applied",
            &error,
        )
    })
}
