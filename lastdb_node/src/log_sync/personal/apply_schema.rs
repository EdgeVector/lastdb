//! Apply remote schema puts and drops with author-clock and winner tracking.
//!
//! A schema has a winner row for its newest put or drop and a `drop-winner` row
//! for its newest drop, incompatible replacement, or post-drop recreation.
//! Every schema put or drop is ordered by `ReplayStamp`: compatible additive
//! evolutions preserve existing records, while incompatible definitions resolve
//! to the newest stamp and replace the losing schema's rows idempotently so no
//! conflict can stall the stream checkpoint.

use fold_db::fold_db_core::mutation_manager::{ReplayStamp, ReplayStreamHead};
use fold_db::schema::types::{Schema, SchemaError};
use fold_db::schema::SchemaCore;
use mutation_log::EntryHeader;
use sha2::{Digest, Sha256};

use super::apply::{failed, stamp_of, ApplyOutcome};
use super::body::SchemaChange;
use super::Ctx;

enum SchemaPlan {
    NoOp,
    FenceLoser(ReplayStamp),
    Load { fence_drop: bool },
    ReplaceIncompatible,
}

pub(super) async fn remote_schema(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
    schema: SchemaChange,
) -> Result<ApplyOutcome, String> {
    ctx.host
        .db
        .schema_manager()
        .ensure_unblocked(&schema.name)
        .await
        .map_err(|error| failed("a schema of another device could not be applied", &error))?;
    if let Some(definition) = schema.definition {
        put_schema(ctx, stream, next, header, definition).await
    } else {
        drop_schema(ctx, stream, next, header, &schema.name)
            .await
            .map(|()| ApplyOutcome::Applied)
    }
}

async fn put_schema(
    ctx: &Ctx<'_>,
    stream: &str,
    next: &ReplayStreamHead,
    header: &EntryHeader,
    mut definition: Schema,
) -> Result<ApplyOutcome, String> {
    if SchemaCore::prepare_replay_schema(&mut definition).is_err() {
        return Ok(ApplyOutcome::InvalidEntry(
            "a schema entry is not a valid owned schema",
        ));
    }
    let stamp = stamp_of(header);
    let name = definition.name.clone();
    let (replace_id, replace_digest) =
        schema_receipt(b"lastdb-personal-schema-replace-v1", header, next);
    for attempt in 0..2 {
        ctx.host
            .db
            .schema_manager()
            .ensure_unblocked(&name)
            .await
            .map_err(|error| failed("a schema of another device could not be applied", &error))?;
        let plan = plan_put(
            ctx,
            &name,
            &definition,
            &stamp,
            (&replace_id, &replace_digest),
        )
        .await
        .map_err(|error| failed("a schema of another device could not be applied", &error))?;
        let mut reconciled_floor = None;
        match &plan {
            SchemaPlan::ReplaceIncompatible => {
                ctx.host
                    .db
                    .schema_manager()
                    .drop_replay_schema(&name, &replace_id, &replace_digest, &stamp)
                    .await
                    .map_err(|error| {
                        failed("a schema of another device could not be applied", &error)
                    })?;
            }
            SchemaPlan::FenceLoser(winner) => {
                ctx.host
                    .db
                    .mutation_manager()
                    .reconcile_replay_schema_floor(&name, winner)
                    .await
                    .map_err(|error| {
                        failed("a schema of another device could not be applied", &error)
                    })?;
                reconciled_floor = Some(winner.clone());
            }
            SchemaPlan::NoOp | SchemaPlan::Load { .. } => {}
        }
        let outcome = ctx
            .host
            .db
            .mutation_manager()
            .write_replay_at_head(
                stream,
                next,
                commit_put(
                    ctx,
                    &name,
                    definition.clone(),
                    &stamp,
                    (&replace_id, &replace_digest),
                    reconciled_floor.as_ref(),
                ),
            )
            .await;
        match outcome {
            Ok(_) => return Ok(ApplyOutcome::Applied),
            Err(SchemaError::Blocked(_)) if attempt == 0 => {}
            Err(error) => {
                return Err(failed(
                    "a schema of another device could not be applied",
                    &error,
                ));
            }
        }
    }
    Err("a schema of another device changed concurrently during replay".into())
}

async fn plan_put(
    ctx: &Ctx<'_>,
    name: &str,
    definition: &Schema,
    stamp: &ReplayStamp,
    replace_receipt: (&[u8; 32], &[u8; 32]),
) -> Result<SchemaPlan, SchemaError> {
    let schema_manager = ctx.host.db.schema_manager();
    let _ = schema_manager.resolve_schema(name).await?;
    let replaced_here = schema_manager
        .replay_receipt_exists(replace_receipt.0, replace_receipt.1)
        .await?;
    ctx.host
        .db
        .mutation_manager()
        .in_replay_read_scope(async {
            classify_in_scope(ctx, name, definition, stamp, replaced_here).await
        })
        .await
}

async fn classify_in_scope(
    ctx: &Ctx<'_>,
    name: &str,
    definition: &Schema,
    stamp: &ReplayStamp,
    replaced_here: bool,
) -> Result<SchemaPlan, SchemaError> {
    let manager = ctx.host.db.mutation_manager();
    let held = manager.replay_schema_winner(name).await?;
    let drop_held = manager.replay_schema_drop_winner(name).await?;
    let beats_entry = |bound: &ReplayStamp| {
        if replaced_here {
            bound > stamp
        } else {
            bound >= stamp
        }
    };
    if drop_held.as_ref().is_some_and(beats_entry) {
        return Ok(SchemaPlan::NoOp);
    }
    let existing = ctx.host.db.schema_manager().get_schema_metadata(name)?;
    if let Some(stored) = existing {
        if let Some(winner) = held.filter(beats_entry) {
            let compatible = SchemaCore::is_compatible_update(definition, &stored)?;
            return Ok(if compatible {
                SchemaPlan::NoOp
            } else {
                SchemaPlan::FenceLoser(winner)
            });
        }
        let compatible = SchemaCore::is_compatible_update(&stored, definition)?;
        return Ok(if compatible {
            SchemaPlan::Load { fence_drop: false }
        } else {
            SchemaPlan::ReplaceIncompatible
        });
    }
    if held.as_ref().is_some_and(beats_entry) {
        return Ok(SchemaPlan::NoOp);
    }
    Ok(SchemaPlan::Load {
        fence_drop: replaced_here || drop_held.is_some(),
    })
}

async fn commit_put(
    ctx: &Ctx<'_>,
    name: &str,
    definition: Schema,
    stamp: &ReplayStamp,
    replace_receipt: (&[u8; 32], &[u8; 32]),
    reconciled_floor: Option<&ReplayStamp>,
) -> Result<(), SchemaError> {
    let manager = ctx.host.db.mutation_manager();
    manager.observe_replay_time(stamp.ordering_ms).await?;
    let replaced_here = ctx
        .host
        .db
        .schema_manager()
        .replay_receipt_exists(replace_receipt.0, replace_receipt.1)
        .await?;
    match classify_in_scope(ctx, name, &definition, stamp, replaced_here).await? {
        SchemaPlan::NoOp => Ok(()),
        SchemaPlan::FenceLoser(winner) => {
            if reconciled_floor != Some(&winner) {
                return Err(SchemaError::Blocked(
                    "schema winner changed during replay; retrying floor reconciliation".into(),
                ));
            }
            manager.accept_replay_drop_schema(name, &winner).await?;
            Ok(())
        }
        SchemaPlan::Load { fence_drop } => {
            ctx.host
                .db
                .schema_manager()
                .load_schema_internal(definition)
                .await?;
            manager.accept_replay_schema(name, stamp).await?;
            if fence_drop {
                manager.accept_replay_drop_schema(name, stamp).await?;
            }
            Ok(())
        }
        SchemaPlan::ReplaceIncompatible => Err(SchemaError::Blocked(
            "schema changed incompatibly during replay; retrying replacement".into(),
        )),
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
    let manager = ctx.host.db.mutation_manager();
    let stamp = stamp_of(header);
    let (id, digest) = schema_receipt(b"lastdb-personal-schema-drop-v1", header, next);
    let done = async {
        ctx.host
            .db
            .schema_manager()
            .drop_replay_schema(name, &id, &digest, &stamp)
            .await?;
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

fn schema_receipt(
    domain: &[u8],
    header: &EntryHeader,
    next: &ReplayStreamHead,
) -> ([u8; 32], [u8; 32]) {
    let mut identity = Sha256::new();
    identity.update(domain);
    identity.update(header.node_key.as_bytes());
    identity.update(header.device_id.as_bytes());
    identity.update(header.writer_seq.to_be_bytes());
    let digest = Sha256::digest(next.hash.as_bytes());
    (identity.finalize().into(), digest.into())
}
