//! Finite CI operations on generated homes; no source rows are fabricated.
use crate::db_operations::atom_store::RangeWalk;
use crate::fold_db_core::fold_db::FoldDB;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Instant;

mod blob;
mod repair_drill;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The executable verifies exact generated-home ownership before this call.
pub async fn measure(db: &FoldDB, phase: &str) -> Result<Value> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true")
        || std::env::var("LASTDB_ISOLATED_COPY").as_deref() != Ok("1")
        || db.sync_engine().is_some()
    {
        return Err("lifecycle measurements require isolated CI without cloud sync".into());
    }
    let started = Instant::now();
    let mut value = match phase {
        "conflicts" => conflicts(db).await?,
        "purge" => purge(db).await?,
        "drain" => drain(db).await?,
        "replay" => replay(db).await?,
        "blob" => blob::measure(db).await?,
        phase if phase.starts_with("repair-") => repair_drill::measure(db, phase).await?,
        _ => return Err("unknown finite lifecycle operation".into()),
    };
    value["phase"] = json!(phase);
    value["elapsed_ms"] = json!(started.elapsed().as_secs_f64() * 1000.0);
    Ok(value)
}

fn mutation(schema: &str, index: usize, body: &str, kind: MutationType) -> Mutation {
    let erase = matches!(kind, MutationType::Delete | MutationType::Purge);
    let fields = if erase {
        HashMap::new()
    } else {
        HashMap::from([
            ("bucket".into(), json!("generated")),
            ("id".into(), json!(format!("key-{index:04}"))),
            ("body".into(), json!(body)),
        ])
    };
    let mut mutation = Mutation::new(
        schema.into(),
        fields,
        KeyValue::new(Some("generated".into()), Some(format!("key-{index:04}"))),
        "lifecycle-ci".into(),
        kind,
    );
    mutation.synchronous = Some(true);
    mutation
}

async fn conflict_rows(db: &FoldDB) -> Result<u64> {
    let mut walk = RangeWalk::new(b"history\0", b"history\x01", 256);
    let mut total = 0;
    for _ in 0..64 {
        if walk.done() {
            return Ok(total);
        }
        let page = db.db_ops().atoms().walk_step(&mut walk).await?;
        if page.unsealed > 0 {
            return Err("history inventory dropped storage rows".into());
        }
        total += page
            .rows
            .iter()
            .filter(|(key, _)| {
                key.windows(b":conflict:".len())
                    .any(|part| part == b":conflict:")
            })
            .count() as u64;
    }
    Err("generated history inventory exceeds 16384 rows".into())
}

async fn conflicts(db: &FoldDB) -> Result<Value> {
    let now = crate::clock::unix_nanos();
    for revision in 0..4 {
        let batch = (0..64)
            .map(|index| {
                let mut item = mutation(
                    "LifecycleHistory",
                    index,
                    &format!("revision-{revision}-key-{index}"),
                    MutationType::Update,
                );
                item.imported_written_at = Some(now - revision * 1_000_000);
                item
            })
            .collect();
        db.mutation_manager()
            .apply_replayed_mutations(batch, None, &format!("lifecycle-source-{revision}"))
            .await?;
    }
    let retained = conflict_rows(db).await?;
    if retained <= 100 {
        return Err("the actual conflict workload did not exceed the erase record cap".into());
    }
    Ok(
        json!({"mutations":256,"keys":64,"retained_conflicts":retained,
        "retention":db.conflict_retention_report().await}),
    )
}

async fn purge(db: &FoldDB) -> Result<Value> {
    let before = conflict_rows(db).await?;
    let batch = (0..64)
        .map(|index| mutation("LifecycleHistory", index, "", MutationType::Purge))
        .collect();
    db.mutation_manager()
        .write_mutations_batch_async(batch, None)
        .await?;
    let after = conflict_rows(db).await?;
    if before <= after || after == 0 {
        return Err("the actual purge did not leave a bounded background drain".into());
    }
    Ok(
        json!({"keys":64,"conflicts_before":before,"conflicts_after":after,
        "retention":db.conflict_retention_report().await}),
    )
}

async fn drain(db: &FoldDB) -> Result<Value> {
    let before = conflict_rows(db).await?;
    db.mutation_manager()
        .write_mutations_batch_async(
            vec![mutation(
                "LifecycleHistory",
                0,
                "reused-tip",
                MutationType::Create,
            )],
            None,
        )
        .await?;
    let mut losing = mutation("LifecycleHistory", 0, "reused-loser", MutationType::Update);
    losing.imported_written_at = Some(crate::clock::unix_nanos() - 1_000_000_000);
    db.mutation_manager()
        .apply_replayed_mutations(vec![losing], None, "lifecycle-reused-source")
        .await?;
    let atoms = db.db_ops().atoms();
    let mut cursor = atoms.retention_cursor();
    let mut released = 0;
    let mut finished = 0;
    let mut steps = 0;
    for _ in 0..512 {
        steps += 1;
        let now = crate::clock::unix_nanos();
        atoms.refresh_queue(&mut cursor, now).await?;
        let ordinary = atoms.release_step(&mut cursor, now).await?;
        let drain = atoms.drain_step(&mut cursor).await?;
        for step in ordinary.iter().chain(drain.iter()) {
            released += step.released;
            finished += step.finished;
        }
        if ordinary.is_none() && drain.is_none() && cursor.pending_drains() == 0 {
            break;
        }
    }
    let after = conflict_rows(db).await?;
    if released < before || finished == 0 || after == 0 || after > 3 {
        return Err(
            "the reopened drain did not finish old records and spare reused-slot records".into(),
        );
    }
    Ok(json!({"conflicts_before":before,"conflicts_after":after,
        "released":released,"finished_markers":finished,"steps":steps,
        "retention":db.conflict_retention_report().await}))
}

async fn replay(db: &FoldDB) -> Result<Value> {
    let floor = db.db_ops().atoms().minimum_replay_time().await?;
    if floor == 0 {
        return Err("the generated home has no explicit delete cutoff".into());
    }
    let mut refused = 0;
    for time in [None, Some(floor.saturating_sub(1)), Some(floor)] {
        let mut old = mutation("LifecycleReplay", 0, "old-replay", MutationType::Update);
        old.imported_written_at = time;
        match db
            .mutation_manager()
            .apply_replayed_mutations(vec![old], None, "lifecycle-offline-source")
            .await
        {
            Err(SchemaError::ReplayBeforeDeleteCutoff { .. }) => refused += 1,
            Err(error) => return Err(error.into()),
            Ok(_) => return Err("an old or unclocked replay crossed the delete cutoff".into()),
        }
    }
    let mut current = mutation("LifecycleReplay", 0, "post-cutoff", MutationType::Create);
    current.imported_written_at = Some(crate::clock::unix_nanos());
    db.mutation_manager()
        .apply_replayed_mutations(vec![current], None, "lifecycle-current-source")
        .await?;
    Ok(
        json!({"cutoff_nanos":floor,"refused_old_or_unclocked":refused,
        "accepted_new_replay":1}),
    )
}
