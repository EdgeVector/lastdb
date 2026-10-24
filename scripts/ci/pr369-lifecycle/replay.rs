use super::common::*;
use crate::schema::types::MutationType;
use serde_json::{json, Value};
use std::path::Path;

pub(super) async fn run(mode: &str, path: &Path, fixed: bool) -> Result<Value> {
    let db = open(path, true, true).await?;
    let now = crate::clock::unix_secs();
    let observed = match mode {
        "delete-age" => delete_age(&db, path, now).await?,
        "cutoff" => cutoff(&db, now).await?,
        "replay-age" => replay_age(&db, now).await?,
        "losing-age" => losing_age(&db, now).await?,
        _ => return Err("unknown replay case".into()),
    };
    require(
        observed == fixed,
        "observed TTL result differs from the expected source version",
    )?;
    let refs = verify(&db).await?;
    db.shutdown_local_only().await?;
    Ok(
        json!({"case": mode, "passed": true, "correct_behavior": observed,
        "expected_fixed": fixed, "baseline_fault_reproduced": !fixed, "verify": refs}),
    )
}

async fn delete_age(db: &crate::fold_db_core::FoldDB, path: &Path, now: u64) -> Result<bool> {
    replay(
        db,
        vec![sign(
            mutation("spared", json!("current"), MutationType::Create),
            now - 10,
            None,
        )?],
        "winner",
    )
    .await?;
    let before = tombstones(path)?;
    replay(
        db,
        vec![sign(
            mutation("spared", Value::Null, MutationType::Delete),
            now - 20,
            None,
        )?],
        "older-delete",
    )
    .await?;
    require(
        values(db, &["spared".into()]).await? == vec![Some(json!("current"))],
        "older Delete removed the newer record",
    )?;
    let has_age = expired(db, "spared", now + 2).await?;
    let search_preserved = tombstones(path)? == before;
    require(
        has_age == search_preserved,
        "TTL and LastSeek cleanup verdicts differ",
    )?;
    Ok(has_age && search_preserved)
}

async fn cutoff(db: &crate::fold_db_core::FoldDB, now: u64) -> Result<bool> {
    replay(
        db,
        vec![sign(
            mutation("cutoff", json!("expired"), MutationType::Create),
            now - 120,
            None,
        )?],
        "before-cutoff",
    )
    .await?;
    replay(
        db,
        vec![sign(
            mutation("cutoff", Value::Null, MutationType::Delete),
            now - 20,
            Some(now - 60),
        )?],
        "ttl-delete",
    )
    .await?;
    require(
        values(db, &["cutoff".into()]).await? == vec![None],
        "TTL Delete did not erase its old target",
    )?;
    replay(
        db,
        vec![sign(
            mutation("cutoff", json!("eligible"), MutationType::Create),
            now - 40,
            None,
        )?],
        "after-cutoff",
    )
    .await?;
    Ok(values(db, &["cutoff".into()]).await? == vec![Some(json!("eligible"))])
}

async fn replay_age(db: &crate::fold_db_core::FoldDB, now: u64) -> Result<bool> {
    let old = sign(
        mutation("revived", json!("old"), MutationType::Create),
        now - 300,
        None,
    )?;
    replay(db, vec![old.clone()], "original").await?;
    put(
        db,
        vec![mutation("revived", Value::Null, MutationType::Purge)],
    )
    .await?;
    require(
        values(db, &["revived".into()]).await? == vec![None],
        "purge left its target visible",
    )?;
    replay(db, vec![old], "original").await?;
    require(
        values(db, &["revived".into()]).await? == vec![Some(json!("old"))],
        "permitted Put replay did not restore its key",
    )?;
    expired(db, "revived", now - 30).await
}

async fn losing_age(db: &crate::fold_db_core::FoldDB, now: u64) -> Result<bool> {
    replay(
        db,
        vec![sign(
            mutation("losing", json!("winner"), MutationType::Create),
            now - 120,
            None,
        )?],
        "winner",
    )
    .await?;
    replay(
        db,
        vec![sign(
            mutation("losing", json!("loser"), MutationType::Update),
            now - 180,
            None,
        )?],
        "loser",
    )
    .await?;
    require(
        values(db, &["losing".into()]).await? == vec![Some(json!("winner"))],
        "older Put replaced the winner",
    )?;
    expired(db, "losing", now - 30).await
}
