use super::common::*;
use crate::db_operations::AtomStore;
use crate::schema::types::MutationType;
use serde_json::{json, Value};
use std::path::Path;

pub(super) async fn concurrency(path: &Path) -> Result<Value> {
    let db = open(path, true, true).await?;
    let now = crate::clock::unix_secs();
    let names: Vec<String> = (0..8).map(|index| format!("race-{index}")).collect();
    let old = names
        .iter()
        .map(|key| {
            sign(
                mutation(key, json!("old"), MutationType::Create),
                now - 120,
                None,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    replay(&db, old, "initial").await?;
    let purges = names
        .iter()
        .map(|key| {
            sign(
                mutation(key, Value::Null, MutationType::Purge),
                now,
                Some(now - 60),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let writes = names
        .iter()
        .map(|key| {
            sign(
                mutation(key, json!("fresh"), MutationType::Update),
                now - 10,
                None,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let (purged, written) =
        tokio::join!(replay(&db, purges, "sweep"), replay(&db, writes, "writer"));
    purged?;
    written?;
    require(
        values(&db, &names).await? == vec![Some(json!("fresh")); names.len()],
        "TTL purge removed a concurrent fresh write",
    )?;
    let refs = verify(&db).await?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":"concurrency", "passed":true, "keys":8, "verify":refs}))
}

pub(super) async fn retry(path: &Path) -> Result<Value> {
    let db = open(path, true, false).await?;
    let original = mutation("retry", json!("same"), MutationType::Create);
    let ids = put(&db, vec![original.clone()]).await?;
    let repeated = put(&db, vec![original.clone()]).await?;
    require(ids == repeated, "unchanged retry lost its receipt")?;
    for kind in [MutationType::Delete, MutationType::Purge] {
        put(&db, vec![mutation("retry", Value::Null, kind)]).await?;
        require(
            values(&db, &["retry".into()]).await? == vec![None],
            "erase left retry key visible",
        )?;
        let mut next = original.clone();
        next.uuid = uuid::Uuid::new_v4().to_string();
        put(&db, vec![next]).await?;
        require(
            values(&db, &["retry".into()]).await? == vec![Some(json!("same"))],
            "cached receipt hid a retry after deletion",
        )?;
    }
    let refs = verify(&db).await?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":"retry", "passed":true, "delete_and_purge":true, "verify":refs}))
}

pub(super) async fn conflicts_seed(path: &Path) -> Result<Value> {
    let db = open(path, true, false).await?;
    let now = crate::clock::unix_secs();
    replay(
        &db,
        vec![sign(
            mutation("conflicts", json!("winner"), MutationType::Create),
            now - 10,
            None,
        )?],
        "winner",
    )
    .await?;
    let losers = (0..108)
        .map(|index| {
            sign(
                mutation(
                    "conflicts",
                    json!(format!("loser-{index}")),
                    MutationType::Update,
                ),
                now - 300 - index,
                None,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    replay(&db, losers, "loser").await?;
    let verified = verify(&db).await?;
    require(
        verified["records"].as_u64().unwrap_or(0) > 100,
        "conflicts did not exceed erase release cap",
    )?;
    put(
        &db,
        vec![mutation("conflicts", Value::Null, MutationType::Purge)],
    )
    .await?;
    let after = verify(&db).await?;
    require(
        after["records"].as_u64().unwrap_or(0) > 0,
        "purge did not leave a drain for restart",
    )?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":"conflicts-seed", "passed":true, "before":verified, "after":after}))
}

pub(super) async fn conflicts_drain(path: &Path) -> Result<Value> {
    let db = open(path, false, false).await?;
    let atoms = db.db_ops().atoms();
    let mut cursor = atoms.retention_cursor();
    let now = atoms.retention_state().clock_nanos();
    atoms.refresh_queue(&mut cursor, now).await?;
    let mut released = 0;
    for _ in 0..128 {
        let ordinary = atoms.release_step(&mut cursor, now).await?;
        let drain = atoms.drain_step(&mut cursor).await?;
        released += ordinary.as_ref().map_or(0, |step| step.released)
            + drain.as_ref().map_or(0, |step| step.released);
        if ordinary.is_none() && drain.is_none() {
            break;
        }
    }
    require(released > 0, "restart found no pending conflict drain")?;
    let ids = (0..108)
        .map(|index| {
            AtomStore::create_atom(SCHEMA, json!(format!("loser-{index}")), None, None)
                .map(|atom| atom.uuid().to_string())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let counters = atoms.atom_counter_views(&ids, None).await?;
    require(
        counters
            .iter()
            .all(|counter| counter.refs.is_none() && !counter.body_present),
        "released conflict retained an atom body or counter",
    )?;
    let verified = verify(&db).await?;
    require(
        verified["records"].as_u64() == Some(0),
        "conflict drain left retained records",
    )?;
    let packed = compact(&db).await?;
    db.shutdown_local_only().await?;
    Ok(
        json!({"case":"conflicts-drain", "passed":true, "released":released,
        "verify":verified, "compaction":packed}),
    )
}
