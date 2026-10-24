use super::common::*;
use crate::db_operations::AtomStore;
use crate::schema::types::MutationType;
use serde_json::{json, Value};
use std::path::Path;

pub(super) async fn seed(path: &Path) -> Result<Value> {
    let db = open(path, true, false).await?;
    let blob = db
        .put_local_file_blob(b"generated shared file for PR369", None, None)
        .await?;
    let writes = ["file-a", "file-b"]
        .iter()
        .map(|key| mutation(key, blob.pointer.clone(), MutationType::Create))
        .collect();
    put(&db, writes).await?;
    let atom = AtomStore::create_atom(SCHEMA, blob.pointer.clone(), None, None)?;
    let counters = db
        .db_ops()
        .atoms()
        .atom_counter_views(&[atom.uuid().into()], None)
        .await?;
    require(
        counters[0].refs == Some(2) && counters[0].body_present,
        "shared file atom does not have two tip references",
    )?;
    put(
        &db,
        vec![mutation("file-a", Value::Null, MutationType::Delete)],
    )
    .await?;
    let counters = db
        .db_ops()
        .atoms()
        .atom_counter_views(&[atom.uuid().into()], None)
        .await?;
    require(
        counters[0].refs == Some(1) && counters[0].body_present,
        "first deletion removed the shared file atom",
    )?;
    let blobs = db
        .db_ops()
        .atoms()
        .blob_counter_views(&[blob.blob_ref.clone()])
        .await?;
    require(
        blobs[0].refs == Some(1) && blobs[0].row_present,
        "referenced blob disappeared",
    )?;
    std::fs::write(
        path.join("expected.json"),
        serde_json::to_vec(&json!({
            "atom":atom.uuid(), "blob":blob.blob_ref, "pointer":blob.pointer
        }))?,
    )?;
    let verified = verify(&db).await?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":"files-seed", "passed":true, "shared_refs_after_delete":1, "verify":verified}))
}

pub(super) async fn finish(path: &Path) -> Result<Value> {
    let expected: Value = serde_json::from_slice(&std::fs::read(path.join("expected.json"))?)?;
    let atom = expected["atom"].as_str().ok_or("atom absent")?.to_string();
    let blob = expected["blob"].as_str().ok_or("blob absent")?.to_string();
    let db = open(path, false, false).await?;
    require(
        values(&db, &["file-a".into(), "file-b".into()]).await?
            == vec![None, Some(expected["pointer"].clone())],
        "shared file changed at restart",
    )?;
    let packed = compact(&db).await?;
    let counters = db
        .db_ops()
        .atoms()
        .atom_counter_views(&[atom.clone()], None)
        .await?;
    require(
        counters[0].refs == Some(1) && counters[0].body_present,
        "compaction removed a shared atom",
    )?;
    let blobs = db
        .db_ops()
        .atoms()
        .blob_counter_views(&[blob.clone()])
        .await?;
    require(
        blobs[0].refs == Some(1) && blobs[0].row_present,
        "compaction removed a referenced blob",
    )?;
    put(
        &db,
        vec![mutation("file-b", Value::Null, MutationType::Purge)],
    )
    .await?;
    let counters = db
        .db_ops()
        .atoms()
        .atom_counter_views(&[atom], None)
        .await?;
    require(
        counters[0].refs.is_none() && !counters[0].body_present,
        "last purge retained the atom",
    )?;
    let blobs = db
        .db_ops()
        .atoms()
        .blob_counter_views(&[blob.clone()])
        .await?;
    require(
        blobs[0].refs == Some(0) && blobs[0].zero_since.is_some() && blobs[0].row_present,
        "last purge did not queue the file dwell",
    )?;
    db.shutdown_local_only().await?;
    Ok(json!({"case":"files-finish", "passed":true, "compaction":packed, "zero_blob_queued":true}))
}

pub(super) async fn dwell(path: &Path) -> Result<Value> {
    let expected: Value = serde_json::from_slice(&std::fs::read(path.join("expected.json"))?)?;
    let blob = expected["blob"].as_str().ok_or("blob absent")?.to_string();
    let db = open(path, false, false).await?;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let atoms = db.db_ops().atoms();
    let guard = atoms
        .begin_blob_dwell_pass()
        .ok_or("dwell pass unavailable")?;
    let mut cursor = guard.take_cursor(32);
    atoms.blob_dwell_begin(&mut cursor).await?;
    let mut deleted = 0;
    for _ in 0..128 {
        let Some(step) = atoms.blob_dwell_step(&mut cursor, 32).await? else {
            break;
        };
        deleted += step.deleted;
    }
    guard.put_cursor(cursor);
    let blobs = atoms.blob_counter_views(&[blob]).await?;
    require(
        deleted > 0 && blobs[0].refs.is_none() && !blobs[0].row_present,
        "restart did not complete the unreferenced file dwell",
    )?;
    let verified = verify(&db).await?;
    let packed = compact(&db).await?;
    db.shutdown_local_only().await?;
    Ok(
        json!({"case":"files-dwell", "passed":true, "deleted":deleted,
        "verify":verified, "compaction":packed}),
    )
}
