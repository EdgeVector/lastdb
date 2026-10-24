//! Real local file upload, shared atom holders, and the normal minimum dwell.
use super::{mutation, FoldDB, MutationType, Result};
use crate::atom::Atom;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn measure(db: &FoldDB) -> Result<Value> {
    let put = db
        .put_local_file_blob(&vec![0x4c; 128 * 1024], None, None)
        .await?;
    let id = Atom::new("LifecycleBlob".into(), put.pointer.clone())
        .uuid()
        .to_string();
    let atoms = [id];
    let blobs = [put.blob_ref];
    let mut batch = Vec::new();
    for index in 0..2 {
        let mut item = mutation("LifecycleBlob", index, "", MutationType::Create);
        item.fields_and_values
            .insert("body".into(), put.pointer.clone());
        batch.push(item);
    }
    db.mutation_manager()
        .write_mutations_batch_async(batch, None)
        .await?;
    let two = db.db_ops().atoms().atom_counter_views(&atoms, None).await?;
    if two[0].refs != Some(2) || !two[0].body_present {
        return Err("the real file-pointer atom does not have its two tip holders".into());
    }
    db.mutation_manager()
        .write_mutations_batch_async(
            vec![mutation("LifecycleBlob", 0, "", MutationType::Purge)],
            None,
        )
        .await?;
    let one = db.db_ops().atoms().atom_counter_views(&atoms, None).await?;
    if one[0].refs != Some(1) || !one[0].body_present {
        return Err("the first purge removed a shared atom body".into());
    }
    let held_blob = db.db_ops().atoms().blob_counter_views(&blobs).await?;
    if held_blob[0].refs.is_none_or(|count| count == 0) || !held_blob[0].row_present {
        return Err("the first purge removed a referenced file blob".into());
    }
    db.mutation_manager()
        .write_mutations_batch_async(
            vec![mutation("LifecycleBlob", 1, "", MutationType::Purge)],
            None,
        )
        .await?;
    let zero = db.db_ops().atoms().atom_counter_views(&atoms, None).await?;
    let waiting = db.db_ops().atoms().blob_counter_views(&blobs).await?;
    if zero[0].refs.is_some()
        || zero[0].body_present
        || waiting[0].refs != Some(0)
        || !waiting[0].row_present
        || !waiting[0].queued
    {
        return Err("the final holder did not delete the atom and start the blob dwell".into());
    }
    // Respect the normal minimum; no fake clock or unsafe short dwell.
    tokio::time::sleep(Duration::from_secs(121)).await;
    let store = db.db_ops().atoms();
    let guard = store
        .begin_blob_dwell_pass()
        .ok_or("the generated dwell pass is busy")?;
    let mut cursor = guard.take_cursor(64);
    store.blob_dwell_begin(&mut cursor).await?;
    let mut deleted = 0;
    for _ in 0..32 {
        let Some(step) = store.blob_dwell_step(&mut cursor, 64).await? else {
            break;
        };
        deleted += step.deleted;
    }
    guard.put_cursor(cursor);
    let gone = store.blob_counter_views(&blobs).await?;
    if deleted == 0 || gone[0].row_present || gone[0].refs.is_some() {
        return Err("the actual due dwell did not remove the unreferenced file blob".into());
    }
    Ok(
        json!({"file_bytes":put.bytes,"two_holders":two,"one_holder":one,
        "last_holder_removed":zero,"blob_while_held":held_blob,"blob_during_dwell":waiting,
        "blob_after_dwell":gone,"dwell_seconds":121,"deleted_blobs":deleted}),
    )
}
