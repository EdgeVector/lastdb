//! Exact read-only validation under the exclusive, inactive restore owner.

mod reader;

use crate::db_operations::record_store::{RecordStore, VerifyRequest};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{restore_safety, PackRuntime};
use crate::storage::{KvStore, NamespacedStore};
use std::sync::Arc;

pub(crate) async fn validate(store: &dyn NamespacedStore) -> Result<(), SchemaError> {
    let runtime = store.disk_pack_runtime();
    let raw = restore_safety::raw_main(Arc::clone(&runtime)).await?;
    let main = store.open_namespace("main").await?;
    lifecycle_rows_read_back(raw.as_ref(), main.as_ref()).await?;
    let namespaces = reader::open(store, runtime, Arc::clone(&main)).await?;
    let records = RecordStore::new(
        Arc::new(crate::storage::TypedKvStore::new(main)),
        namespaces,
    );
    records.minimum_replay_time().await?;
    // The retention queue is global: one personal run checks it.
    let mut request = VerifyRequest {
        max_pages: Some(1024),
        max_ms: Some(30_000),
        pause_ms: Some(0),
        ..VerifyRequest::default()
    };
    loop {
        let report = records.verify_refs_step(request.clone()).await?;
        if report.mismatches_total != 0 || !report.notes.is_empty() {
            return Err(invalid(
                "the restored retention queue does not have a complete exact match",
            ));
        }
        if report.done {
            break;
        }
        let next = report
            .next_cursor
            .ok_or_else(|| invalid("reference verification has no cursor"))?;
        if request.cursor.as_ref() == Some(&next) {
            return Err(invalid("reference verification did not advance"));
        }
        request.cursor = Some(next);
    }
    Ok(())
}

/// Every restored row of the record lifecycle must read back from the logical
/// store.
async fn lifecycle_rows_read_back(
    raw: &dyn KvStore,
    logical: &dyn KvStore,
) -> Result<(), SchemaError> {
    let mut cursor = None;
    loop {
        let page = raw
            .scan_range_physical_paged(b"", b"\xff", cursor.as_ref(), 128, 1)
            .await?;
        let mut keys = Vec::new();
        for (key, _) in page.rows {
            crate::storage::current_keys::validate("main", &key)?;
            if crate::storage::current_keys::counter_owned(&key)? {
                keys.push(key);
            }
        }
        if !keys.is_empty() && logical.get_many(keys).await?.iter().any(Option::is_none) {
            return Err(invalid(
                "a restored lifecycle record does not read back from the logical store",
            ));
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        if cursor.as_ref() == Some(&next) {
            return Err(invalid("restore scope discovery did not advance"));
        }
        cursor = Some(next);
    }
    Ok(())
}

pub(crate) async fn validate_pending(store: &dyn NamespacedStore) -> Result<(), SchemaError> {
    let runtime = store.disk_pack_runtime();
    if !runtime.restore_validation_pending() {
        return Ok(());
    }
    let owner = Arc::clone(&runtime);
    restore_safety::exclusive(runtime, async {
        validate(store).await?;
        finish(store, &owner).await
    })
    .await
}

pub(crate) async fn finish(
    store: &dyn NamespacedStore,
    runtime: &PackRuntime,
) -> Result<(), SchemaError> {
    store.restore_durability_barrier().await?;
    runtime.finish_restore_validation()?;
    Ok(())
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(format!(
        "restore counter validation: {message}; target remains blocked"
    ))
}
