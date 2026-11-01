//! Exact read-only validation under the exclusive, inactive restore owner.

mod reader;

use crate::db_operations::atom_store::{AtomStore, RecountOptions, RecountReport, VerifyRequest};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{restore_safety, PackRuntime};
use crate::storage::{KvStore, NamespacedStore};
use std::collections::BTreeSet;
use std::sync::Arc;

pub(crate) async fn validate(store: &dyn NamespacedStore) -> Result<(), SchemaError> {
    let runtime = store
        .disk_pack_runtime()
        .ok_or_else(|| invalid("native restore owner is missing"))?;
    let raw = restore_safety::raw_main(Arc::clone(&runtime)).await?;
    let main = store.open_namespace("main").await?;
    let scopes = restored_scopes(raw.as_ref(), main.as_ref()).await?;
    let namespaces = reader::open(store, runtime, Arc::clone(&main)).await?;
    let atoms = AtomStore::new(Arc::new(crate::storage::TypedKvStore::new(main)))
        .with_namespaced_store(namespaces);
    atoms.minimum_replay_time().await?;
    // The retention queue and the blob counters are global: one personal run
    // checks them. The blob recount below reads the records of every scope.
    let mut request = VerifyRequest {
        max_pages: Some(1024),
        max_ms: Some(30_000),
        pause_ms: Some(0),
        ..VerifyRequest::default()
    };
    loop {
        let report = atoms.verify_refs_step(request.clone()).await?;
        if report.mismatches_total != 0 || !report.notes.is_empty() {
            return Err(invalid(
                "the restored retention queue or blob counters do not have a complete exact match",
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
    validate_global_blobs(&atoms, scopes).await?;
    Ok(())
}

async fn validate_global_blobs(
    atoms: &AtomStore,
    scopes: BTreeSet<Option<String>>,
) -> Result<(), SchemaError> {
    let options = RecountOptions {
        execute: false,
        blobs_exact: true,
        scopes: scopes.into_iter().collect(),
        ..RecountOptions::default()
    };
    let mut report = RecountReport::default();
    atoms.recount_blobs(&options, &mut report).await?;
    if !report.blobs.ran
        || !report.blobs.exact
        || report.changed() != 0
        || report.blobs.unreadable_counters != 0
        || report.blobs.rows_missing != 0
        || !report.blobs.notes.is_empty()
    {
        return Err(invalid(
            "restored global file references do not match their counters and files",
        ));
    }
    Ok(())
}

async fn restored_scopes(
    raw: &dyn KvStore,
    logical: &dyn KvStore,
) -> Result<BTreeSet<Option<String>>, SchemaError> {
    let mut scopes = BTreeSet::from([None]);
    let mut cursor = None;
    loop {
        let page = raw
            .scan_range_physical_paged(b"", b"\xff", cursor.as_ref(), 128, 1)
            .await?;
        let mut keys = Vec::new();
        for (key, _) in page.rows {
            crate::storage::current_keys::validate("main", &key)?;
            if crate::storage::current_keys::counter_owned(&key)? {
                let text =
                    std::str::from_utf8(&key).map_err(|_| invalid("invalid restored key"))?;
                scopes.insert(crate::storage::current_keys::exact_scope(text)?.map(str::to_owned));
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
    Ok(scopes)
}

pub(crate) async fn validate_pending(store: &dyn NamespacedStore) -> Result<(), SchemaError> {
    let Some(runtime) = store.disk_pack_runtime() else {
        return Ok(());
    };
    if !runtime.restore_validation_pending() {
        return Ok(());
    }
    let owner = Arc::clone(&runtime);
    restore_safety::exclusive(runtime, async {
        // Matching counts cannot prove that every namespace arrived.
        // Only a complete row-by-row restore can clear this key.
        if restore_safety::raw_main(Arc::clone(&owner))
            .await?
            .get(restore_safety::PENDING_KEY.as_bytes())
            .await?
            .is_some()
        {
            return Err(invalid(
                "snapshot restore is incomplete; repeat the complete restore before startup",
            ));
        }
        validate(store).await?;
        finish(store, &owner).await
    })
    .await
}

pub(crate) async fn finish(
    store: &dyn NamespacedStore,
    runtime: &PackRuntime,
) -> Result<(), SchemaError> {
    store
        .open_namespace("main")
        .await?
        .delete(restore_safety::PENDING_KEY.as_bytes())
        .await?;
    store.restore_durability_barrier().await?;
    runtime.finish_restore_validation()?;
    Ok(())
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(format!(
        "restore counter validation: {message}; target remains blocked"
    ))
}
