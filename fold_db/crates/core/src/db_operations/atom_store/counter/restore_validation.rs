//! Exact read-only restore validation while the native preparation gate is held.

use crate::db_operations::atom_store::{AtomStore, VerifyRequest};
use crate::schema::SchemaError;
use crate::storage::disk_pack::{restore_safety, PackRuntime};
use crate::storage::{KvStore, NamespacedStore};
use std::collections::BTreeSet;
use std::sync::Arc;

pub(crate) async fn validate(store: &dyn NamespacedStore) -> Result<(), SchemaError> {
    let runtime = store
        .disk_pack_runtime()
        .ok_or_else(|| invalid("native restore owner is missing"))?;
    let raw = restore_safety::raw_main(runtime).await?;
    let main = store.open_namespace("main").await?;
    let scopes = restored_scopes(raw.as_ref(), main.as_ref()).await?;
    let atoms = AtomStore::new(Arc::new(crate::storage::TypedKvStore::new(main)));
    for scope in scopes {
        let mut request = VerifyRequest {
            scope,
            max_pages: Some(1024),
            max_ms: Some(30_000),
            pause_ms: Some(0),
            ..VerifyRequest::default()
        };
        loop {
            let report = atoms.verify_refs_step(request.clone()).await?;
            if report.mismatches_total != 0
                || !report.tally_complete
                || report.touched_atoms != 0
                || report
                    .notes
                    .iter()
                    .any(|note| !note.starts_with("0 tips read in scope "))
            {
                return Err(invalid(
                    "restored records and counters do not have a complete exact match",
                ));
            }
            if report.done {
                break;
            }
            let next = report
                .next_cursor
                .ok_or_else(|| invalid("counter verification has no cursor"))?;
            if request.cursor.as_ref() == Some(&next) {
                return Err(invalid("counter verification did not advance"));
            }
            request.cursor = Some(next);
        }
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
            if crate::storage::current_keys::counter_owned("main", &key)? {
                let text =
                    std::str::from_utf8(&key).map_err(|_| invalid("invalid restored key"))?;
                scopes.insert(crate::storage::current_keys::exact_scope(text)?.map(str::to_owned));
                keys.push(key);
            }
        }
        if !keys.is_empty() && logical.get_many(keys).await?.iter().any(Option::is_none) {
            return Err(invalid(
                "a restored lifecycle record was discarded by the encryption reader",
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
