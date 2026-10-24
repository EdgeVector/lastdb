//! One restore owner excludes cleanup; a durable marker survives cancellation.

use super::*;
use crate::db_operations::atom_store::counter::restore_validation;
use crate::storage::disk_pack::restore_safety;

pub(super) async fn run<T>(
    snapshot: &Snapshot,
    store: &dyn NamespacedStore,
    logical: &dyn NamespacedStore,
    future: impl std::future::Future<Output = SyncResult<T>>,
) -> SyncResult<T> {
    // Refuse bad framing and reserved markers before changing the destination.
    snapshot.validate_current_version()?;
    snapshot.validate_unique_restore_keys()?;
    let runtime = store.disk_pack_runtime().ok_or_else(|| {
        SyncError::Storage("restore requires an exclusive native counter-validation scope".into())
    })?;
    if !logical
        .disk_pack_runtime()
        .is_some_and(|reader| Arc::ptr_eq(&runtime, &reader))
    {
        return Err(SyncError::Storage(
            "restore counter reader belongs to a different home".into(),
        ));
    }
    let owner = Arc::clone(&runtime);
    restore_safety::exclusive(
        runtime,
        crate::sync::capture::with_capture_suppressed(async {
            let main = store.open_namespace("main").await?;
            main.put(restore_safety::PENDING_KEY.as_bytes(), b"pending".to_vec())
                .await?;
            store.restore_durability_barrier().await?;
            let result = future.await?;
            restore_validation::validate(logical)
                .await
                .map_err(|e| SyncError::Storage(e.to_string()))?;
            restore_validation::finish(store, &owner)
                .await
                .map_err(|e| SyncError::Storage(e.to_string()))?;
            Ok(result)
        }),
    )
    .await
}
