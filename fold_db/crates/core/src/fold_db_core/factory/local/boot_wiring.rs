//! Boot wiring helpers for the local FoldDB factory: DbOperations open,
//! decrypt-integrity gate, and sync-engine attach.

use super::{assert_critical_namespaces_decryptable, map_boot_decrypt_error, OpenPrimaryStore};
use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use crate::fold_db_core::FoldDB;
use std::sync::Arc;

/// Open [`DbOperations`] over the stacked store with the atom-content key and
/// molecule key codec derived from the identity E2E root.
pub(super) async fn open_db_operations(
    store: Arc<dyn crate::storage::traits::NamespacedStore>,
    primary_store: &OpenPrimaryStore,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<DbOperations> {
    // Plain hash-group: seal atom content field. Frame-AEAD protects via
    // packaging. Segment-log uses whole-value ENC seam instead.
    let atom_content_key = match (
        primary_store
            .laststore
            .as_ref()
            .map(|s| s.options().packaging),
        primary_store
            .laststore
            .as_ref()
            .map(|s| s.options().layout_mode),
    ) {
        (Some(laststore::PackagingMode::Plain), Some(laststore::LayoutMode::HashGroup)) => {
            Some(e2e_keys.encryption_key())
        }
        // Native frames stay plain; retain the field codec and copied ENC reads.
        _ if primary_store.store.disk_pack_runtime().is_some() => Some(e2e_keys.encryption_key()),
        _ => None,
    };
    // HashKey blind + RangeKey OPE (design-lastdb-hashkey-blind-v1 /
    // design-lastdb-rangekey-ope-v1): product default is blind+OPE when env is
    // unset (fresh Mini). Explicit plain via env for tests. Keys always come
    // from identity E2E root so first boot writes encrypted storage keys.
    let hash_key_encoding = crate::atom::HashKeyEncoding::from_env_or_default();
    let range_key_encoding = crate::atom::RangeKeyEncoding::from_env_or_default();
    tracing::info!(
        ?hash_key_encoding,
        ?range_key_encoding,
        "molecule key storage encoding (LASTDB_HASH_KEY_ENCODING / LASTDB_RANGE_KEY_ENCODING; default blind_v1+ope_v1)"
    );
    let hash_key_codec = crate::atom::MoleculeKeyCodec::with_encodings(
        hash_key_encoding,
        range_key_encoding,
        Some(e2e_keys.index_key()),
        Some(e2e_keys.ope_key()),
    );
    DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
        store,
        atom_content_key,
        hash_key_codec,
        Some(e2e_keys.encryption_key()),
    )
    .await
    .map_err(|e| FoldDbError::Config(e.to_string()))
}

/// Boot-time decrypt integrity gate for the encrypting seam or the raw LastStore.
pub(super) async fn verify_boot_decrypt_integrity(
    enc_store_ref: Option<&crate::storage::EncryptingNamespacedStore>,
    primary_store: &OpenPrimaryStore,
) -> FoldDbResult<()> {
    // Boot-time decrypt integrity gate (incident-lastdbd-0226-wrong-key-fresh-db):
    // if the local store already holds user namespaces, every critical
    // encrypted namespace must open cleanly under the current key. A single
    // undecryptable row used to surface later as empty board history while the
    // daemon kept serving and writing under the same home — refuse instead.
    if let Some(enc) = enc_store_ref {
        use crate::storage::traits::NamespacedStore;
        let namespaces = enc.list_namespaces().await.unwrap_or_default();
        let has_user_data = namespaces.iter().any(|ns| ns != "__sled__default");
        if has_user_data {
            assert_critical_namespaces_decryptable(enc).await?;
        }
    } else if let Some(laststore) = primary_store.laststore.as_deref() {
        laststore.verify_integrity().map_err(|e| {
            map_boot_decrypt_error(format!("laststore frame/footer verification failed: {e}"))
        })?;
    }
    Ok(())
}

/// Attach the sync engine to the node, run cloud bootstrap when needed, and
/// start the backup uploader and sync loop unless cloud workers are deferred.
pub(super) async fn wire_sync_engine(
    fold_db: &FoldDB,
    engine: Arc<crate::sync::SyncEngine>,
    defer_cloud_workers: bool,
    bootstrap_from_cloud: bool,
    sync_interval_ms: u64,
) -> FoldDbResult<()> {
    if defer_cloud_workers {
        engine.set_backup_only_mode().await;
        // The active configuration plus a durable resume marker means
        // local writes after this boot must enter the tail. The backup-only
        // interlock still refuses upload and no worker starts below.
        engine.set_cloud_sync_disabled(false).await;
        fold_db.set_sync_engine(Arc::clone(&engine)).await;
    } else {
        fold_db.set_sync_engine(Arc::clone(&engine)).await;
    }
    let bootstrap_result: FoldDbResult<()> = async {
        if bootstrap_from_cloud {
            engine.bootstrap_all().await.map_err(|e| {
                FoldDbError::Config(format!("cloud photograph bootstrap failed: {e}"))
            })?;
        }
        // Restore org (+ share) cloud-sync targets registered on prior boots.
        if let Err(e) = fold_db.apply_org_sync_targets_from_store().await {
            tracing::warn!(
                error = %e,
                "failed to re-apply org sync targets from store at boot"
            );
        }
        if bootstrap_from_cloud {
            engine
                .bootstrap_targets_from(1)
                .await
                .map_err(|e| FoldDbError::Config(format!("scoped cloud bootstrap failed: {e}")))?;
        }
        Ok(())
    }
    .await;
    match bootstrap_result {
        Ok(()) => {
            if !defer_cloud_workers {
                engine.start_laststore_backup_uploader();
                fold_db.start_sync(sync_interval_ms);
            }
        }
        Err(err) => {
            engine.stop_laststore_backup_uploader();
            return Err(err);
        }
    }
    Ok(())
}
