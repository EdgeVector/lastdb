//! Fresh namespaces keep all old receiver records reachable on disk.

use super::*;
use crate::sync::snapshot::Snapshot;
use base64::{engine::general_purpose::STANDARD, Engine as _};

pub(super) async fn write(
    engine: &SyncEngine,
    staged: &[stage::StagedShare],
    receiver: &str,
) -> SyncResult<()> {
    for item in staged {
        let meta = std::fs::symlink_metadata(&item.path)?;
        if !meta.is_file()
            || meta.file_type().is_symlink()
            || meta.len() > (64 * 1024 * 1024 + 4096)
        {
            return Err(held("the retained share seed file is unsafe"));
        }
        let bytes = std::fs::read(&item.path)?;
        if crate::hex::sha256_hex(&bytes) != item.ciphertext_sha256 {
            return Err(held("the retained share seed bytes changed"));
        }
        let snapshot = Snapshot::unseal(&bytes, &item.target.crypto).await?;
        stage::validate(&snapshot, &item.target.prefix)?;
        if crate::hex::sha256_hex(serde_json::to_vec(&snapshot)?) != item.snapshot_sha256 {
            return Err(held("the retained share snapshot identity changed"));
        }
        for namespace in snapshot.namespaces {
            let store = engine.store.open_namespace(&namespace.name).await?;
            for entries in namespace.entries.chunks(4) {
                let rows = entries
                    .iter()
                    .map(|entry| {
                        let key = STANDARD
                            .decode(&entry.key)
                            .map_err(|_| held("the share key is invalid"))?;
                        let key = if namespace.name == "cas_blobs" {
                            key
                        } else {
                            crate::sync::cloud_share::rewrite_key(
                                &key,
                                &item.target.prefix,
                                receiver,
                            )?
                        };
                        Ok((
                            key,
                            STANDARD
                                .decode(&entry.value)
                                .map_err(|_| held("the share value is invalid"))?,
                        ))
                    })
                    .collect::<SyncResult<Vec<_>>>()?;
                put_exact(store.as_ref(), rows).await?;
            }
        }
    }
    engine.store.restore_durability_barrier().await?;
    Ok(())
}

async fn put_exact(
    store: &dyn crate::storage::KvStore,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
) -> SyncResult<()> {
    let current = store
        .get_many(rows.iter().map(|(key, _)| key.clone()).collect())
        .await?;
    if current.len() != rows.len() {
        return Err(held("the receiver comparison batch is incomplete"));
    }
    for ((_, value), current) in rows.iter().zip(current) {
        if current.as_ref().is_some_and(|current| current != value) {
            return Err(held(
                "the sender share union contains conflicting records or files",
            ));
        }
    }
    store.restore_batch_put(rows).await?;
    Ok(())
}
