//! Fresh receiver namespaces admit source and existing destination body peaks.

use super::*;
use crate::sync::cloud_purge::memory::{self, Admission};
use crate::sync::snapshot::Snapshot;
use base64::{engine::general_purpose::STANDARD, Engine as _};

pub(super) async fn write(
    engine: &SyncEngine,
    staged: &[stage::StagedShare],
    receiver: &str,
) -> SyncResult<()> {
    if !staged.is_empty() {
        // A share seed is another party's data: record that before it lands.
        engine.mark_peer_apply_seen_now().await?;
    }
    for item in staged {
        let meta = std::fs::symlink_metadata(&item.path)?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err(held("the retained share seed file is unsafe"));
        }
        let peak = memory::checked_sum(&[
            memory::checked_scale(memory::logical_row_bound(meta.len()), 24)?,
            memory::row_peak(0)?,
        ])?;
        let admission = Admission::acquire(peak).await?;
        let bytes = std::fs::read(&item.path)?;
        if bytes.len() as u64 != meta.len()
            || crate::hex::sha256_hex(&bytes) != item.ciphertext_sha256
        {
            return Err(held("the retained share seed bytes changed"));
        }
        let snapshot = Snapshot::unseal(&bytes, &item.target.crypto).await?;
        stage::validate(&snapshot, &item.target.prefix)?;
        if crate::hex::sha256_hex(serde_json::to_vec(&snapshot)?) != item.snapshot_sha256 {
            return Err(held("the retained share snapshot identity changed"));
        }
        drop(bytes);
        for namespace in snapshot.namespaces {
            for entry in namespace.entries {
                let key = STANDARD
                    .decode(&entry.key)
                    .map_err(|_| held("the share key is invalid"))?;
                let key = if namespace.name == "cas_blobs" {
                    key
                } else {
                    crate::sync::cloud_share::rewrite_key(&key, &item.target.prefix, receiver)?
                };
                put_exact(engine, &namespace.name, key, &entry.value, &admission).await?;
            }
        }
    }
    engine.store.restore_durability_barrier().await?;
    Ok(())
}

async fn put_exact(
    engine: &SyncEngine,
    namespace: &str,
    key: Vec<u8>,
    encoded: &str,
    admission: &Admission,
) -> SyncResult<()> {
    let id = std::str::from_utf8(&key)
        .map_err(|_| held("the receiver key is invalid"))?
        .to_string();
    let sizes = memory::raw_lengths(
        admission,
        memory::checked_scale(encoded.len() as u64, 8)?,
        engine.store.as_ref(),
        namespace,
        std::slice::from_ref(&id),
    )?;
    let peak = memory::checked_sum(&[
        memory::row_peak(encoded.len() as u64)?,
        sizes[0].map(memory::read_peak).transpose()?.unwrap_or(0),
    ])?;
    admission.check(peak)?;
    let value = STANDARD
        .decode(encoded)
        .map_err(|_| held("the share value is invalid"))?;
    let store = engine.store.open_namespace(namespace).await?;
    let current = store.get(&key).await?;
    if current.is_some() != sizes[0].is_some() {
        return Err(held("the receiver destination changed after admission"));
    }
    if let Some(current) = current {
        if current != value {
            return Err(held(
                "the sender share union contains conflicting records or files",
            ));
        }
    } else {
        store.restore_batch_put(vec![(key, value)]).await?;
    }
    Ok(())
}
