//! Completeness is withdrawn before restore and published after source durability.

use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

const REFERENCE_PROOFS: [&str; 1] = ["mref:v1:complete"];

pub(in crate::sync::snapshot) struct ReferenceRestoreProofs {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

pub(super) fn is_reference_proof(key: &[u8]) -> bool {
    REFERENCE_PROOFS.iter().any(|proof| key == proof.as_bytes())
}

/// An incomplete destination remains incomplete: personal restore retains other
/// scopes, whose reference coverage cannot be inferred from the incoming backup.
pub(in crate::sync::snapshot) async fn begin_reference_restore(
    store: &dyn NamespacedStore,
    namespaces: &[NamespaceData],
    mode: RestoreMode<'_>,
) -> SyncResult<ReferenceRestoreProofs> {
    let main = store.open_namespace("main").await?;
    let keys: Vec<_> = REFERENCE_PROOFS
        .iter()
        .map(|key| key.as_bytes().to_vec())
        .collect();
    let previous = main.get_many(keys.clone()).await?;
    if previous.len() != keys.len() {
        return Err(SyncError::Storage(
            "restore reference proof batch lost alignment".into(),
        ));
    }
    let mut rows = Vec::new();
    if previous.iter().all(Option::is_some) {
        match mode {
            RestoreMode::Personal => {
                for namespace in namespaces
                    .iter()
                    .filter(|namespace| namespace.name == "main")
                {
                    for entry in &namespace.entries {
                        let key = BASE64.decode(&entry.key).map_err(|error| {
                            SyncError::Serialization(format!("invalid key base64: {error}"))
                        })?;
                        if is_reference_proof(&key) {
                            let value = BASE64.decode(&entry.value).map_err(|error| {
                                SyncError::Serialization(format!("invalid value base64: {error}"))
                            })?;
                            rows.push((key, value));
                        }
                    }
                }
                if rows.len() != REFERENCE_PROOFS.len() {
                    rows.clear();
                }
            }
            RestoreMode::Scoped(_) => {
                rows = keys
                    .iter()
                    .cloned()
                    .zip(previous.into_iter().flatten())
                    .collect();
            }
        }
    }
    main.batch_delete(keys).await?;
    store.restore_durability_barrier().await?;
    Ok(ReferenceRestoreProofs { rows })
}

/// Caller flushes all source namespaces and owner/target pairs before this call.
/// A failure before this point leaves every completeness proof absent.
pub(in crate::sync::snapshot) async fn finish_reference_restore(
    store: &dyn NamespacedStore,
    proofs: ReferenceRestoreProofs,
) -> SyncResult<()> {
    if !proofs.rows.is_empty() {
        store
            .open_namespace("main")
            .await?
            .restore_batch_put(proofs.rows)
            .await?;
    }
    store.restore_durability_barrier().await?;
    Ok(())
}
