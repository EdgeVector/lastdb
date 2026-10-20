//! Restore only the current exact owner/target directory; no source conversion.

use super::*;
use crate::db_operations::atom_store::BlobRefEdge;
use crate::storage::{KvMutation, KvStore};
use std::collections::HashMap;
use std::sync::Arc;

fn selected(key: &[u8], mode: RestoreMode<'_>, is_main: bool) -> bool {
    match mode {
        RestoreMode::Personal => !is_org_scoped_key_bytes(key),
        RestoreMode::Scoped(prefixes) => {
            namespace_key_belongs_to_any_target(key, prefixes, is_main)
        }
    }
}

pub(super) async fn clear_existing(
    store: &dyn NamespacedStore,
    kv: &Arc<dyn KvStore>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    mode: RestoreMode<'_>,
    is_main: bool,
) -> SyncResult<()> {
    let mut sources = Vec::new();
    let mut owners = Vec::new();
    for (key, value) in rows {
        if !selected(&key, mode, is_main)
            || (is_main
                && (BlobRefEdge::is_target_key(&key)
                    || proofs::is_reference_proof(&key)
                    || files::is_main_file_key(&key)))
        {
            continue;
        }
        if let Some(target) = is_main
            .then(|| BlobRefEdge::target_from_owner_key(&key))
            .flatten()
        {
            validate_scoped_owner(&key, &value, mode)?;
            owners.push((key, target));
        } else {
            sources.push(key);
        }
    }
    for chunk in sources.chunks(128) {
        kv.batch_delete(chunk.to_vec()).await?;
    }
    let main = store.open_namespace("main").await?;
    for chunk in owners.chunks(64) {
        crate::storage::disk_pack::transaction::background_scope(
            store.disk_pack_runtime(),
            async {
                kv.batch_delete(chunk.iter().map(|(owner, _)| owner.clone()).collect())
                    .await?;
                main.batch_delete(chunk.iter().map(|(_, target)| target.clone()).collect())
                    .await
            },
        )
        .await?;
    }
    Ok(())
}

fn validate_scoped_owner(key: &[u8], value: &[u8], mode: RestoreMode<'_>) -> SyncResult<()> {
    if let RestoreMode::Scoped(prefixes) = mode {
        for scope in prefixes {
            if let Some(edge) = BlobRefEdge::scoped_owner_row(key, value, scope)? {
                if BlobRefEdge::target_from_owner_key(key).as_deref()
                    != Some(edge.storage_key().as_bytes())
                {
                    return Err(SyncError::Serialization(
                        "blob owner target identity mismatch".into(),
                    ));
                }
                return Ok(());
            }
        }
        return Err(SyncError::Serialization(
            "blob owner row is outside the exact restore scope".into(),
        ));
    }
    Ok(())
}

/// Install retaining targets before any source in this namespace is restored.
/// Logical scoped backups carry owner rows; complete backups carry both rows.
pub(super) async fn prepare_incoming(
    store: &dyn NamespacedStore,
    entries: &[(Vec<u8>, Vec<u8>)],
    mode: RestoreMode<'_>,
) -> SyncResult<()> {
    let selected_owners: HashMap<_, _> = entries
        .iter()
        .filter(|(key, _)| selected(key, mode, true))
        .filter_map(|(key, value)| {
            BlobRefEdge::target_from_owner_key(key).map(|target| (target, (key, value)))
        })
        .collect();
    let values: HashMap<_, _> = entries
        .iter()
        .map(|(key, value)| (key.as_slice(), value))
        .collect();
    let mut puts = Vec::new();
    for (target, (owner, value)) in &selected_owners {
        validate_scoped_owner(owner, value, mode)?;
        let target_value = match mode {
            RestoreMode::Scoped(_) => (*value).clone(),
            RestoreMode::Personal => (*values.get(target.as_slice()).ok_or_else(|| {
                SyncError::Serialization(
                    "current backup is missing a blob target for its owner".into(),
                )
            })?)
            .clone(),
        };
        puts.push(KvMutation::put(target.clone(), target_value));
    }
    let main = store.open_namespace("main").await?;
    for chunk in puts.chunks(128) {
        main.batch_mutate(chunk.to_vec()).await?;
    }
    Ok(())
}
