use super::*;
use crate::storage::traits::NamespacedStore;

/// Prepare an inert LastStore copy for an offline, independent S0 root cut.
/// The caller must validate the stopped copy before it opens the store.
pub async fn prepare_offline_s0_restore_marker(store: &dyn NamespacedStore) -> Result<(), String> {
    let pin_log = store
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(|error| format!("open offline S0 marker namespace: {error}"))?;
    if let Some(raw) = pin_log
        .get(BACKUP_RESTORE_F_KEY)
        .await
        .map_err(|error| format!("read offline S0 marker: {error}"))?
    {
        let existing: BackupRestoreFrontier = serde_json::from_slice(&raw)
            .map_err(|error| format!("invalid existing offline S0 marker: {error}"))?;
        let mode = existing
            .validate()
            .map_err(|error| format!("invalid existing offline S0 marker: {error}"))?;
        if mode == BackupRestoreMode::S0Only {
            if existing.by_writer.is_empty() {
                return Ok(());
            }
            return Err("existing S0 marker has a nonempty writer frontier".into());
        }
    }
    let marker = BackupRestoreFrontier {
        version: 2,
        by_writer: BTreeMap::new(),
        mode: Some(BackupRestoreMode::S0Only),
    };
    let raw = serde_json::to_vec(&marker)
        .map_err(|error| format!("encode offline S0 marker: {error}"))?;
    pin_log
        .put(BACKUP_RESTORE_F_KEY, raw)
        .await
        .map_err(|error| format!("write offline S0 marker: {error}"))?;
    pin_log
        .flush()
        .await
        .map_err(|error| format!("flush offline S0 marker: {error}"))?;
    require_offline_s0_restore_marker(store).await
}

/// Refuse a saved offline plan unless its source still has the exact S0 mode.
pub async fn require_offline_s0_restore_marker(store: &dyn NamespacedStore) -> Result<(), String> {
    let pin_log = store
        .open_namespace(PIN_LOG_NAMESPACE)
        .await
        .map_err(|error| format!("open offline S0 marker namespace: {error}"))?;
    let raw = pin_log
        .get(BACKUP_RESTORE_F_KEY)
        .await
        .map_err(|error| format!("read offline S0 marker: {error}"))?
        .ok_or("offline S0 restore marker is absent")?;
    let marker: BackupRestoreFrontier = serde_json::from_slice(&raw)
        .map_err(|error| format!("invalid offline S0 restore marker: {error}"))?;
    if marker.version != 2
        || marker.mode != Some(BackupRestoreMode::S0Only)
        || !marker.by_writer.is_empty()
    {
        return Err("offline S0 restore marker is not canonical v2 s0_only".into());
    }
    marker
        .validate()
        .map_err(|error| format!("invalid offline S0 restore marker: {error}"))?;
    Ok(())
}
