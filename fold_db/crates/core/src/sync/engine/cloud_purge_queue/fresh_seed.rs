//! Remove another device's staging from an exact, inactive replacement seed.

use super::*;

impl SyncEngine {
    /// Permit local tail replay only on the exact-plan reader of a fresh
    /// inactive home. The hard Cloud Off interlock remains armed, and the
    /// reader rejects every cloud mutation independently.
    pub async fn begin_readonly_cloud_replacement_replay(&self) -> SyncResult<()> {
        if self.cloud_sync_disabled_at().await.is_none()
            || !self.auth.is_cloud_adoption_reader()
            || !self.is_backup_only_mode()
        {
            return Err(SyncError::CloudPurgeHeld(
                "replacement replay requires an exact-plan reader with cloud writes off".into(),
            ));
        }
        self.backup_only_mode
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Use only before the first boot of a fresh restored home. The caller
    /// verifies the exact Ready inventory and supplies its target frontiers.
    /// There is no prior peer home, so this operation creates no retention
    /// receipt. A stopped backup-only engine cannot publish these changes.
    pub async fn reset_fresh_cloud_replacement_seed(
        &self,
        replacement_frontiers: &BTreeMap<String, BTreeMap<String, u64>>,
    ) -> SyncResult<String> {
        if !self.is_backup_only_mode() || replacement_frontiers.is_empty() {
            return Err(SyncError::CloudPurgeHeld(
                "fresh replacement cleanup requires a stopped engine and exact frontiers".into(),
            ));
        }
        if replacement_frontiers.values().any(|frontier| {
            frontier
                .iter()
                .any(|(writer, through)| writer.is_empty() || *through == u64::MAX)
        }) {
            return Err(SyncError::Storage(
                "fresh replacement frontier is invalid".into(),
            ));
        }
        let _config = self.target_config_lock.lock().await;
        let targets = self.targets.lock().await.clone();
        let inherited_floor = self
            .pin_log
            .durable_frontier_floor_for_writer(&self.device_id, &targets, true)
            .await
            .map_err(SyncError::Storage)?;
        for namespace in ["sync_outbox", "sync_capture_reexport", PIN_LOG_NAMESPACE] {
            clear_fresh_namespace(self.store.open_namespace(namespace).await?.as_ref()).await?;
        }
        clear_fresh_namespace(
            self.cursor_store
                .open_namespace("sync_cursors")
                .await?
                .as_ref(),
        )
        .await?;
        let floor = self
            .pin_log
            .install_fresh_seed_frontiers(replacement_frontiers, inherited_floor)
            .await
            .map_err(SyncError::Storage)?;
        self.pending.lock().await.clear();
        self.download_cursors.lock().await.clear();
        *self.outbox_meta.lock().await = OutboxMeta::default();
        *self.seq.lock().await = floor;
        *self.capture_reexport_scan_cursor.lock().await = None;
        self.capture_reexport_presence
            .store(0, std::sync::atomic::Ordering::Release);
        self.store.restore_durability_barrier().await?;
        self.cursor_store.restore_durability_barrier().await?;
        Ok(crate::hex::sha256_hex(serde_json::to_vec(
            &serde_json::json!({"format_version":1,"frontiers":replacement_frontiers,"allocation_floor":floor}),
        )?))
    }
}

async fn clear_fresh_namespace(store: &dyn crate::storage::KvStore) -> SyncResult<()> {
    loop {
        let rows = store.scan_range_paged(&[], &[0xff], 128).await?;
        if rows.is_empty() {
            break;
        }
        store
            .batch_delete(rows.into_iter().map(|(key, _)| key).collect())
            .await?;
    }
    store.flush().await?;
    Ok(())
}
