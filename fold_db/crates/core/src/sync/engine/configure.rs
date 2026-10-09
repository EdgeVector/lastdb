//! Share/org target configuration and reconfigure.

use super::super::org_sync::{SyncPartitioner, SyncTarget};
use super::*;

impl SyncEngine {
    /// Configure non-personal sync targets (cross-user shares).
    ///
    /// Replaces all non-personal targets atomically. The personal target at
    /// index 0 is always preserved. The partitioner classifies pending log
    /// entries to the correct target by key prefix.
    ///
    /// This is the runtime reconfiguration entry point: callers MUST invoke
    /// this every time share rules or share subscriptions change in Sled so
    /// the sync engine picks up new upload/download prefixes without
    /// restarting the node.
    ///
    /// `extra_targets` should contain one `SyncTarget` per:
    /// - active outbound share rule (uploads under `{share_prefix}/log/`)
    /// - active inbound share subscription (downloads under
    ///   `{share_prefix}/log/`)
    ///
    /// The `partitioner` must be built from the same share rules so write
    /// routing stays consistent with the target list.
    pub async fn configure_targets(
        &self,
        partitioner: SyncPartitioner,
        extra_targets: Vec<SyncTarget>,
    ) {
        self.configure_targets_with_restore_scopes(
            partitioner,
            extra_targets,
            std::collections::HashMap::default(),
        )
        .await;
    }

    /// Configure targets and the local storage prefixes included in each
    /// target's photograph.
    pub async fn configure_targets_with_restore_scopes(
        &self,
        partitioner: SyncPartitioner,
        extra_targets: Vec<SyncTarget>,
        restore_scopes: std::collections::HashMap<String, Vec<String>>,
    ) {
        let _config = self.target_config_lock.lock().await;
        *self.partitioner.lock().await = Some(partitioner);
        let mut targets = self.targets.lock().await;
        targets.truncate(1); // Keep personal target
        targets.extend(extra_targets.iter().cloned());
        let mut scopes = self.target_restore_scopes.lock().await;
        scopes.clear();
        for target in &extra_targets {
            scopes.insert(
                target.prefix.clone(),
                restore_scopes
                    .get(&target.prefix)
                    .cloned()
                    .filter(|prefixes| !prefixes.is_empty())
                    .unwrap_or_else(|| vec![target.prefix.clone()]),
            );
        }
        self.target_config_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// Alias for [`configure_targets`] used by sharing-related call sites.
    pub async fn reconfigure_sharing(
        &self,
        partitioner: SyncPartitioner,
        extra_targets: Vec<SyncTarget>,
    ) {
        self.configure_targets(partitioner, extra_targets).await;
    }

    /// Check if any non-personal sync targets are configured.
    pub async fn has_scoped_targets(&self) -> bool {
        self.targets.lock().await.len() > 1
    }

    /// Return the R2 prefix of every configured sync target, in order.
    ///
    /// Index 0 is the personal prefix. Remaining entries are share prefixes in
    /// the order they were registered via `configure_targets` /
    /// `reconfigure_sharing`. Primarily intended for tests and status
    /// endpoints that need to verify runtime reconfiguration took effect.
    pub async fn target_prefixes(&self) -> Vec<String> {
        self.targets
            .lock()
            .await
            .iter()
            .map(|t| t.prefix.clone())
            .collect()
    }

    pub(crate) async fn target_config_snapshot(
        &self,
    ) -> (Vec<SyncTarget>, Option<SyncPartitioner>) {
        let (targets, partitioner, _) = self.target_config_snapshot_with_generation().await;
        (targets, partitioner)
    }

    pub(crate) async fn target_config_snapshot_with_generation(
        &self,
    ) -> (Vec<SyncTarget>, Option<SyncPartitioner>, u64) {
        let _config = self.target_config_lock.lock().await;
        let targets = self.targets.lock().await.clone();
        let partitioner = self.partitioner.lock().await.clone();
        let generation = self
            .target_config_generation
            .load(std::sync::atomic::Ordering::Acquire);
        (targets, partitioner, generation)
    }

    /// Claim ownership of a cloud head id (org_hash or db_hash) on Exemem.
    ///
    /// Required before org_hash-scoped presigns succeed under principal
    /// membership. Soft-fails are returned as Err for the caller to surface.
    pub async fn register_cloud_head_owner(
        &self,
        head_hash: &str,
    ) -> Result<crate::sync::auth::ops::register_db::DbRegistration, crate::sync::error::SyncError>
    {
        self.auth.register_db_for_hash(head_hash).await
    }

    /// Grant another principal writer/reader on a cloud head (owner only).
    pub async fn grant_cloud_head_member(
        &self,
        head_hash: &str,
        target_user_hash: &str,
        role: &str,
    ) -> Result<crate::sync::auth::ops::register_db::DbRegistration, crate::sync::error::SyncError>
    {
        self.auth
            .register_db_member(head_hash, target_user_hash, role)
            .await
    }

    /// Revoke a principal's live access to a cloud head (owner kick or self-leave).
    pub async fn revoke_cloud_head_member(
        &self,
        head_hash: &str,
        target_user_hash: &str,
    ) -> Result<(), crate::sync::error::SyncError> {
        self.auth
            .unregister_db_member(head_hash, target_user_hash)
            .await
    }
}
